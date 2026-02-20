// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package pipeline_equivalence

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"compare_framework/internal/agentrunner"
	"compare_framework/internal/compare"

	"gopkg.in/yaml.v3"
)

const stageID = "pipeline_equivalence"

// agentInstruction is the system instruction for the pipeline comparison agent.
// It defines the agent's role, evaluation criteria, and required output schema.
const agentInstruction = `You are an expert in Elasticsearch ingest pipelines used in Elastic integrations.

Your task is to compare a golden (official) ingest pipeline with an AI-generated pipeline for the same data stream. You analyze semantic similarity, processor choices, and quality.

## Evaluation Criteria

1. **Semantic Purpose**: Compare processors by what they achieve, not by name or position. A grok and dissect doing the same extraction are semantically similar.
2. **Processor Choice**: For each logical processing step, identify the processor type used in each pipeline (grok, dissect, script, set, rename, convert, date, remove, append, pipeline, etc.) and judge which is better.
3. **Quality**: Consider correctness, performance, readability, robustness, and Elastic best practices.
4. **Completeness**: Identify logic present in one pipeline but absent from the other.

## Important
- The golden pipeline is from the official Elastic integration — but it is NOT automatically better.
- The AI-generated pipeline may use a more efficient, correct, or modern approach. Judge objectively.
- If the golden has multiple pipeline files (e.g. default.yml + sub-pipelines referenced via "pipeline" processors), treat them as one logical pipeline.
- Map processors across pipelines by purpose. List unmatched ones separately.

## Output Format
Return a JSON object with this exact structure:
{
  "semantic_similarity_score": <0-100>,
  "overall_verdict": "<equivalent|mostly_equivalent|different|significantly_different>",
  "summary": "<1-3 sentence comparison summary>",
  "processor_comparisons": [
    {
      "index": <0-based position in the logical processing sequence>,
      "purpose": "<what this processing step achieves>",
      "golden_type": "<processor type in golden>",
      "generated_type": "<processor type in generated>",
      "types_match": <true|false>,
      "better_choice": "<golden|generated|equivalent>",
      "reasoning": "<concise 1-2 sentence explanation>"
    }
  ],
  "missing_in_generated": ["<processing logic in golden but absent from generated>"],
  "extra_in_generated": ["<processing logic in generated but absent from golden>"],
  "quality_notes": "<overall quality observations: error handling, edge cases, performance>"
}

Scoring guide: 100 = semantically identical; 80-99 = mostly equivalent, minor differences; 50-79 = same goal, notable differences; <50 = fundamentally different approach.`

// Options configures the pipeline equivalence stage.
type Options struct {
	Runner          *agentrunner.Runner
	IntegrationsDir string
	Package         string
	DataStream      string
	ResultPath      string
	Temperature     float64
}

// GeminiResponse is the structured JSON the agent returns.
type GeminiResponse struct {
	SemanticSimilarityScore int                   `json:"semantic_similarity_score"`
	OverallVerdict          string                `json:"overall_verdict"`
	Summary                 string                `json:"summary"`
	ProcessorComparisons    []ProcessorComparison `json:"processor_comparisons"`
	MissingInGenerated      []string              `json:"missing_in_generated"`
	ExtraInGenerated        []string              `json:"extra_in_generated"`
	QualityNotes            string                `json:"quality_notes"`
}

// ProcessorComparison is the per-processor agent verdict.
type ProcessorComparison struct {
	Index         int    `json:"index"`
	Purpose       string `json:"purpose"`
	GoldenType    string `json:"golden_type"`
	GeneratedType string `json:"generated_type"`
	TypesMatch    bool   `json:"types_match"`
	BetterChoice  string `json:"better_choice"` // golden | generated | equivalent
	Reasoning     string `json:"reasoning"`
}

// Run loads golden and generated pipelines, sends them to a Gemini agent for semantic comparison,
// and returns a stage result with per-processor verdicts and an overall similarity score.
func Run(ctx context.Context, opts Options) compare.StageResult {
	res := compare.StageResult{
		StageID:          stageID,
		ValidationStatus: compare.StatusPass,
	}

	if opts.Runner == nil {
		res.ValidationStatus = compare.StatusWarn
		res.AddIssue(stageID, "agent", "warning", "agent runner not available (missing GOOGLE_API_KEY?)", "")
		return res
	}

	goldenPipelines, err := loadGoldenPipelines(opts.IntegrationsDir, opts.Package, opts.DataStream)
	if err != nil {
		res.ValidationStatus = compare.StatusWarn
		res.AddIssue(stageID, "golden", "warning", fmt.Sprintf("load golden pipelines: %v", err), "")
		return res
	}
	generatedPipeline, err := loadGeneratedPipeline(opts.ResultPath)
	if err != nil {
		res.ValidationStatus = compare.StatusWarn
		res.AddIssue(stageID, "generated", "warning", fmt.Sprintf("load generated pipeline: %v", err), "")
		return res
	}

	prompt := buildPrompt(opts.Package, opts.DataStream, goldenPipelines, generatedPipeline)

	var temp *float32
	if opts.Temperature > 0 {
		t := float32(opts.Temperature)
		temp = &t
	}

	raw, err := opts.Runner.RunJSONAgent(ctx, agentrunner.AgentConfig{
		Name:        "pipeline-equivalence",
		Instruction: agentInstruction,
		Temperature: temp,
	}, prompt)
	if err != nil {
		res.ValidationStatus = compare.StatusWarn
		res.AddIssue(stageID, "agent", "warning", fmt.Sprintf("agent call failed: %v", err), "")
		return res
	}

	var gr GeminiResponse
	if err := json.Unmarshal(raw, &gr); err != nil {
		res.ValidationStatus = compare.StatusWarn
		res.AddIssue(stageID, "agent", "warning", fmt.Sprintf("parse agent response: %v", err), "")
		res.Raw = json.RawMessage(raw)
		return res
	}

	res.Verdict = gr.OverallVerdict
	res.Scores = map[string]int{"semantic_similarity": gr.SemanticSimilarityScore}
	res.Raw = gr

	switch gr.OverallVerdict {
	case "equivalent", "mostly_equivalent":
		res.ValidationStatus = compare.StatusPass
	case "different":
		res.ValidationStatus = compare.StatusWarn
	default:
		res.ValidationStatus = compare.StatusFail
	}

	for _, pc := range gr.ProcessorComparisons {
		if pc.TypesMatch && pc.BetterChoice == "equivalent" {
			continue
		}
		severity := "info"
		if pc.BetterChoice == "golden" {
			severity = "warning"
		}
		msg := fmt.Sprintf("processor[%d] %s: golden=%s generated=%s (better: %s)",
			pc.Index, pc.Purpose, pc.GoldenType, pc.GeneratedType, pc.BetterChoice)
		res.AddIssue(stageID, fmt.Sprintf("processor[%d]", pc.Index), severity, msg, pc.Reasoning)
	}
	for _, m := range gr.MissingInGenerated {
		res.AddIssue(stageID, "coverage", "warning", "missing in generated: "+m, "")
	}
	for _, e := range gr.ExtraInGenerated {
		res.AddIssue(stageID, "coverage", "info", "extra in generated: "+e, "")
	}
	if gr.QualityNotes != "" {
		res.AddIssue(stageID, "quality", "info", gr.QualityNotes, "")
	}
	return res
}

// loadGoldenPipelines loads all ingest pipeline files for a package/data stream.
func loadGoldenPipelines(integrationsDir, pkg, dataStream string) (map[string]interface{}, error) {
	pipeDir := filepath.Join(integrationsDir, "packages", pkg, "data_stream", dataStream, "elasticsearch", "ingest_pipeline")
	entries, err := os.ReadDir(pipeDir)
	if err != nil {
		return nil, err
	}
	pipelines := make(map[string]interface{})
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".json") && !strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(pipeDir, name))
		if err != nil {
			return nil, err
		}
		var parsed interface{}
		if strings.HasSuffix(name, ".json") {
			if err := json.Unmarshal(data, &parsed); err != nil {
				return nil, fmt.Errorf("parse %s: %w", name, err)
			}
		} else {
			if err := yaml.Unmarshal(data, &parsed); err != nil {
				return nil, fmt.Errorf("parse %s: %w", name, err)
			}
		}
		pipelines[name] = parsed
	}
	if len(pipelines) == 0 {
		return nil, fmt.Errorf("no pipeline files in %s", pipeDir)
	}
	return pipelines, nil
}

func loadGeneratedPipeline(resultPath string) (interface{}, error) {
	data, err := os.ReadFile(resultPath)
	if err != nil {
		return nil, err
	}
	var result struct {
		IngestPipeline interface{} `json:"ingestPipeline"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	if result.IngestPipeline == nil {
		return nil, fmt.Errorf("result.json has no ingestPipeline")
	}
	return result.IngestPipeline, nil
}

// buildPrompt constructs the user message with the actual pipeline data.
func buildPrompt(pkg, dataStream string, goldenPipelines map[string]interface{}, generatedPipeline interface{}) string {
	var b strings.Builder

	fmt.Fprintf(&b, "Compare the pipelines for \"%s/%s\".\n\n", pkg, dataStream)
	b.WriteString("## Golden Pipeline(s)\n\n")

	for name, p := range goldenPipelines {
		j, _ := json.MarshalIndent(p, "", "  ")
		fmt.Fprintf(&b, "### %s\n```json\n%s\n```\n\n", name, j)
	}

	j, _ := json.MarshalIndent(generatedPipeline, "", "  ")
	fmt.Fprintf(&b, "## Generated Pipeline\n```json\n%s\n```\n", j)

	return b.String()
}
