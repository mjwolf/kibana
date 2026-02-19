// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package report

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"compare_framework/internal/compare"
)

// Report is the final report: summary + structured details.
type Report struct {
	Summary   Summary    `json:"summary"`
	Details   Details    `json:"details"`
	Timestamp time.Time  `json:"timestamp"`
}

// Summary is the human-readable overview.
type Summary struct {
	PassCount   int    `json:"pass_count"`
	FailCount   int    `json:"fail_count"`
	SkipCount   int    `json:"skip_count"`
	OverallScore int   `json:"overall_score,omitempty"`
	Overview   string `json:"overview"`
}

// Details contains all errors/problems for programmatic use.
type Details struct {
	Issues []compare.Issue `json:"issues"`
	Stages []compare.StageResult `json:"stages"`
}

// Compiler aggregates stage results into the final report.
type Compiler struct{}

// NewCompiler creates a report compiler.
func NewCompiler() *Compiler {
	return &Compiler{}
}

// Compile builds the report from stage results. It does not contain pipeline/ECS/processor logic—only aggregation.
func (c *Compiler) Compile(stageResults []compare.StageResult, passCount, failCount, skipCount int, overallScore int) *Report {
	var allIssues []compare.Issue
	for _, sr := range stageResults {
		allIssues = append(allIssues, sr.Issues...)
	}
	overview := fmt.Sprintf("Pass: %d, Fail: %d, Skip: %d. Overall score: %d.", passCount, failCount, skipCount, overallScore)
	if failCount > 0 {
		overview += " Some packages or stages failed."
	} else if passCount > 0 {
		overview += " All ran packages passed."
	}
	return &Report{
		Summary: Summary{
			PassCount:     passCount,
			FailCount:     failCount,
			SkipCount:     skipCount,
			OverallScore:  overallScore,
			Overview:      overview,
		},
		Details: Details{
			Issues: allIssues,
			Stages: stageResults,
		},
		Timestamp: time.Now().UTC(),
	}
}

// Write writes the report to runDir: summary as Markdown, structured details as report.json.
func Write(runDir string, report *Report) error {
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return err
	}
	// 1. Human-readable summary (Markdown)
	summaryPath := filepath.Join(runDir, "report.md")
	md := strings.TrimSpace(report.Summary.Overview) + "\n\n"
	md += fmt.Sprintf("- Pass: %d\n", report.Summary.PassCount)
	md += fmt.Sprintf("- Fail: %d\n", report.Summary.FailCount)
	md += fmt.Sprintf("- Skip: %d\n", report.Summary.SkipCount)
	if report.Summary.OverallScore >= 0 {
		md += fmt.Sprintf("- Overall score: %d\n", report.Summary.OverallScore)
	}
	md += "\n## Issues\n\n"
	for _, i := range report.Details.Issues {
		md += fmt.Sprintf("- [%s] %s: %s\n", i.Severity, i.Location, i.Message)
		if i.Reasoning != "" {
			md += "  " + i.Reasoning + "\n"
		}
	}
	if err := os.WriteFile(summaryPath, []byte(md), 0644); err != nil {
		return err
	}
	// 2. Structured details (JSON) for CI
	reportPath := filepath.Join(runDir, "report.json")
	b, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(reportPath, b, 0644)
}
