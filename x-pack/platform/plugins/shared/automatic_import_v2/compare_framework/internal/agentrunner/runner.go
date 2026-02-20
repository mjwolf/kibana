// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

// Package agentrunner provides a shared ADK-based agent runner for LLM comparison stages.
package agentrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"google.golang.org/adk/agent"
	"google.golang.org/adk/agent/llmagent"
	"google.golang.org/adk/model"
	"google.golang.org/adk/model/gemini"
	"google.golang.org/adk/runner"
	"google.golang.org/adk/session"
	"google.golang.org/genai"
)

// Runner wraps a Gemini model and runs ADK agents against it.
// Create one Runner and share it across comparison stages.
type Runner struct {
	llmModel model.LLM
	modelID  string
}

// New creates a Runner with the given Gemini model. Reads GOOGLE_API_KEY from env.
func New(ctx context.Context, modelID string) (*Runner, error) {
	apiKey := os.Getenv("GOOGLE_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("GOOGLE_API_KEY env var is required")
	}
	m, err := gemini.NewModel(ctx, modelID, &genai.ClientConfig{APIKey: apiKey})
	if err != nil {
		return nil, fmt.Errorf("create Gemini model: %w", err)
	}
	return &Runner{llmModel: m, modelID: modelID}, nil
}

// AgentConfig configures a single agent run.
type AgentConfig struct {
	Name        string
	Instruction string   // system-level instruction (agent role + output format)
	Temperature *float32 // nil uses model default
}

// RunJSONAgent creates an LLM agent with the given instruction, sends the prompt as a user message,
// and returns the agent's response parsed as raw JSON. The agent is configured with
// ResponseMIMEType "application/json" so Gemini returns structured output.
func (r *Runner) RunJSONAgent(ctx context.Context, cfg AgentConfig, prompt string) (json.RawMessage, error) {
	genCfg := &genai.GenerateContentConfig{
		ResponseMIMEType: "application/json",
	}
	if cfg.Temperature != nil {
		genCfg.Temperature = cfg.Temperature
	}

	adkAgent, err := llmagent.New(llmagent.Config{
		Name:                     cfg.Name,
		Description:              cfg.Name,
		Model:                    r.llmModel,
		Instruction:              cfg.Instruction,
		DisallowTransferToParent: true,
		DisallowTransferToPeers:  true,
		GenerateContentConfig:    genCfg,
	})
	if err != nil {
		return nil, fmt.Errorf("create agent: %w", err)
	}

	sessionService := session.InMemoryService()
	rr, err := runner.New(runner.Config{
		AppName:        "compare-" + cfg.Name,
		Agent:          adkAgent,
		SessionService: sessionService,
	})
	if err != nil {
		return nil, fmt.Errorf("create runner: %w", err)
	}

	sess, err := sessionService.Create(ctx, &session.CreateRequest{
		AppName: "compare-" + cfg.Name,
		UserID:  "compare",
	})
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}

	userContent := genai.NewContentFromText(prompt, genai.RoleUser)
	var outputs []string

	for event, err := range rr.Run(ctx, "compare", sess.Session.ID(), userContent, agent.RunConfig{}) {
		if err != nil {
			return nil, fmt.Errorf("agent error: %w", err)
		}
		if event == nil {
			continue
		}
		if event.Content != nil {
			for _, part := range event.Content.Parts {
				if part.Text != "" {
					outputs = append(outputs, part.Text)
				}
			}
		}
		if event.IsFinalResponse() {
			break
		}
	}

	text := strings.TrimSpace(strings.Join(outputs, ""))
	if text == "" {
		return nil, fmt.Errorf("agent returned empty response")
	}
	var raw json.RawMessage
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, fmt.Errorf("response not valid JSON: %w\n%s", err, truncate(text, 500))
	}
	return raw, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
