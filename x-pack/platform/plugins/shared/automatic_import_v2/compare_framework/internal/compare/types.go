// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package compare

// ValidationStatus is the outcome of a stage (pass, warn, fail).
type ValidationStatus string

const (
	StatusPass ValidationStatus = "pass"
	StatusWarn ValidationStatus = "warn"
	StatusFail ValidationStatus = "fail"
)

// Issue is one problem found by a stage.
type Issue struct {
	Category   string `json:"category"`
	Location   string `json:"location"`
	Severity   string `json:"severity"` // e.g. error, warning
	Message    string `json:"message"`
	Reasoning  string `json:"reasoning,omitempty"`
}

// StageResult is the common result interface for all comparison stages.
type StageResult struct {
	StageID         string          `json:"stage_id"`
	Verdict         string          `json:"verdict,omitempty"`
	Scores          map[string]int  `json:"scores,omitempty"`
	Issues          []Issue         `json:"issues"`
	ValidationStatus ValidationStatus `json:"validation_status"`
	Raw             interface{}     `json:"raw,omitempty"`
}

// AddIssue appends an issue to the stage result.
func (s *StageResult) AddIssue(cat, location, severity, message, reasoning string) {
	s.Issues = append(s.Issues, Issue{
		Category:  cat,
		Location:  location,
		Severity:  severity,
		Message:   message,
		Reasoning: reasoning,
	})
}
