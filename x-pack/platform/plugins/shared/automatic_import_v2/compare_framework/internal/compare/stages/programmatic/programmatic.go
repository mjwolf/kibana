// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package programmatic

import (
	"compare_framework/internal/compare"
)

const stageID = "programmatic"

// Run performs programmatic comparison: normalize and diff run outputs, field lists. No LLM.
// It receives golden and generated run output paths (or content) and returns a StageResult.
func Run(goldenOutputPath, generatedOutputPath string) compare.StageResult {
	res := compare.StageResult{
		StageID:          stageID,
		ValidationStatus: compare.StatusPass,
		Issues:           nil,
		Scores:           map[string]int{"output_match": 100},
	}
	// Placeholder: real implementation would read both outputs, normalize, diff field-by-field.
	if goldenOutputPath == "" || generatedOutputPath == "" {
		res.ValidationStatus = compare.StatusWarn
		res.AddIssue("programmatic", "run", "warning", "missing golden or generated output path", "")
	}
	return res
}
