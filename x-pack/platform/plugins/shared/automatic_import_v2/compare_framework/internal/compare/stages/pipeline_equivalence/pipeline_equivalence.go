// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package pipeline_equivalence

import (
	"compare_framework/internal/compare"
)

const stageID = "pipeline_equivalence"

// Run asks Gemini whether the two pipelines are semantically equivalent and lists differences.
// Returns stage result with verdict + issues + reasoning. Stub: returns pass until Gemini is integrated.
func Run(goldenOutputPath, generatedOutputPath string, _ interface{}) compare.StageResult {
	res := compare.StageResult{
		StageID:          stageID,
		Verdict:          "equivalent",
		ValidationStatus: compare.StatusPass,
		Issues:           nil,
	}
	// TODO: call Gemini with golden vs generated run outputs; parse structured verdict + explanation.
	return res
}
