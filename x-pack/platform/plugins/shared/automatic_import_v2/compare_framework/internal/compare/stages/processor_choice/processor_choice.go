// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package processor_choice

import (
	"compare_framework/internal/compare"
)

const stageID = "processor_choice"

// Run asks Gemini whether the correct/best processors were chosen. Stub: returns pass until Gemini is integrated.
func Run(goldenProcessors, generatedProcessors interface{}) compare.StageResult {
	res := compare.StageResult{
		StageID:          stageID,
		Verdict:          "acceptable",
		ValidationStatus: compare.StatusPass,
		Issues:           nil,
	}
	// TODO: call Gemini with normalized processor lists; get per-processor verdict + reasoning.
	return res
}
