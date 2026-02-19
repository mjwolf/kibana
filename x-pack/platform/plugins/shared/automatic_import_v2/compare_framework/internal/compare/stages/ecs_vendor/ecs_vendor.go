// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package ecs_vendor

import (
	"compare_framework/internal/compare"
)

const stageID = "ecs_vendor"

// Run validates ECS match, could-improve, vendor consistency via Gemini. Stub: returns pass until Gemini is integrated.
func Run(goldenFields, generatedFields []string, ecsFieldList interface{}) compare.StageResult {
	res := compare.StageResult{
		StageID:          stageID,
		ValidationStatus: compare.StatusPass,
		Scores:           map[string]int{"ecs_match": 100},
		Issues:           nil,
	}
	// TODO: load ECS CSV, send field lists + ECS spec to Gemini; get best ECS match, could-improve, vendor consistency + reasoning.
	return res
}
