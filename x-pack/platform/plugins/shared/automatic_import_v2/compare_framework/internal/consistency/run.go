// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package consistency

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"compare_framework/internal/config"
	"compare_framework/internal/runner"
)

// Mode is either per_package (full comparison per package) or cross_package_only (consistency across runs only).
type Mode string

const (
	ModePerPackage      Mode = "per_package"
	ModeCrossPackageOnly Mode = "cross_package_only"
)

// Run executes consistency mode: for each package/data stream, create N data streams (or N runs), collect N results,
// then compare. If mode is cross_package_only, only aggregate consistency metrics are computed (no per-package LLM).
func Run(cfg *config.Config, runDir string, samplesDir string) (*ConsistencyReport, int, error) {
	if cfg.ConsistencyRuns <= 0 {
		cfg.ConsistencyRuns = 10
	}
	mode := Mode(cfg.ConsistencyCompareMode)
	if mode != ModePerPackage && mode != ModeCrossPackageOnly {
		mode = ModeCrossPackageOnly
	}
	report := &ConsistencyReport{
		Mode:       string(mode),
		Runs:       cfg.ConsistencyRuns,
		Timestamp:  time.Now().UTC(),
		Results:    nil,
		Pairwise:   nil,
	}
	r := runner.New(cfg)
	// Run N times: each "run" creates a new data stream (e.g. access_run_1, access_run_2, ...) with same samples.
	// For simplicity we run the runner once per iteration with a distinct data stream suffix; the runner creates
	// one integration per package and one data stream per data stream. To get N results we need N data streams
	// per (pkg, ds). So we run runner N times, each time with a different runDir subdir (run_1, run_2, ...) and
	// the runner creates integration with data stream id like ds_1, ds_2. Actually the runner currently creates
	// one integration per (pkg, ds) with a single data stream. To have N runs we'd create N data streams under
	// the same integration (e.g. dataStreamId = access_run_1, access_run_2). So we need to either:
	// 1) Call runner N times with a modified config that uses different data stream IDs each time, or
	// 2) Extend the runner to create N data streams per (pkg, ds) in one go.
	// Minimal implementation: run the runner N times, each time with runDir = runDir/run_<i>, and collect N summaries.
	// Then compare pairwise (programmatic diff of result.json) when mode is cross_package_only; or run full stages when per_package.
	var summaries []runner.RunSummary
	for i := 0; i < cfg.ConsistencyRuns; i++ {
		subDir := filepath.Join(runDir, fmt.Sprintf("run_%d", i+1))
		sum, code, err := r.Run(subDir, samplesDir, fmt.Sprintf("run_%d", i+1), false)
		if err != nil {
			return report, 1, err
		}
		if code != 0 {
			report.FailCount++
		}
		summaries = append(summaries, *sum)
	}
	report.Results = summaries
	// Pairwise similarity: for each pair of runs, compare result.json (simplified: same pass count = same)
	for i := 0; i < len(summaries); i++ {
		for j := i + 1; j < len(summaries); j++ {
			a, b := summaries[i], summaries[j]
			same := a.PassCount == b.PassCount && a.FailCount == b.FailCount
			report.Pairwise = append(report.Pairwise, PairwiseResult{RunA: i + 1, RunB: j + 1, Same: same})
		}
	}
	outPath := filepath.Join(runDir, "consistency_report.json")
	b, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile(outPath, b, 0644)
	exitCode := 0
	if report.FailCount > 0 {
		exitCode = 1
	}
	return report, exitCode, nil
}

// ConsistencyReport is the output of consistency mode.
type ConsistencyReport struct {
	Mode      string              `json:"mode"`
	Runs      int                 `json:"runs"`
	Timestamp time.Time           `json:"timestamp"`
	Results   []runner.RunSummary  `json:"results"`
	Pairwise  []PairwiseResult    `json:"pairwise"`
	FailCount int                 `json:"fail_count"`
}

// PairwiseResult is one pair comparison.
type PairwiseResult struct {
	RunA int  `json:"run_a"`
	RunB int  `json:"run_b"`
	Same bool `json:"same"`
}
