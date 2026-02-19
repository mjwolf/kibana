// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

func main() {
	outPath := flag.String("output", "", "Write compare report to this path (default: stdout)")
	flag.Parse()
	runDirs := flag.Args()
	if len(runDirs) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: compare-runs <run_dir1> <run_dir2> [run_dir3 ...] [-output report.json]\n")
		os.Exit(1)
	}

	reports := make([]runSummary, 0, len(runDirs))
	for _, dir := range runDirs {
		s, err := loadSummary(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "load %s: %v\n", dir, err)
			os.Exit(1)
		}
		reports = append(reports, s)
	}

	delta := compareReports(reports)
	if *outPath != "" {
		b, _ := json.MarshalIndent(delta, "", "  ")
		_ = os.WriteFile(*outPath, b, 0644)
	} else {
		b, _ := json.MarshalIndent(delta, "", "  ")
		fmt.Println(string(b))
	}
	os.Exit(0)
}

type runSummary struct {
	Dir       string `json:"dir"`
	PassCount int    `json:"pass_count"`
	FailCount int    `json:"fail_count"`
	SkipCount int    `json:"skip_count"`
}

func loadSummary(runDir string) (runSummary, error) {
	var s runSummary
	s.Dir = runDir
	b, err := os.ReadFile(filepath.Join(runDir, "summary.json"))
	if err != nil {
		return s, err
	}
	var raw struct {
		PassCount int `json:"pass_count"`
		FailCount int `json:"fail_count"`
		SkipCount int `json:"skip_count"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return s, err
	}
	s.PassCount = raw.PassCount
	s.FailCount = raw.FailCount
	s.SkipCount = raw.SkipCount
	return s, nil
}

type compareReport struct {
	Runs   []runSummary `json:"runs"`
	Delta  string       `json:"delta"`  // e.g. "3 better, 2 worse, 5 same"
	Better int          `json:"better"`
	Worse  int          `json:"worse"`
	Same   int          `json:"same"`
}

func compareReports(reports []runSummary) compareReport {
	out := compareReport{Runs: reports}
	if len(reports) < 2 {
		return out
	}
	a, b := reports[0], reports[1]
	if a.PassCount > b.PassCount {
		out.Better = 1
		out.Delta = "run2 has more passes"
	} else if a.PassCount < b.PassCount {
		out.Worse = 1
		out.Delta = "run2 has fewer passes"
	} else {
		out.Same = 1
		out.Delta = "same pass count"
	}
	return out
}
