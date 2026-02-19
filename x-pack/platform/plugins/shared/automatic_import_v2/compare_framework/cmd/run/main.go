// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"compare_framework/internal/config"
	"compare_framework/internal/compare"
	"compare_framework/internal/compare/stages/ecs_vendor"
	"compare_framework/internal/compare/stages/pipeline_equivalence"
	"compare_framework/internal/compare/stages/processor_choice"
	"compare_framework/internal/compare/stages/programmatic"
	"compare_framework/internal/consistency"
	"compare_framework/internal/pipeline"
	"compare_framework/internal/report"
	"compare_framework/internal/runner"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config YAML")
	packageFilter := flag.String("package", "", "Run only this package (overrides config)")
	dataStreamFilter := flag.String("data-stream", "", "Run only this data stream (with -package)")
	reRunFrom := flag.String("re-run-from", "", "Re-run using config and package list from this run dir")
	dryRun := flag.Bool("dry-run", false, "Skip LLM stages; run only programmatic and pipeline execution")
	consistencyMode := flag.Bool("consistency", false, "Run consistency mode (N runs, pairwise compare)")
	verbose := flag.Bool("verbose", false, "Log each package/data stream result (status and error)")
	flag.Parse()

	var cfg *config.Config
	var runDir string
	if *reRunFrom != "" {
		cfgPath := filepath.Join(*reRunFrom, "config.json")
		var err error
		cfg, err = config.Load(cfgPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "re-run config: %v\n", err)
			os.Exit(1)
		}
		runDir = filepath.Join(cfg.OutputDir, "run_"+filepath.Base(*reRunFrom))
		if runDir == *reRunFrom {
			runDir = filepath.Join(cfg.OutputDir, "run_"+time.Now().Format("20060102_150405"))
		}
	} else {
		var err error
		cfg, err = config.Load(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "config: %v\n", err)
			os.Exit(1)
		}
		if err := cfg.Validate(); err != nil {
			fmt.Fprintf(os.Stderr, "config: %v\n", err)
			os.Exit(1)
		}
		runDir = filepath.Join(cfg.OutputDir, "run_"+time.Now().Format("20060102_150405"))
	}

	if cfg.Packages == nil && *packageFilter == "" {
		cfg.Packages = []string{}
	}
	if *packageFilter != "" {
		cfg.Packages = []string{*packageFilter}
	}
	_ = dataStreamFilter // TODO: pass to runner to limit to one data stream

	samplesBase := wdOrCurrent()
	if cfg.OutputDir != "" {
		samplesBase = cfg.OutputDir
	}
	if *reRunFrom != "" {
		samplesBase = filepath.Dir(*reRunFrom)
	}

	var summary *runner.RunSummary
	var exitCode int
	var err error
	if *consistencyMode {
		_, exitCode, runErr := consistency.Run(cfg, runDir, samplesBase)
		if runErr != nil {
			fmt.Fprintf(os.Stderr, "consistency: %v\n", runErr)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stdout, "Consistency run dir: %s\n", runDir)
		os.Exit(exitCode)
	}
	r := runner.New(cfg)
	summary, exitCode, err = r.Run(runDir, samplesBase, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		os.Exit(1)
	}
	if *verbose {
		for _, res := range summary.Results {
			if res.Error != "" {
				fmt.Fprintf(os.Stderr, "[%s/%s] %s: %s\n", res.Package, res.DataStream, res.Status, res.Error)
			} else {
				fmt.Fprintf(os.Stderr, "[%s/%s] %s\n", res.Package, res.DataStream, res.Status)
			}
		}
	}

	// Pipeline execution (elastic-package) for each package that has result.json
	pipeResults, _ := pipeline.Exec(pipeline.ExecOptions{
		IntegrationsDir: cfg.IntegrationsDir,
		RunDir:          runDir,
		SamplesDir:      samplesBase,
		Timeout:         cfg.ElasticPackageTimeout,
	})
	_ = pipeResults

	// Run comparison stages and compile report
	var stageResults []compare.StageResult
	for _, res := range summary.Results {
		if res.Status != "pass" {
			continue
		}
		gPath := res.GoldenPath
		rPath := res.ResultPath
		stageResults = append(stageResults, programmatic.Run(gPath, rPath))
		if !*dryRun {
			stageResults = append(stageResults, pipeline_equivalence.Run(gPath, rPath, nil))
			stageResults = append(stageResults, processor_choice.Run(nil, nil))
			stageResults = append(stageResults, ecs_vendor.Run(nil, nil, nil))
		}
		break
	}
	// When no runs passed, do not add programmatic stage with empty paths (avoids misleading "missing golden or generated output path").
	overallScore := 0
	if summary.PassCount+summary.FailCount > 0 {
		overallScore = summary.PassCount * 100 / (summary.PassCount + summary.FailCount)
	}
	compiler := report.NewCompiler()
	rep := compiler.Compile(stageResults, summary.PassCount, summary.FailCount, summary.SkipCount, overallScore)
	if err := report.Write(runDir, rep); err != nil {
		fmt.Fprintf(os.Stderr, "report: %v\n", err)
	}
	fmt.Fprintf(os.Stdout, "Run dir: %s\n", runDir)
	fmt.Fprintf(os.Stdout, "%s\n", rep.Summary.Overview)
	os.Exit(exitCode)
}

func wdOrCurrent() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}
