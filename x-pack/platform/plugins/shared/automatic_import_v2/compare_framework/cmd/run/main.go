// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"compare_framework/internal/agentrunner"
	"compare_framework/internal/client"
	"compare_framework/internal/compare"
	"compare_framework/internal/compare/stages/ecs_vendor"
	"compare_framework/internal/compare/stages/pipeline_equivalence"
	"compare_framework/internal/compare/stages/processor_choice"
	"compare_framework/internal/compare/stages/programmatic"
	"compare_framework/internal/config"
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
	fromPackages := flag.String("from-packages", "", "Load pre-generated package zips from this dir instead of calling Kibana API (overrides config pregenerated_packages_dir)")
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
	if *dataStreamFilter != "" && *packageFilter != "" {
		if cfg.DataStreams == nil {
			cfg.DataStreams = make(map[string][]string)
		}
		cfg.DataStreams[*packageFilter] = []string{*dataStreamFilter}
	}

	samplesBase := wdOrCurrent()
	if cfg.OutputDir != "" {
		samplesBase = cfg.OutputDir
	}
	if *reRunFrom != "" {
		samplesBase = filepath.Dir(*reRunFrom)
	}

	packagesDir := cfg.PregeneratedPackagesDir
	if *fromPackages != "" {
		packagesDir = *fromPackages
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
	if *verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Starting comparison run (config: %s, run dir: %s)\n", *configPath, runDir)
	}
	r := runner.New(cfg)
	if packagesDir != "" {
		if *verbose {
			fmt.Fprintf(os.Stderr, "[verbose] Stage: loading pre-generated packages from %s\n", packagesDir)
		}
		summary, exitCode, err = r.RunFromPackages(runDir, packagesDir, *verbose)
	} else {
		if *verbose {
			fmt.Fprintf(os.Stderr, "[verbose] Stage: running packages against Kibana (create integration, upload samples, poll results)\n")
		}
		summary, exitCode, err = r.Run(runDir, samplesBase, "", *verbose)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "run: %v\n", err)
		os.Exit(1)
	}
	if *verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Kibana stage finished\n")
		for _, res := range summary.Results {
			if res.Error != "" {
				fmt.Fprintf(os.Stderr, "[verbose]   [%s/%s] %s: %s\n", res.Package, res.DataStream, res.Status, res.Error)
			} else {
				fmt.Fprintf(os.Stderr, "[verbose]   [%s/%s] %s\n", res.Package, res.DataStream, res.Status)
			}
		}
	}

	// Pipeline execution (Kibana simulate API) only when at least one generated package exists.
	var pipeResults []pipeline.ExecResult
	if summary.PassCount > 0 {
		kibanaClient := client.New(cfg.KibanaURL, client.AuthHeader(cfg.Auth.Type, cfg.Auth.APIKey, cfg.Auth.Username, cfg.Auth.Password), cfg.HTTPTimeout, cfg.HTTPRetries)
		if *verbose {
			fmt.Fprintf(os.Stderr, "[verbose] Stage: pipeline execution (Kibana simulate API)\n")
		}
		pipeResults, _ = pipeline.Exec(pipeline.ExecOptions{
			IntegrationsDir: cfg.IntegrationsDir,
			RunDir:          runDir,
			SamplesDir:      samplesBase,
			Client:          kibanaClient,
		})
		if *verbose {
			fmt.Fprintf(os.Stderr, "[verbose] Pipeline execution finished\n")
		}
	} else if *verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Skipping pipeline execution (no generated package)\n")
	}
	_ = pipeResults

	// Run comparison stages and compile report
	if *verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Stage: comparison (programmatic, pipeline equivalence, processor choice, ECS/vendor)\n")
	}

	// Create the ADK agent runner once for all LLM stages.
	ctx := context.Background()
	var agentRunner *agentrunner.Runner
	if !*dryRun {
		ar, arErr := agentrunner.New(ctx, cfg.GeminiModel)
		if arErr != nil {
			fmt.Fprintf(os.Stderr, "[warn] agent runner init failed (LLM stages will be skipped): %v\n", arErr)
		}
		agentRunner = ar
	}

	var stageResults []compare.StageResult
	for _, res := range summary.Results {
		if res.Status != "pass" {
			continue
		}
		gPath := res.GoldenPath
		rPath := res.ResultPath
		if *verbose {
			fmt.Fprintf(os.Stderr, "[verbose] Running stages for %s/%s (golden=%s result=%s)\n", res.Package, res.DataStream, gPath, rPath)
		}
		stageResults = append(stageResults, programmatic.Run(gPath, rPath))
		if !*dryRun {
			stageResults = append(stageResults, pipeline_equivalence.Run(ctx, pipeline_equivalence.Options{
				Runner:          agentRunner,
				IntegrationsDir: cfg.IntegrationsDir,
				Package:         res.Package,
				DataStream:      res.DataStream,
				ResultPath:      rPath,
				Temperature:     cfg.GeminiTemperature,
			}))
			stageResults = append(stageResults, processor_choice.Run(nil, nil))
			stageResults = append(stageResults, ecs_vendor.Run(nil, nil, nil))
		}
		break
	}
	if *verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Comparison stages finished (%d stage result(s))\n", len(stageResults))
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
	if *verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Report written to %s (report.md, report.json)\n", runDir)
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
