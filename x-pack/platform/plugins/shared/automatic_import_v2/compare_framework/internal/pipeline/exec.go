// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"compare_framework/internal/samples"
)

// ExecOptions configures pipeline execution via elastic-package.
type ExecOptions struct {
	IntegrationsDir string
	RunDir          string        // run output dir containing packages/<pkg>/<ds>/result.json and golden.json
	SamplesDir      string        // base dir for samples/<pkg>/<ds>/samples.log or samples.ndjson
	WorkDir         string        // directory for work/golden_<pkg>_<ds> and work/generated_<pkg>_<ds>
	Timeout         time.Duration
}

// RunOutput holds the path to the test output for one run (golden or generated).
type RunOutput struct {
	Dir    string
	Output string
	Err    error
}

// ExecResult is the result of running pipeline tests for one package/data stream.
type ExecResult struct {
	Package    string
	DataStream string
	Golden     RunOutput
	Generated  RunOutput
}

// Exec runs elastic-package test pipeline for each package/data stream in runDir. It copies the golden package
// twice, replaces pipeline test inputs with framework samples, and in the generated copy replaces the ingest
// pipeline with the one from result.json. Returns one ExecResult per package/data stream.
func Exec(opts ExecOptions) ([]ExecResult, error) {
	if opts.WorkDir == "" {
		opts.WorkDir = filepath.Join(opts.RunDir, "work")
	}
	if err := os.MkdirAll(opts.WorkDir, 0755); err != nil {
		return nil, err
	}
	packagesDir := filepath.Join(opts.RunDir, "packages")
	entries, err := os.ReadDir(packagesDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var results []ExecResult
	for _, pkgE := range entries {
		if !pkgE.IsDir() {
			continue
		}
		pkg := pkgE.Name()
		dsDir := filepath.Join(packagesDir, pkg)
		dsEntries, err := os.ReadDir(dsDir)
		if err != nil {
			return results, err
		}
		for _, dsE := range dsEntries {
			if !dsE.IsDir() {
				continue
			}
			ds := dsE.Name()
			res := execOne(opts, pkg, ds)
			results = append(results, res)
		}
	}
	return results, nil
}

func execOne(opts ExecOptions, pkg, dataStream string) ExecResult {
	res := ExecResult{Package: pkg, DataStream: dataStream}
	resultPath := filepath.Join(opts.RunDir, "packages", pkg, dataStream, "result.json")
	resultData, err := os.ReadFile(resultPath)
	if err != nil {
		res.Generated.Err = err
		return res
	}
	var result struct {
		IngestPipeline interface{} `json:"ingestPipeline"`
	}
	if err := json.Unmarshal(resultData, &result); err != nil {
		res.Generated.Err = err
		return res
	}
	goldenPkgPath := filepath.Join(opts.IntegrationsDir, "packages", pkg)
	samplesPath, err := samples.ResolveSamplesPath(opts.SamplesDir, pkg, dataStream)
	if err != nil {
		res.Generated.Err = err
		return res
	}
	samplesData, _ := os.ReadFile(samplesPath)
	workGolden := filepath.Join(opts.WorkDir, "golden_"+pkg+"_"+dataStream)
	workGenerated := filepath.Join(opts.WorkDir, "generated_"+pkg+"_"+dataStream)
	if err := copyDir(goldenPkgPath, workGolden); err != nil {
		res.Golden.Err = err
		return res
	}
	if err := copyDir(goldenPkgPath, workGenerated); err != nil {
		res.Generated.Err = err
		return res
	}
	pipeInputDir := filepath.Join(workGolden, "data_stream", dataStream, "_dev", "test", "pipeline")
	if err := writeSamplesAsPipelineInput(pipeInputDir, samplesData); err != nil {
		res.Golden.Err = err
		return res
	}
	pipeInputDirGen := filepath.Join(workGenerated, "data_stream", dataStream, "_dev", "test", "pipeline")
	if err := writeSamplesAsPipelineInput(pipeInputDirGen, samplesData); err != nil {
		res.Generated.Err = err
		return res
	}
	// Replace ingest pipeline in generated copy with AIv2 output
	pipeDir := filepath.Join(workGenerated, "data_stream", dataStream, "elasticsearch", "ingest_pipeline")
	if result.IngestPipeline != nil {
		if err := writeGeneratedPipeline(pipeDir, result.IngestPipeline); err != nil {
			res.Generated.Err = err
			return res
		}
	}
	// Run elastic-package test pipeline for both
	res.Golden.Dir = workGolden
	res.Golden.Output, res.Golden.Err = runElasticPackageTest(context.Background(), workGolden, dataStream, opts.Timeout)
	res.Generated.Dir = workGenerated
	res.Generated.Output, res.Generated.Err = runElasticPackageTest(context.Background(), workGenerated, dataStream, opts.Timeout)
	return res
}

func writeSamplesAsPipelineInput(pipeDir string, samples []byte) error {
	if err := os.MkdirAll(pipeDir, 0755); err != nil {
		return err
	}
	// Write as test-input.log (one sample per line) for pipeline test consumption
	outPath := filepath.Join(pipeDir, "test-input.log")
	return os.WriteFile(outPath, samples, 0644)
}

func writeGeneratedPipeline(pipeDir string, pipeline interface{}) error {
	entries, err := os.ReadDir(pipeDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() && (strings.HasSuffix(e.Name(), ".yml") || strings.HasSuffix(e.Name(), ".yaml") || strings.HasSuffix(e.Name(), ".json")) {
			if err := os.Remove(filepath.Join(pipeDir, e.Name())); err != nil {
				return err
			}
		}
	}
	b, err := json.MarshalIndent(pipeline, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(pipeDir, "default.json"), b, 0644)
}

func runElasticPackageTest(ctx context.Context, workDir, dataStream string, timeout time.Duration) (string, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, "elastic-package", "test", "pipeline", "-C", workDir, "-d", dataStream)
	cmd.Dir = workDir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("elastic-package: %w", err)
	}
	return string(out), nil
}

func copyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0755); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		sp := filepath.Join(src, e.Name())
		dp := filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := copyDir(sp, dp); err != nil {
				return err
			}
			continue
		}
		data, err := os.ReadFile(sp)
		if err != nil {
			return err
		}
		if err := os.WriteFile(dp, data, 0644); err != nil {
			return err
		}
	}
	return nil
}

