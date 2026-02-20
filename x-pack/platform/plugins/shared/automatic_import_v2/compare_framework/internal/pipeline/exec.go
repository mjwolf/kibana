// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"compare_framework/internal/client"
	"compare_framework/internal/samples"

	"gopkg.in/yaml.v3"
)

const maxSimulateDocs = 1000

// ExecOptions configures pipeline execution via Kibana simulate API.
type ExecOptions struct {
	IntegrationsDir string
	RunDir          string // run output dir containing packages/<pkg>/<ds>/result.json
	SamplesDir      string // base dir for samples/<pkg>/<ds>/samples.log or samples.ndjson
	Client          *client.Client
}

// RunOutput holds the simulate result for one run (golden or generated).
type RunOutput struct {
	Output string
	Err    error
}

// ExecResult is the result of running pipeline simulate for one package/data stream.
type ExecResult struct {
	Package    string
	DataStream string
	Golden     RunOutput
	Generated  RunOutput
}

// Exec runs Kibana ingest pipeline simulate for each package/data stream in runDir.
// For each pkg/ds: loads golden pipeline from integrations dir, generated pipeline from result.json,
// samples from SamplesDir; calls simulate API for both pipelines and records output/errors.
func Exec(opts ExecOptions) ([]ExecResult, error) {
	if opts.Client == nil {
		return nil, fmt.Errorf("pipeline.Exec: Client is required")
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
	samplesPath, err := samples.ResolveSamplesPath(opts.SamplesDir, pkg, dataStream)
	if err != nil {
		res.Golden.Err = err
		res.Generated.Err = err
		return res
	}
	samplesData, err := os.ReadFile(samplesPath)
	if err != nil {
		res.Golden.Err = err
		res.Generated.Err = err
		return res
	}
	docs := samplesToSimulateDocs(samplesData)
	if len(docs) == 0 {
		res.Golden.Err = fmt.Errorf("no sample documents")
		res.Generated.Err = fmt.Errorf("no sample documents")
		return res
	}
	goldenPipeline, err := loadGoldenPipeline(opts.IntegrationsDir, pkg, dataStream)
	if err != nil {
		res.Golden.Err = err
		return res
	}
	res.Golden.Output, res.Golden.Err = runSimulate(opts.Client, goldenPipeline, docs)
	if result.IngestPipeline != nil {
		res.Generated.Output, res.Generated.Err = runSimulate(opts.Client, result.IngestPipeline, docs)
	} else {
		res.Generated.Err = fmt.Errorf("result.json has no ingestPipeline")
	}
	return res
}

func loadGoldenPipeline(integrationsDir, pkg, dataStream string) (interface{}, error) {
	pipeDir := filepath.Join(integrationsDir, "packages", pkg, "data_stream", dataStream, "elasticsearch", "ingest_pipeline")
	entries, err := os.ReadDir(pipeDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, ".json") && !strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml") {
			continue
		}
		path := filepath.Join(pipeDir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var out map[string]interface{}
		if strings.HasSuffix(name, ".json") {
			if err := json.Unmarshal(data, &out); err != nil {
				return nil, err
			}
		} else {
			if err := yaml.Unmarshal(data, &out); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("no pipeline file in %s", pipeDir)
}

func samplesToSimulateDocs(data []byte) []client.SimulateDocument {
	lines := strings.Split(string(data), "\n")
	var docs []client.SimulateDocument
	for i, line := range lines {
		if i >= maxSimulateDocs {
			break
		}
		line = strings.TrimSuffix(line, "\r")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		docs = append(docs, client.SimulateDocument{
			Index:  "index",
			ID:     fmt.Sprintf("%d", i+1),
			Source: map[string]interface{}{"message": line},
		})
	}
	return docs
}

func runSimulate(c *client.Client, pipeline interface{}, docs []client.SimulateDocument) (string, error) {
	resp, err := c.SimulatePipeline(pipeline, docs, false)
	if err != nil {
		return "", err
	}
	var errMsgs []string
	for i, d := range resp.Docs {
		if d.Doc != nil && d.Doc.Error != nil {
			errMsgs = append(errMsgs, fmt.Sprintf("doc[%d]: %s", i, d.Doc.Error.Message))
		}
	}
	if len(errMsgs) > 0 {
		return "", fmt.Errorf("simulate errors: %s", strings.Join(errMsgs, "; "))
	}
	out, _ := json.Marshal(resp)
	return string(out), nil
}
