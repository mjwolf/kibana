// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package samples

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
)

const maxSamplesPerStream = 100

// SeedOptions configures the seed operation.
type SeedOptions struct {
	IntegrationsDir string
	OutputDir       string   // base dir under which samples/<pkg>/<ds>/samples.log or samples.ndjson is written
	Seed            int64    // RNG seed for reproducibility; 0 uses default
	PackageFilter   []string // if non-empty, only these packages
}

// Seed builds the sample corpus from the integrations repo and writes samples to OutputDir (samples/<pkg>/<ds>/samples.log for text logs, samples.ndjson for JSON).
func Seed(opts SeedOptions) (written int, err error) {
	if opts.OutputDir == "" {
		opts.OutputDir = "samples"
	}
	rng := rand.New(rand.NewSource(opts.Seed))
	packagesDir := filepath.Join(opts.IntegrationsDir, "packages")
	entries, err := os.ReadDir(packagesDir)
	if err != nil {
		return 0, fmt.Errorf("list packages: %w", err)
	}
	wantPkg := make(map[string]bool)
	for _, p := range opts.PackageFilter {
		wantPkg[p] = true
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		if len(wantPkg) > 0 && !wantPkg[e.Name()] {
			continue
		}
		pkgPath := filepath.Join(packagesDir, e.Name())
		dataStreamsDir := filepath.Join(pkgPath, "data_stream")
		dsEntries, err := os.ReadDir(dataStreamsDir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return written, fmt.Errorf("list data_streams %s: %w", e.Name(), err)
		}
		for _, ds := range dsEntries {
			if !ds.IsDir() || strings.HasPrefix(ds.Name(), ".") {
				continue
			}
			pipeDir := filepath.Join(dataStreamsDir, ds.Name(), "_dev", "test", "pipeline")
			lines, logOnly, err := extractSamplesFromPipelineDir(pipeDir)
			if err != nil {
				return written, err
			}
			if len(lines) == 0 {
				continue
			}
			// Shuffle and take up to maxSamplesPerStream
			shuffled := make([]string, len(lines))
			copy(shuffled, lines)
			for i := range shuffled {
				j := rng.Intn(i + 1)
				shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
			}
			if len(shuffled) > maxSamplesPerStream {
				shuffled = shuffled[:maxSamplesPerStream]
			}
			outDir := filepath.Join(opts.OutputDir, "samples", e.Name(), ds.Name())
			if err := os.MkdirAll(outDir, 0755); err != nil {
				return written, fmt.Errorf("mkdir %s: %w", outDir, err)
			}
			samplesFileName := "samples.ndjson"
			if logOnly {
				samplesFileName = "samples.log"
			}
			outPath := filepath.Join(outDir, samplesFileName)
			if err := writeLines(outPath, shuffled); err != nil {
				return written, err
			}
			written++
		}
	}
	return written, nil
}

func extractSamplesFromPipelineDir(pipeDir string) ([]string, bool, error) {
	entries, err := os.ReadDir(pipeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var all []string
	logOnly := true
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasSuffix(name, "-expected.json") {
			continue
		}
		path := filepath.Join(pipeDir, name)
		if strings.HasSuffix(name, ".log") {
			lines, err := readLines(path)
			if err != nil {
				return nil, false, fmt.Errorf("read %s: %w", path, err)
			}
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" {
					all = append(all, line)
				}
			}
			continue
		}
		if strings.HasSuffix(name, ".json") && strings.HasPrefix(name, "test-") {
			logOnly = false
			lines, err := extractSamplesFromJSON(path)
			if err != nil {
				return nil, false, fmt.Errorf("parse %s: %w", path, err)
			}
			all = append(all, lines...)
		}
	}
	return all, logOnly, nil
}

func extractSamplesFromJSON(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		// Try array of objects
		var arr []map[string]interface{}
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, err
		}
		var out []string
		for _, m := range arr {
			if msg, _ := m["message"].(string); msg != "" {
				out = append(out, msg)
			} else {
				b, _ := json.Marshal(m)
				out = append(out, string(b))
			}
		}
		return out, nil
	}
	// Object: check for events[].message
	if ev, ok := obj["events"].([]interface{}); ok {
		var out []string
		for _, e := range ev {
			if m, ok := e.(map[string]interface{}); ok {
				if msg, _ := m["message"].(string); msg != "" {
					out = append(out, msg)
				}
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	if msg, _ := obj["message"].(string); msg != "" {
		return []string{msg}, nil
	}
	b, _ := json.Marshal(obj)
	return []string{string(b)}, nil
}

func readLines(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, s := range strings.Split(string(data), "\n") {
		lines = append(lines, s)
	}
	return lines, nil
}

func writeLines(path string, lines []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, line := range lines {
		if _, err := fmt.Fprintln(f, line); err != nil {
			return err
		}
	}
	return nil
}

// ResolveSamplesPath returns the path to the samples file for a package/data stream (samples.log or samples.ndjson).
// It tries samples.log first, then samples.ndjson, so both extensions are supported.
func ResolveSamplesPath(baseDir, pkg, dataStream string) (string, error) {
	dir := filepath.Join(baseDir, "samples", pkg, dataStream)
	for _, name := range []string{"samples.log", "samples.ndjson"} {
		p := filepath.Join(dir, name)
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("no samples file in %s (tried samples.log, samples.ndjson)", dir)
}
