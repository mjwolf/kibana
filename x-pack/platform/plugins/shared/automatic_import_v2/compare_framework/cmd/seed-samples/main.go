// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"compare_framework/internal/config"
	"compare_framework/internal/samples"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config YAML")
	outputDir := flag.String("output", "", "Base dir for samples/ (default: config output_dir or .)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	base := *outputDir
	if base == "" {
		base = cfg.OutputDir
	}
	if base == "" {
		base = "."
	}
	base, _ = filepath.Abs(base)

	names, _ := cfg.PackageList()
	opts := samples.SeedOptions{
		IntegrationsDir: cfg.IntegrationsDir,
		OutputDir:       base,
		Seed:            cfg.Seed,
		PackageFilter:   names,
	}
	written, err := samples.Seed(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stdout, "Wrote %d sample files to %s/samples/\n", written, base)
	os.Exit(0)
}
