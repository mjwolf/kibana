// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Auth holds authentication for Kibana.
type Auth struct {
	Type     string `yaml:"type"` // api_key or basic
	APIKey   string `yaml:"api_key"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// ScoreWeights defines weights for overall score (0-1 per dimension).
type ScoreWeights struct {
	Programmatic        float64 `yaml:"programmatic"`
	PipelineEquivalence float64 `yaml:"pipeline_equivalence"`
	ProcessorChoice     float64 `yaml:"processor_choice"`
	ECSVendor           float64 `yaml:"ecs_vendor"`
}

// Config is the framework configuration.
type Config struct {
	IntegrationsDir              string       `yaml:"integrations_dir"`
	KibanaURL                    string       `yaml:"kibana_url"`
	ConnectorID                  string       `yaml:"connector_id"`
	Auth                         Auth         `yaml:"auth"`
	Packages                     []string     `yaml:"packages"` // package names or path to list file
	OutputDir                    string       `yaml:"output_dir"`
	ECSFieldsURL                 string       `yaml:"ecs_fields_url"`
	GeminiModel                  string       `yaml:"gemini_model"`
	GeminiTemperature            float64      `yaml:"gemini_temperature"`
	ConsistencyRuns              int          `yaml:"consistency_runs"`
	ConsistencyCompareMode       string       `yaml:"consistency_compare_mode"` // per_package | cross_package_only
	PassThreshold                int          `yaml:"pass_threshold"`
	ScoreWeights                 ScoreWeights `yaml:"score_weights"`
	SkipPackagesWithoutSamples   bool         `yaml:"skip_packages_without_samples"`
	CleanupAfterRun              bool         `yaml:"cleanup_after_run"`
	HTTPRetries            int    `yaml:"http_retries"`
	HTTPTimeoutStr         string `yaml:"http_timeout"`
	GeminiTimeoutStr       string `yaml:"gemini_timeout"`
	ElasticPackageTimeoutStr string `yaml:"elastic_package_timeout"`
	// Parsed durations (set in Load)
	HTTPTimeout            time.Duration `yaml:"-"`
	GeminiTimeout          time.Duration `yaml:"-"`
	ElasticPackageTimeout  time.Duration `yaml:"-"`
	IntegrationsRef        string `yaml:"integrations_ref"` // optional git ref for golden pinning
	GoldenSnapshotDir            string       `yaml:"golden_snapshot_dir"`   // optional path to frozen golden copy
	Seed                         int64        `yaml:"seed"`                  // for seed-samples RNG
}

// DefaultConfig returns config with defaults applied.
func DefaultConfig() Config {
	return Config{
		ECSFieldsURL:               "https://github.com/elastic/ecs/raw/v9.3.0/generated/csv/fields.csv",
		GeminiModel:                "gemini-3-flash-preview",
		GeminiTemperature:           0,
		ConsistencyRuns:            10,
		ConsistencyCompareMode:     "cross_package_only",
		PassThreshold:              80,
		SkipPackagesWithoutSamples: true,
		CleanupAfterRun:             true,
		HTTPRetries:           3,
		HTTPTimeoutStr:        "60s",
		GeminiTimeoutStr:      "120s",
		ElasticPackageTimeoutStr: "300s",
		ScoreWeights: ScoreWeights{
			Programmatic:        0.3,
			PipelineEquivalence: 0.25,
			ProcessorChoice:     0.2,
			ECSVendor:           0.25,
		},
	}
}

// Load reads config from path (YAML). Defaults are applied; then file overrides.
func Load(path string) (*Config, error) {
	cfg := DefaultConfig()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}
	// Env override for seed
	if s := os.Getenv("SEED_SAMPLES_SEED"); s != "" {
		var seed int64
		if _, err := fmt.Sscanf(s, "%d", &seed); err == nil {
			cfg.Seed = seed
		}
	}
	if cfg.Seed == 0 {
		cfg.Seed = 0
	}
	// Resolve paths
	if cfg.IntegrationsDir != "" {
		cfg.IntegrationsDir = expandPath(cfg.IntegrationsDir)
	}
	if cfg.OutputDir != "" {
		cfg.OutputDir = expandPath(cfg.OutputDir)
	}
	if cfg.GoldenSnapshotDir != "" {
		cfg.GoldenSnapshotDir = expandPath(cfg.GoldenSnapshotDir)
	}
	// Parse durations
	if cfg.HTTPTimeoutStr != "" {
		if d, err := time.ParseDuration(cfg.HTTPTimeoutStr); err == nil {
			cfg.HTTPTimeout = d
		} else {
			cfg.HTTPTimeout = 60 * time.Second
		}
	} else {
		cfg.HTTPTimeout = 60 * time.Second
	}
	if cfg.GeminiTimeoutStr != "" {
		if d, err := time.ParseDuration(cfg.GeminiTimeoutStr); err == nil {
			cfg.GeminiTimeout = d
		} else {
			cfg.GeminiTimeout = 120 * time.Second
		}
	} else {
		cfg.GeminiTimeout = 120 * time.Second
	}
	if cfg.ElasticPackageTimeoutStr != "" {
		if d, err := time.ParseDuration(cfg.ElasticPackageTimeoutStr); err == nil {
			cfg.ElasticPackageTimeout = d
		} else {
			cfg.ElasticPackageTimeout = 300 * time.Second
		}
	} else {
		cfg.ElasticPackageTimeout = 300 * time.Second
	}
	return &cfg, nil
}


func expandPath(p string) string {
	if strings.HasPrefix(p, "~/") {
		home, _ := os.UserHomeDir()
		if home != "" {
			return filepath.Join(home, p[2:])
		}
	}
	return p
}

// Validate returns an error if required fields are missing.
func (c *Config) Validate() error {
	if c.IntegrationsDir == "" {
		return fmt.Errorf("integrations_dir is required")
	}
	if c.KibanaURL == "" {
		return fmt.Errorf("kibana_url is required")
	}
	if c.ConnectorID == "" {
		return fmt.Errorf("connector_id is required")
	}
	if c.OutputDir == "" {
		return fmt.Errorf("output_dir is required")
	}
	if c.ConsistencyCompareMode != "" && c.ConsistencyCompareMode != "per_package" && c.ConsistencyCompareMode != "cross_package_only" {
		return fmt.Errorf("consistency_compare_mode must be per_package or cross_package_only")
	}
	return nil
}

// PackageList returns the list of package names to run. If packages is a single path to a file, read it.
func (c *Config) PackageList() ([]string, error) {
	if len(c.Packages) == 0 {
		return nil, nil
	}
	if len(c.Packages) == 1 {
		p := expandPath(c.Packages[0])
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			data, err := os.ReadFile(p)
			if err != nil {
				return nil, err
			}
			var names []string
			for _, line := range strings.Split(string(data), "\n") {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "#") {
					names = append(names, line)
				}
			}
			return names, nil
		}
	}
	return c.Packages, nil
}
