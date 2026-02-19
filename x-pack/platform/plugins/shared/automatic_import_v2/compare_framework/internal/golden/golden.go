// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package golden

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Package represents a loaded integration package (manifest only; data streams are listed separately).
type Package struct {
	Name        string
	Path       string
	Manifest   map[string]interface{}
	DataStreams []DataStream
}

// DataStream represents one data stream under a package.
type DataStream struct {
	Name     string
	Path    string
	Manifest map[string]interface{}
	// PipelinePath is the path to the ingest pipeline file(s).
	PipelinePath string
	// FieldsPath is the path to the fields directory.
	FieldsPath string
	// PipelineFiles and FieldFiles are loaded content (paths or raw); caller can read when needed.
	PipelineFiles []string
	FieldFiles    []string
}

// Loader loads golden packages from the filesystem.
type Loader struct {
	integrationsDir string
	snapshotDir     string // if set, use instead of integrationsDir for golden source
}

// NewLoader creates a loader. If goldenSnapshotDir is non-empty, it is used as the source instead of integrationsDir.
func NewLoader(integrationsDir, goldenSnapshotDir string) *Loader {
	return &Loader{integrationsDir: integrationsDir, snapshotDir: goldenSnapshotDir}
}

// ListPackages returns package names under packages/ in the integrations directory.
func (l *Loader) ListPackages() ([]string, error) {
	packagesDir := filepath.Join(l.integrationsDir, "packages")
	if l.snapshotDir != "" {
		packagesDir = filepath.Join(l.snapshotDir, "packages")
	}
	entries, err := os.ReadDir(packagesDir)
	if err != nil {
		return nil, fmt.Errorf("list packages: %w", err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() && !strings.HasPrefix(e.Name(), ".") {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// LoadPackage loads one package by name (manifest + list of data streams with their paths).
func (l *Loader) LoadPackage(name string) (*Package, error) {
	src := l.integrationsDir
	if l.snapshotDir != "" {
		src = l.snapshotDir
	}
	pkgPath := filepath.Join(src, "packages", name)
	manifestPath := filepath.Join(pkgPath, "manifest.yml")
	manifest, err := readYAMLMap(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("load package %s manifest: %w", name, err)
	}
	dataStreamsDir := filepath.Join(pkgPath, "data_stream")
	streamEntries, err := os.ReadDir(dataStreamsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return &Package{Name: name, Path: pkgPath, Manifest: manifest, DataStreams: nil}, nil
		}
		return nil, fmt.Errorf("list data_streams for %s: %w", name, err)
	}
	var streams []DataStream
	for _, e := range streamEntries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		dsPath := filepath.Join(dataStreamsDir, e.Name())
		dsManifestPath := filepath.Join(dsPath, "manifest.yml")
		dsManifest, _ := readYAMLMap(dsManifestPath)
		pipelinePath := filepath.Join(dsPath, "elasticsearch", "ingest_pipeline")
		fieldsPath := filepath.Join(dsPath, "fields")
		var pipelineFiles, fieldFiles []string
		if fis, err := os.ReadDir(pipelinePath); err == nil {
			for _, f := range fis {
				if !f.IsDir() && (strings.HasSuffix(f.Name(), ".yml") || strings.HasSuffix(f.Name(), ".yaml") || strings.HasSuffix(f.Name(), ".json")) {
					pipelineFiles = append(pipelineFiles, filepath.Join(pipelinePath, f.Name()))
				}
			}
		}
		if fis, err := os.ReadDir(fieldsPath); err == nil {
			for _, f := range fis {
				if !f.IsDir() && (strings.HasSuffix(f.Name(), ".yml") || strings.HasSuffix(f.Name(), ".yaml")) {
					fieldFiles = append(fieldFiles, filepath.Join(fieldsPath, f.Name()))
				}
			}
		}
		streams = append(streams, DataStream{
			Name:         e.Name(),
			Path:         dsPath,
			Manifest:     dsManifest,
			PipelinePath: pipelinePath,
			FieldsPath:   fieldsPath,
			PipelineFiles: pipelineFiles,
			FieldFiles:    fieldFiles,
		})
	}
	return &Package{Name: name, Path: pkgPath, Manifest: manifest, DataStreams: streams}, nil
}

// LoadPackages loads multiple packages by name. Skips missing packages and returns those that loaded.
func (l *Loader) LoadPackages(names []string) ([]*Package, error) {
	var pkgs []*Package
	for _, name := range names {
		pkg, err := l.LoadPackage(name)
		if err != nil {
			return pkgs, err
		}
		pkgs = append(pkgs, pkg)
	}
	return pkgs, nil
}

func readYAMLMap(path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	if err := yaml.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}
