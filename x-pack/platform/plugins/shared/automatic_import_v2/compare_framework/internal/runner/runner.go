// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package runner

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"compare_framework/internal/client"
	"compare_framework/internal/config"
	"compare_framework/internal/golden"
	"compare_framework/internal/samples"
)

// RunResult is the outcome for one package/data stream.
type RunResult struct {
	Package     string `json:"package"`
	DataStream  string `json:"data_stream"`
	Status      string `json:"status"` // pass, skip, fail
	IntegrationID string `json:"integration_id,omitempty"`
	DataStreamID  string `json:"data_stream_id,omitempty"`
	Error        string `json:"error,omitempty"`
	ResultPath   string `json:"result_path,omitempty"`
	GoldenPath   string `json:"golden_path,omitempty"`
}

// RunSummary is the summary for a full run.
type RunSummary struct {
	Timestamp   time.Time   `json:"timestamp"`
	PassCount   int         `json:"pass_count"`
	SkipCount   int         `json:"skip_count"`
	FailCount   int         `json:"fail_count"`
	Results     []RunResult `json:"results"`
	PromptVersion string    `json:"prompt_version,omitempty"`
	KibanaURL   string      `json:"kibana_url,omitempty"`
	ConnectorID string      `json:"connector_id,omitempty"`
}

// Runner runs the golden comparison: create integration, upload samples, poll, fetch results.
type Runner struct {
	cfg    *config.Config
	client *client.Client
	loader *golden.Loader
}

// New creates a runner.
func New(cfg *config.Config) *Runner {
	auth := client.AuthHeader(cfg.Auth.Type, cfg.Auth.APIKey, cfg.Auth.Username, cfg.Auth.Password)
	cl := client.New(cfg.KibanaURL, auth, cfg.HTTPTimeout, cfg.HTTPRetries)
	return &Runner{cfg: cfg, client: cl, loader: golden.NewLoader(cfg.IntegrationsDir, cfg.GoldenSnapshotDir)}
}

// Run executes the comparison for the configured packages. runDir is the output directory (e.g. output/run_<timestamp>).
// It creates integration + data stream per package/data stream, uploads samples from samplesDir (samples/<pkg>/<ds>/samples.log or samples.ndjson),
// polls until completed/failed, fetches results, writes result.json and golden.json. Returns summary and exit code (0 = all pass, 1 = any fail).
// Use integrationSuffix (e.g. "run_1") to create distinct integrations when running consistency mode. When verbose is true, progress is logged to stderr.
func (r *Runner) Run(runDir string, samplesDir string, integrationSuffix string, verbose bool) (*RunSummary, int, error) {
	if integrationSuffix == "" {
		integrationSuffix = time.Now().Format("20060102150405")
	}
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return nil, 1, err
	}
	// Persist config snapshot
	configPath := filepath.Join(runDir, "config.json")
	if b, err := json.MarshalIndent(r.cfg, "", "  "); err == nil {
		_ = os.WriteFile(configPath, b, 0644)
	}

	names, err := r.cfg.PackageList()
	if err != nil {
		return nil, 1, err
	}
	if len(names) == 0 {
		names, err = r.loader.ListPackages()
		if err != nil {
			return nil, 1, err
		}
	}
	pkgs, err := r.loader.LoadPackages(names)
	if err != nil {
		return nil, 1, err
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Loaded %d package(s), starting Kibana run (run dir: %s)\n", len(pkgs), runDir)
		r.client.Logf = func(format string, args ...interface{}) {
			fmt.Fprintf(os.Stderr, "[verbose] "+format+"\n", args...)
		}
	} else {
		r.client.Logf = nil
	}
	summary := &RunSummary{
		Timestamp:     time.Now().UTC(),
		Results:       nil,
		PromptVersion: "v1",
		KibanaURL:     r.cfg.KibanaURL,
		ConnectorID:   r.cfg.ConnectorID,
	}
	var exitCode int
	for _, pkg := range pkgs {
		for _, ds := range pkg.DataStreams {
			if !r.cfg.ShouldTestDataStream(pkg.Name, ds.Name) {
				if verbose {
					fmt.Fprintf(os.Stderr, "[verbose] Skipping data stream %s/%s (filtered by config)\n", pkg.Name, ds.Name)
				}
				continue
			}
			res := r.runOne(runDir, samplesDir, pkg.Name, ds.Name, integrationSuffix, verbose)
			summary.Results = append(summary.Results, res)
			switch res.Status {
			case "pass":
				summary.PassCount++
			case "skip":
				summary.SkipCount++
			default:
				summary.FailCount++
				exitCode = 1
			}
		}
	}
	if summary.FailCount > 0 {
		exitCode = 1
	}
	// Write summary
	summaryPath := filepath.Join(runDir, "summary.json")
	if b, err := json.MarshalIndent(summary, "", "  "); err == nil {
		_ = os.WriteFile(summaryPath, b, 0644)
	}
	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Kibana run finished: %d pass, %d fail, %d skip\n", summary.PassCount, summary.FailCount, summary.SkipCount)
	}
	// Cleanup: delete created integrations/data streams if configured
	if r.cfg.CleanupAfterRun {
		for _, res := range summary.Results {
			if res.IntegrationID != "" && res.DataStreamID != "" {
				_ = r.client.DeleteDataStream(res.IntegrationID, res.DataStreamID)
			}
		}
	}
	return summary, exitCode, nil
}

func (r *Runner) runOne(runDir, samplesDir, pkg, dataStream, integrationSuffix string, verbose bool) RunResult {
	res := RunResult{Package: pkg, DataStream: dataStream, Status: "fail"}
	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Starting data stream: %s/%s\n", pkg, dataStream)
	}
	samplesPath, err := samples.ResolveSamplesPath(samplesDir, pkg, dataStream)
	if err != nil {
		if r.cfg.SkipPackagesWithoutSamples {
			res.Status = "skip"
			res.Error = "no samples file"
			return res
		}
		res.Error = err.Error()
		return res
	}
	samplesData, err := readSamplesFile(samplesPath)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	if len(samplesData) == 0 && r.cfg.SkipPackagesWithoutSamples {
		res.Status = "skip"
		res.Error = "no samples"
		return res
	}

	// Each (pkg, dataStream) pair gets its own integration since the API supports one data stream per integration.
	integrationID := safeID(pkg + "_" + dataStream)
	dataStreamID := safeID(dataStream)
	if integrationSuffix != "" {
		integrationID = integrationID + "_" + integrationSuffix
		dataStreamID = dataStreamID + "_" + integrationSuffix
	}
	// Create integration with one data stream (no samples in create; we upload next)
	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Kibana API: creating integration %s with data stream %s\n", integrationID, dataStreamID)
	}
	title := pkg + " - " + dataStream
	createReq := client.CreateIntegrationRequest{
		IntegrationID:    integrationID,
		Title:            title,
		Description:      "Compare framework run: " + title,
		ConnectorID:      r.cfg.ConnectorID,
		LangSmithOptions: r.cfg.LangSmith,
		DataStreams: []client.CreateDataStreamReq{{
			DataStreamID: dataStreamID,
			Title:        dataStream,
			Description:  "Data stream for " + pkg + " / " + dataStream,
			InputTypes:   []client.InputType{{Name: "filestream"}},
		}},
	}
	if _, err := r.client.CreateIntegration(createReq); err != nil {
		res.Error = fmt.Sprintf("create integration: %v", err)
		return res
	}
	res.IntegrationID = integrationID
	res.DataStreamID = dataStreamID

	defer func() {
		if res.Status != "pass" && r.cfg.CleanupAfterRun {
			_ = r.client.DeleteDataStream(integrationID, dataStreamID)
		}
	}()

	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Kibana API: uploading %d samples to data stream %s\n", len(samplesData), dataStreamID)
	}
	if err := r.client.UploadSamples(integrationID, dataStreamID, samplesData, filepath.Base(samplesPath)); err != nil {
		res.Error = fmt.Sprintf("upload samples: %v", err)
		return res
	}
	// Poll using the integration detail endpoint (reliable status) instead of the results endpoint (garbled errors on 400).
	const maxPolls = 120
	const pollInterval = 5 * time.Second
	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Kibana API: polling data stream status (up to %d x %s)\n", maxPolls, pollInterval)
	}
	var dsStatus string
	for i := 0; i < maxPolls; i++ {
		time.Sleep(pollInterval)
		status, err := r.client.GetDataStreamStatus(integrationID, dataStreamID)
		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "[verbose] GetDataStreamStatus poll %d/%d: error: %v\n", i+1, maxPolls, err)
			}
			continue
		}
		dsStatus = status
		if verbose && (i == 0 || (i+1)%12 == 0) {
			fmt.Fprintf(os.Stderr, "[verbose] GetDataStreamStatus poll %d/%d: status=%s\n", i+1, maxPolls, status)
		}
		if status == "completed" || status == "failed" {
			if verbose {
				fmt.Fprintf(os.Stderr, "[verbose] Data stream status: %s (after %d polls)\n", status, i+1)
			}
			break
		}
	}
	if dsStatus == "failed" {
		res.Error = "data stream failed"
		return res
	}
	if dsStatus != "completed" {
		res.Error = fmt.Sprintf("timeout: data stream status is %q after %d polls", dsStatus, maxPolls)
		return res
	}
	// Fetch results now that status is completed.
	results, err := r.client.GetDataStreamResults(integrationID, dataStreamID)
	if err != nil {
		res.Error = fmt.Sprintf("get results: %v", err)
		return res
	}

	pkgDir := filepath.Join(runDir, "packages", pkg, dataStream)
	if err := os.MkdirAll(pkgDir, 0755); err != nil {
		res.Error = err.Error()
		return res
	}
	resultPath := filepath.Join(pkgDir, "result.json")
	// Write with keys expected by pipeline exec (ingestPipeline, pipeline_docs).
	resultFile := struct {
		IngestPipeline interface{} `json:"ingestPipeline"`
		PipelineDocs   interface{} `json:"pipeline_docs"`
	}{IngestPipeline: results.IngestPipeline, PipelineDocs: results.PipelineDocs}
	if b, err := json.MarshalIndent(resultFile, "", "  "); err == nil {
		_ = os.WriteFile(resultPath, b, 0644)
	}
	res.ResultPath = resultPath
	saveGeneratedPackage(runDir, pkg, dataStream, results)
	// Golden: write normalized golden pipeline/fields from loader (simplified: just placeholder path)
	goldenPath := filepath.Join(pkgDir, "golden.json")
	_ = os.WriteFile(goldenPath, []byte("{}"), 0644)
	res.GoldenPath = goldenPath
	res.Status = "pass"
	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Data stream %s/%s finished: pass (result.json written)\n", pkg, dataStream)
	}
	return res
}

func readSamplesFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var lines []string
	for _, s := range strings.Split(string(data), "\n") {
		s = strings.TrimSpace(s)
		if s != "" {
			lines = append(lines, s)
		}
	}
	return lines, nil
}

func safeID(s string) string {
	s = strings.ReplaceAll(s, "-", "_")
	if len(s) > 50 {
		s = s[:50]
	}
	return s
}

// saveGeneratedPackage writes the generated package under runDir/packages/<pkg>/ (manifest + data_stream/.../ingest_pipeline)
// and also creates runDir/packages/<pkg>_<ds>.zip with the same layout plus result.json for offline reuse.
func saveGeneratedPackage(runDir, pkg, dataStream string, results *client.DataStreamResults) {
	if results.IngestPipeline == nil {
		return
	}
	pkgRoot := filepath.Join(runDir, "packages", pkg)
	pipeDir := filepath.Join(pkgRoot, "data_stream", dataStream, "elasticsearch", "ingest_pipeline")
	if err := os.MkdirAll(pipeDir, 0755); err != nil {
		return
	}
	pipePath := filepath.Join(pipeDir, "default.json")
	pipeBytes, _ := json.MarshalIndent(results.IngestPipeline, "", "  ")
	_ = os.WriteFile(pipePath, pipeBytes, 0644)
	manifest := fmt.Sprintf("format_version: '1.0.0'\nname: %s\ntitle: %s\nversion: 1.0.0\n", pkg, pkg)
	manifestPath := filepath.Join(pkgRoot, "manifest.yml")
	_ = os.WriteFile(manifestPath, []byte(manifest), 0644)

	zipName := pkg + "_" + dataStream + ".zip"
	zipPath := filepath.Join(runDir, "packages", zipName)
	zipRoot := pkg + "_" + dataStream + "-1.0.0/"
	resultPath := filepath.Join(runDir, "packages", pkg, dataStream, "result.json")
	resultBytes, err := os.ReadFile(resultPath)
	if err != nil {
		return
	}
	f, err := os.Create(zipPath)
	if err != nil {
		return
	}
	defer f.Close()
	w := zip.NewWriter(f)
	addZipFile(w, zipRoot+"manifest.yml", []byte(manifest))
	addZipFile(w, zipRoot+"result.json", resultBytes)
	addZipFile(w, zipRoot+"data_stream/"+dataStream+"/elasticsearch/ingest_pipeline/default.json", pipeBytes)
	_ = w.Close()
}

func addZipFile(w *zip.Writer, name string, body []byte) {
	h := &zip.FileHeader{Name: name, Method: zip.Deflate}
	fw, _ := w.CreateHeader(h)
	_, _ = fw.Write(body)
}

// RunFromPackages loads pre-generated package zips from packagesDir, extracts them into runDir/packages/<pkg>/<ds>/,
// and returns a RunSummary with status "pass" for each. Used when pregenerated_packages_dir is set to skip Kibana API.
// If packagesDir is a run directory (contains a "packages" subdir), zips are read from packagesDir/packages.
func (r *Runner) RunFromPackages(runDir, packagesDir string, verbose bool) (*RunSummary, int, error) {
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return nil, 1, err
	}
	configPath := filepath.Join(runDir, "config.json")
	if b, err := json.MarshalIndent(r.cfg, "", "  "); err == nil {
		_ = os.WriteFile(configPath, b, 0644)
	}
	// If the given dir has a "packages" subdir (run dir layout), use that for .zip files.
	zipDir := packagesDir
	if entries, _ := os.ReadDir(packagesDir); len(entries) > 0 {
		hasZips := false
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".zip") {
				hasZips = true
				break
			}
		}
		if !hasZips {
			for _, e := range entries {
				if e.IsDir() && e.Name() == "packages" {
					zipDir = filepath.Join(packagesDir, "packages")
					break
				}
			}
		}
	}
	entries, err := os.ReadDir(zipDir)
	if err != nil {
		return nil, 1, fmt.Errorf("read pregenerated packages dir: %w", err)
	}
	summary := &RunSummary{
		Timestamp:     time.Now().UTC(),
		Results:       nil,
		PromptVersion: "v1",
		KibanaURL:     r.cfg.KibanaURL,
		ConnectorID:   r.cfg.ConnectorID,
	}
	var exitCode int
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".zip") {
			continue
		}
		zipPath := filepath.Join(zipDir, e.Name())
		res, err := loadPackageZip(runDir, zipPath, verbose)
		if err != nil {
			if verbose {
				fmt.Fprintf(os.Stderr, "[verbose] Skip %s: %v\n", e.Name(), err)
			}
			continue
		}
		summary.Results = append(summary.Results, res)
		summary.PassCount++
	}
	summaryPath := filepath.Join(runDir, "summary.json")
	if b, err := json.MarshalIndent(summary, "", "  "); err == nil {
		_ = os.WriteFile(summaryPath, b, 0644)
	}
	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Loaded %d package(s) from %s\n", summary.PassCount, zipDir)
	}
	return summary, exitCode, nil
}

// loadPackageZip extracts one zip into runDir/packages/<pkg>/<ds>/ and returns a RunResult.
// Zip layout: <pkg>_<ds>-1.0.0/manifest.yml, result.json, data_stream/<ds>/elasticsearch/ingest_pipeline/default.json
func loadPackageZip(runDir, zipPath string, verbose bool) (RunResult, error) {
	res := RunResult{Status: "pass"}
	zr, err := zip.OpenReader(zipPath)
	if err != nil {
		return res, err
	}
	defer zr.Close()
	var zipRoot string
	var resultBytes, pipeBytes []byte
	var manifestBytes []byte
	var dataStreamInPath string
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		name := f.Name
		if zipRoot == "" {
			idx := strings.Index(name, "/")
			if idx > 0 {
				zipRoot = name[:idx+1]
			}
		}
		if strings.HasSuffix(name, "result.json") {
			rc, err := f.Open()
			if err != nil {
				return res, err
			}
			resultBytes, _ = io.ReadAll(rc)
			rc.Close()
		}
		if strings.HasSuffix(name, "manifest.yml") {
			rc, err := f.Open()
			if err != nil {
				return res, err
			}
			manifestBytes, _ = io.ReadAll(rc)
			rc.Close()
		}
		if strings.Contains(name, "/data_stream/") && strings.HasSuffix(name, "elasticsearch/ingest_pipeline/default.json") {
			rc, err := f.Open()
			if err != nil {
				return res, err
			}
			pipeBytes, _ = io.ReadAll(rc)
			rc.Close()
			// data_stream/<ds>/elasticsearch/...
			rest := name[strings.Index(name, "/data_stream/")+len("/data_stream/"):]
			if idx := strings.Index(rest, "/"); idx > 0 {
				dataStreamInPath = rest[:idx]
			}
		}
	}
	if zipRoot == "" || len(resultBytes) == 0 || len(pipeBytes) == 0 {
		return res, fmt.Errorf("zip missing result.json or pipeline (root: %q)", zipRoot)
	}
	// Parse pkg and ds from root "cisco_ios_log-1.0.0/"
	rootName := strings.TrimSuffix(zipRoot, "/")
	rootName = strings.TrimSuffix(rootName, "-1.0.0")
	lastUnderscore := strings.LastIndex(rootName, "_")
	if lastUnderscore <= 0 {
		return res, fmt.Errorf("zip root name has no _ separator: %s", rootName)
	}
	pkg := rootName[:lastUnderscore]
	dataStream := rootName[lastUnderscore+1:]
	if dataStreamInPath != "" {
		dataStream = dataStreamInPath
	}
	res.Package = pkg
	res.DataStream = dataStream
	pkgDir := filepath.Join(runDir, "packages", pkg, dataStream)
	if err := os.MkdirAll(pkgDir, 0755); err != nil {
		return res, err
	}
	resultPath := filepath.Join(pkgDir, "result.json")
	if err := os.WriteFile(resultPath, resultBytes, 0644); err != nil {
		return res, err
	}
	res.ResultPath = resultPath
	pipeDir := filepath.Join(runDir, "packages", pkg, "data_stream", dataStream, "elasticsearch", "ingest_pipeline")
	if err := os.MkdirAll(pipeDir, 0755); err != nil {
		return res, err
	}
	if err := os.WriteFile(filepath.Join(pipeDir, "default.json"), pipeBytes, 0644); err != nil {
		return res, err
	}
	pkgRoot := filepath.Join(runDir, "packages", pkg)
	if len(manifestBytes) > 0 {
		_ = os.WriteFile(filepath.Join(pkgRoot, "manifest.yml"), manifestBytes, 0644)
	}
	goldenPath := filepath.Join(pkgDir, "golden.json")
	_ = os.WriteFile(goldenPath, []byte("{}"), 0644)
	res.GoldenPath = goldenPath
	if verbose {
		fmt.Fprintf(os.Stderr, "[verbose] Loaded package %s/%s from %s\n", pkg, dataStream, filepath.Base(zipPath))
	}
	return res, nil
}
