# Automatic Import V2 Compare Framework

Standalone test framework for the LLM-driven Automatic Import V2 package generator. It drives AIv2 via HTTP only, loads golden packages from the elastic/integrations repo, and compares generated outputs to golden using programmatic comparison and Gemini (pipeline equivalence, processor choice, ECS/vendor field comparison).

## Requirements

- Go 1.25+
- `elastic-package` on PATH (for pipeline execution)
- `GOOGLE_API_KEY` env var (Gemini API key)
- Kibana with Automatic Import V2 API (plugin enabled). For local/dev use you must allow external access to internal APIs so the compare framework can call the API. In Kibana’s config (e.g. `config/kibana.dev.yml`) set:
  ```yaml
  server.restrictInternalApis: false
  ```

## Build and run (Makefile)

```bash
make build    # build binaries into bin/
make test     # run tests
make run      # build and run comparison (requires config.yaml)
make seed     # build and run seed-samples
make clean    # remove bin/, output/, work/
```

Or with Go directly: `go build ./...`

## Commands

- **Run comparison**: `go run ./cmd/run -config config.yaml` (or use default config path)
- **Seed samples**: `go run ./cmd/seed-samples -config config.yaml`
- **Compare runs**: `go run ./cmd/compare-runs output/run_XXX output/run_YYY [-output report.json]`
- **Consistency mode**: `go run ./cmd/run -config config.yaml -consistency`

## CLI flags (run)

| Flag | Description |
|------|-------------|
| `-config` | Path to config YAML (default: config.yaml) |
| `-package` | Run only this package (overrides config) |
| `-data-stream` | Run only this data stream (with -package) |
| `-re-run-from` | Re-run using config and package list from this run dir |
| `-dry-run` | Skip LLM stages; run only programmatic and pipeline execution |
| `-consistency` | Run consistency mode (N runs, pairwise compare) |
| `-verbose` | Log each package/data stream result (status and error) to stderr |

## Config

Copy `config.example.yaml` and set `integrations_dir`, `kibana_url`, `connector_id`, and auth. See the file for all options.

## Exit codes

- **0**: All selected packages/data streams pass (above pass threshold).
- **1**: Any package failed, any stage failed, or critical error (invalid config, no packages to run).

CI can rely on these exit codes to fail the build.

## Report

Each run writes to `output/run_<timestamp>/`:

- **report.md** – Human-readable summary (overview, pass/fail/skip counts, list of issues).
- **report.json** – Machine-readable report for CI: `summary` (pass_count, fail_count, skip_count, overall_score, overview), `details` (issues with category, location, severity, message, reasoning; stages array), `timestamp`. Use this to fail the build or parse issues programmatically.
