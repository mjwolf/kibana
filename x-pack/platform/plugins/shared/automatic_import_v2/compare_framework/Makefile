# Automatic Import V2 Compare Framework
# Build, test, and run targets.

GO ?= go
BINDIR := bin

.PHONY: build test run seed compare-runs clean help

build:
	$(GO) build -o $(BINDIR)/run ./cmd/run
	$(GO) build -o $(BINDIR)/seed-samples ./cmd/seed-samples
	$(GO) build -o $(BINDIR)/compare-runs ./cmd/compare-runs

test:
	$(GO) test ./...

run: build
	$(BINDIR)/run -config config.yaml -verbose

seed: build
	$(BINDIR)/seed-samples -config config.yaml

compare-runs: build
	@echo "Usage: $(BINDIR)/compare-runs <run_dir1> <run_dir2> [-output report.json]"

clean:
	rm -rf $(BINDIR) output work

help:
	@echo "Targets:"
	@echo "  build        - Build run, seed-samples, compare-runs into $(BINDIR)/"
	@echo "  test         - Run tests"
	@echo "  run          - Build and run comparison (requires config.yaml)"
	@echo "  seed         - Build and run seed-samples"
	@echo "  compare-runs - Build compare-runs (invoke with run dirs)"
	@echo "  clean        - Remove $(BINDIR), output, work"
	@echo "  help         - This help"
