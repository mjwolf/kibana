# Automatic Import V2 Compare Framework
# Build, test, and run targets. Binaries are rebuilt only when Go sources change.

GO ?= go
BINDIR := bin
GO_SRCS := $(shell find ./cmd ./internal -name '*.go')

.PHONY: build test run seed compare-runs clean help

$(BINDIR)/run: $(GO_SRCS)
	$(GO) build -o $(BINDIR)/run ./cmd/run

$(BINDIR)/seed-samples: $(GO_SRCS)
	$(GO) build -o $(BINDIR)/seed-samples ./cmd/seed-samples

$(BINDIR)/compare-runs: $(GO_SRCS)
	$(GO) build -o $(BINDIR)/compare-runs ./cmd/compare-runs

build: $(BINDIR)/run $(BINDIR)/seed-samples $(BINDIR)/compare-runs

test:
	$(GO) test ./...

run: $(BINDIR)/run
	$(BINDIR)/run -config config.yaml -verbose

seed: $(BINDIR)/seed-samples
	$(BINDIR)/seed-samples -config config.yaml

compare-runs: $(BINDIR)/compare-runs
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
