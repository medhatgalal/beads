package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"
)

func main() {
	var (
		path        = flag.String("path", "", "new SQLite path for the isolated canonical-store soak")
		output      = flag.String("output", "", "optional new JSON evidence path (stdout when empty)")
		operations  = flag.Int("operations", 1_000_000, "number of terminal operations")
		producers   = flag.Int("producers", 400, "bounded producer/tombstone rows")
		batch       = flag.Int("batch", 1_000, "atomic evidence-acceleration batch size")
		sampleEvery = flag.Int("sample-every", 100_000, "sample interval")
		logicalRate = flag.Float64("logical-rate", 10, "modeled operations per second")
		outcomeSize = flag.Int("outcome-bytes", 256, "fixed terminal outcome payload bytes")
	)
	flag.Parse()
	if *path == "" {
		fatalf("--path is required")
	}
	config := soakConfig{
		Path:         *path,
		Operations:   *operations,
		Producers:    *producers,
		BatchSize:    *batch,
		SampleEvery:  *sampleEvery,
		LogicalRate:  *logicalRate,
		OutcomeBytes: *outcomeSize,
		MemoryBound:  256 << 20,
		Binding: Binding{
			CellID:      "isolated-retention-soak-cell",
			TeamID:      "isolated-retention-soak-team",
			ProjectID:   "ae-soak",
			LedgerEpoch: 1,
		},
		Policy: Policy{
			MaxProducers:           512,
			MaxReceipts:            20_000,
			MaxReceiptsPerProducer: 64,
			KeepRecentPerProducer:  16,
			MaxOutcomeBytes:        4 << 10,
			MinimumRetryHorizon:    time.Minute,
		},
	}
	report, err := runSoak(context.Background(), config)
	if err != nil {
		fatalf("retention soak: %v", err)
	}
	var destination io.Writer = os.Stdout
	var file *os.File
	if *output != "" {
		file, err = os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			fatalf("create evidence output: %v", err)
		}
		defer file.Close()
		destination = file
	}
	encoder := json.NewEncoder(destination)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(report); err != nil {
		fatalf("encode evidence: %v", err)
	}
	if file != nil {
		if err := file.Sync(); err != nil {
			fatalf("sync evidence: %v", err)
		}
	}
	if !report.Pass {
		os.Exit(2)
	}
}

func fatalf(format string, values ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
