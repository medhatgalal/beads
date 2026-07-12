package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	var cfg bootstrapConfig
	flag.StringVar(&cfg.Host, "host", "127.0.0.1", "numeric loopback Dolt host")
	flag.IntVar(&cfg.Port, "port", 13360, "isolated loopback Dolt SQL port (3306 and 3307 are forbidden)")
	flag.StringVar(&cfg.Database, "database", "", "existing beads_perf_lab_ database")
	flag.StringVar(&cfg.ProjectID, "project-id", "", "expected project UUID stored in metadata")
	flag.StringVar(&cfg.LabID, "lab-id", "", "synthetic lab UUID")
	flag.StringVar(&cfg.PasswordFile, "password-file", "", "0600 password file for the per-project lab principal")
	flag.DurationVar(&cfg.Timeout, "timeout", 30*time.Second, "bootstrap timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Timeout)
	defer cancel()
	result, err := runBootstrap(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lab bootstrap failed: %v\n", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetEscapeHTML(true)
	if err := enc.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "lab bootstrap succeeded but result encoding failed")
		os.Exit(1)
	}
}
