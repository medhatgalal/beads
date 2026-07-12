// fleet-inspect independently verifies the exact durable effects of one
// complete realistic-fleet report. It accepts no arbitrary SQL and connects
// only to explicitly inventoried numeric loopback Dolt endpoints.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	var inventoryPath, runnerPath, outputPath string
	flag.StringVar(&inventoryPath, "inventory", "", "0600 JSON inventory of isolated loopback databases and queues")
	flag.StringVar(&runnerPath, "runner-report", "", "complete realistic-fleet schema-v2 JSON report")
	flag.StringVar(&outputPath, "output", "", "0600 JSON inspection result path")
	flag.Parse()
	if flag.NArg() != 0 || inventoryPath == "" || runnerPath == "" || outputPath == "" {
		fatalf("--inventory, --runner-report, --output, and no positional arguments are required")
	}
	inventoryBody, err := readGuardedFile(inventoryPath, 4<<20, true)
	if err != nil {
		fatalf("inventory rejected: %v", err)
	}
	defer zero(inventoryBody)
	runnerBody, err := readGuardedFile(runnerPath, 128<<20, false)
	if err != nil {
		fatalf("runner report rejected: %v", err)
	}
	defer zero(runnerBody)
	inv, err := parseInventory(inventoryBody)
	if err != nil {
		fatalf("inventory rejected: %v", err)
	}
	runner, err := parseRunnerReport(runnerBody)
	if err != nil {
		fatalf("runner report rejected: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	result, err := inspectFleet(ctx, inv, runner, inventoryBody, runnerBody)
	if err != nil {
		fatalf("inspection rejected: %v", err)
	}
	body, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fatalf("encode inspection result")
	}
	body = append(body, '\n')
	if err := writePrivateFile(outputPath, body); err != nil {
		fatalf("write inspection result: %v", err)
	}
	zero(body)
	fmt.Printf("wrote %s databases=%d operations=%d passed=%v\n", outputPath, len(result.Databases), result.ExpectedOperations, result.Passed)
	if !result.Passed {
		os.Exit(1)
	}
}

func writePrivateFile(path string, body []byte) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("output path must be absolute")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("output must be a regular non-symlink file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if err := rejectSymlinkComponents(parent); err != nil {
		return err
	}
	info, err := os.Stat(parent)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("output parent must already exist")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Write(body); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func fatalf(format string, values ...any) {
	fmt.Fprintf(os.Stderr, "fleet-inspect: "+format+"\n", values...)
	os.Exit(2)
}
