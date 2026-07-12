// realistic-fleet drives only explicitly acknowledged synthetic loopback
// gateway targets. Closed-loop mode is the capacity candidate. Open-loop mode
// is intentionally labeled a non-capacity failure envelope.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func main() {
	var configPath, outputPath string
	var executeSynthetic bool
	flag.StringVar(&configPath, "config", "", "JSON configuration for synthetic loopback gateways")
	flag.StringVar(&outputPath, "output", "", "structured JSON result path")
	flag.BoolVar(&executeSynthetic, "execute-synthetic", false, "required acknowledgement before issuing synthetic writes")
	flag.Parse()
	if flag.NArg() != 0 || configPath == "" || outputPath == "" {
		fatalf("config, output, and no positional arguments are required")
	}
	if !executeSynthetic {
		fatalf("refusing to run without --execute-synthetic")
	}
	body, err := readPrivateRegular(configPath, 4<<20)
	if err != nil {
		fatalf("read config: %v", err)
	}
	cfg, err := parseConfig(body)
	if err != nil {
		fatalf("config: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(cfg.DurationSeconds)*time.Second+30*time.Minute,
	)
	defer cancel()
	result, err := run(ctx, cfg)
	if err != nil {
		fatalf("run: %v", err)
	}
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fatalf("marshal report: %v", err)
	}
	if err := writePrivateRegular(outputPath, append(encoded, '\n')); err != nil {
		fatalf("write report: %v", err)
	}
	fmt.Printf("wrote %s mode=%s capacity=%s passed=%v\n",
		outputPath, result.Mode, result.CapacityInterpretation, result.Passed)
	if !result.Passed {
		os.Exit(1)
	}
}

func readPrivateRegular(path string, maxBytes int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("path must be a regular non-symlink file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	body, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("file exceeds %d bytes", maxBytes)
	}
	return body, nil
}

func writePrivateRegular(path string, body []byte) error {
	if existing, err := os.Lstat(path); err == nil {
		if !existing.Mode().IsRegular() || existing.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("output must be a regular non-symlink file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(path)
	if info, err := os.Stat(parent); err != nil || !info.IsDir() {
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

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
