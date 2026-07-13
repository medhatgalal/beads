package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

const evidenceRoot = "/private/tmp/beads-fleet-scale-20260712"

func workerArgs(envelope workerEnvelope) []string {
	args := []string{
		"--action", "worker",
		"--run-id", envelope.RunID,
		"--producer", envelope.ProducerID,
		"--producer-epoch", strconv.FormatUint(envelope.ProducerEpoch, 10),
		"--sequence", strconv.FormatUint(envelope.Sequence, 10),
		"--repository-epoch", strconv.FormatUint(envelope.RepositoryEpoch, 10),
		"--payload", envelope.Payload,
	}
	if envelope.StartAtUnixNano != 0 {
		args = append(args, "--start-at-ns", strconv.FormatInt(envelope.StartAtUnixNano, 10))
	}
	if envelope.Failpoint != "" {
		args = append(args, "--failpoint", envelope.Failpoint, "--ready-file", envelope.ReadyFile)
	}
	return args
}

type processOutcome struct {
	result operationResult
	err    error
	stderr string
}

func singleWorker(ctx context.Context, envelope workerEnvelope) (operationResult, error) {
	executable, err := os.Executable()
	if err != nil {
		return operationResult{}, err
	}
	command := exec.CommandContext(ctx, executable, workerArgs(envelope)...)
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Run(); err != nil {
		return operationResult{}, fmt.Errorf("worker failed: %v: %s", err, stderr.String())
	}
	var result operationResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return operationResult{}, fmt.Errorf("decode worker result: %w", err)
	}
	return result, nil
}

func concurrentWorkers(ctx context.Context, envelope workerEnvelope) ([]operationResult, error) {
	return concurrentWorkerEnvelopes(ctx, []workerEnvelope{envelope, envelope})
}

func concurrentWorkerEnvelopes(ctx context.Context, envelopes []workerEnvelope) ([]operationResult, error) {
	if len(envelopes) != 2 {
		return nil, fmt.Errorf("concurrency proof requires exactly two workers")
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	startAt := time.Now().Add(800 * time.Millisecond).UnixNano()
	runtimeDir := filepath.Join(evidenceRoot, "retention-dolt-runtime-"+envelopes[0].RunID[:8])
	readyFiles := make([]string, 0, len(envelopes))
	type running struct {
		command *exec.Cmd
		stdout  bytes.Buffer
		stderr  bytes.Buffer
	}
	runningWorkers := make([]*running, 0, len(envelopes))
	for index, envelope := range envelopes {
		envelope.StartAtUnixNano = startAt
		envelope.Failpoint = "before-ledger-cas"
		envelope.ReadyFile = filepath.Join(runtimeDir, fmt.Sprintf("operation-cap-%d-%s.ready", index, envelope.ProducerID))
		readyFiles = append(readyFiles, envelope.ReadyFile)
		worker := &running{}
		worker.command = exec.CommandContext(ctx, executable, workerArgs(envelope)...)
		worker.command.Stdout = &worker.stdout
		worker.command.Stderr = &worker.stderr
		if err := worker.command.Start(); err != nil {
			for _, started := range runningWorkers {
				_ = started.command.Process.Kill()
				_ = started.command.Wait()
			}
			return nil, err
		}
		runningWorkers = append(runningWorkers, worker)
	}
	if err := waitAndReleaseLedgerBarriers(ctx, readyFiles); err != nil {
		for _, started := range runningWorkers {
			_ = started.command.Process.Kill()
			_ = started.command.Wait()
		}
		return nil, err
	}
	outcomes := make(chan processOutcome, len(runningWorkers))
	for _, worker := range runningWorkers {
		go func(worker *running) {
			err := worker.command.Wait()
			var result operationResult
			if err == nil {
				err = json.Unmarshal(worker.stdout.Bytes(), &result)
			}
			outcomes <- processOutcome{result: result, err: err, stderr: worker.stderr.String()}
		}(worker)
	}
	results := make([]operationResult, 0, len(runningWorkers))
	for range runningWorkers {
		outcome := <-outcomes
		if outcome.err != nil {
			return nil, fmt.Errorf("worker failed: %v: %s", outcome.err, outcome.stderr)
		}
		results = append(results, outcome.result)
	}
	return results, nil
}

func registrationArgs(envelope registrationEnvelope) []string {
	args := []string{
		"--action", "register-worker",
		"--run-id", envelope.RunID,
		"--producer", envelope.ProducerID,
		"--producer-epoch", strconv.FormatUint(envelope.ProducerEpoch, 10),
		"--repository-epoch", strconv.FormatUint(envelope.RepositoryEpoch, 10),
	}
	if envelope.StartAtUnixNano != 0 {
		args = append(args, "--start-at-ns", strconv.FormatInt(envelope.StartAtUnixNano, 10))
	}
	if envelope.Failpoint != "" {
		args = append(args, "--failpoint", envelope.Failpoint, "--ready-file", envelope.ReadyFile)
	}
	return args
}

type registrationProcessOutcome struct {
	result registrationResult
	err    error
	stderr string
}

func concurrentRegistrationWorkers(ctx context.Context, envelopes []registrationEnvelope) ([]registrationResult, error) {
	if len(envelopes) != 2 {
		return nil, fmt.Errorf("registration concurrency proof requires exactly two workers")
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	startAt := time.Now().Add(800 * time.Millisecond).UnixNano()
	runtimeDir := filepath.Join(evidenceRoot, "retention-dolt-runtime-"+envelopes[0].RunID[:8])
	readyFiles := make([]string, 0, len(envelopes))
	type running struct {
		command *exec.Cmd
		stdout  bytes.Buffer
		stderr  bytes.Buffer
	}
	runningWorkers := make([]*running, 0, len(envelopes))
	for index, envelope := range envelopes {
		envelope.StartAtUnixNano = startAt
		envelope.Failpoint = "before-ledger-cas"
		envelope.ReadyFile = filepath.Join(runtimeDir, fmt.Sprintf("producer-cap-%d-%s.ready", index, envelope.ProducerID))
		readyFiles = append(readyFiles, envelope.ReadyFile)
		worker := &running{}
		worker.command = exec.CommandContext(ctx, executable, registrationArgs(envelope)...)
		worker.command.Stdout = &worker.stdout
		worker.command.Stderr = &worker.stderr
		if err := worker.command.Start(); err != nil {
			for _, started := range runningWorkers {
				_ = started.command.Process.Kill()
				_ = started.command.Wait()
			}
			return nil, err
		}
		runningWorkers = append(runningWorkers, worker)
	}
	if err := waitAndReleaseLedgerBarriers(ctx, readyFiles); err != nil {
		for _, started := range runningWorkers {
			_ = started.command.Process.Kill()
			_ = started.command.Wait()
		}
		return nil, err
	}
	outcomes := make(chan registrationProcessOutcome, len(runningWorkers))
	for _, worker := range runningWorkers {
		go func(worker *running) {
			err := worker.command.Wait()
			var result registrationResult
			if err == nil {
				err = json.Unmarshal(worker.stdout.Bytes(), &result)
			}
			outcomes <- registrationProcessOutcome{result: result, err: err, stderr: worker.stderr.String()}
		}(worker)
	}
	results := make([]registrationResult, 0, len(runningWorkers))
	for range runningWorkers {
		outcome := <-outcomes
		if outcome.err != nil {
			return nil, fmt.Errorf("registration worker failed: %v: %s", outcome.err, outcome.stderr)
		}
		results = append(results, outcome.result)
	}
	return results, nil
}

func waitAndReleaseLedgerBarriers(ctx context.Context, readyFiles []string) error {
	deadline := time.Now().Add(10 * time.Second)
	for _, readyFile := range readyFiles {
		for {
			if _, err := os.Stat(readyFile); err == nil {
				break
			} else if !os.IsNotExist(err) {
				return err
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("worker did not reach deterministic ledger barrier")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	for _, readyFile := range readyFiles {
		releaseFile := readyFile + ".release"
		file, err := os.OpenFile(releaseFile, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		if _, err := file.WriteString("release\n"); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

func killedWorker(ctx context.Context, runtimeDir string, envelope workerEnvelope) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	if envelope.Failpoint == "" {
		return fmt.Errorf("killed worker requires a failpoint")
	}
	envelope.ReadyFile = filepath.Join(runtimeDir, envelope.ProducerID+"-"+envelope.Failpoint+".ready")
	command := exec.CommandContext(ctx, executable, workerArgs(envelope)...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(envelope.ReadyFile); err == nil {
			break
		} else if !os.IsNotExist(err) {
			_ = command.Process.Kill()
			_ = command.Wait()
			return err
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			return fmt.Errorf("worker did not reach failpoint: %s", stderr.String())
		}
		select {
		case <-ctx.Done():
			_ = command.Process.Kill()
			_ = command.Wait()
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := command.Process.Kill(); err != nil {
		return fmt.Errorf("kill worker at %s: %w", envelope.Failpoint, err)
	}
	if err := command.Wait(); err == nil {
		return fmt.Errorf("killed worker exited successfully")
	}
	return nil
}

func killedCompaction(ctx context.Context, runtimeDir, runID, producer, failpoint string, repositoryEpoch, producerEpoch, through uint64) error {
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	readyFile := filepath.Join(runtimeDir, producer+"-"+failpoint+".ready")
	args := []string{
		"--action", "compact-worker", "--run-id", runID, "--producer", producer,
		"--repository-epoch", strconv.FormatUint(repositoryEpoch, 10),
		"--producer-epoch", strconv.FormatUint(producerEpoch, 10),
		"--through", strconv.FormatUint(through, 10),
		"--failpoint", failpoint, "--ready-file", readyFile,
	}
	command := exec.CommandContext(ctx, executable, args...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(readyFile); err == nil {
			break
		} else if !os.IsNotExist(err) {
			_ = command.Process.Kill()
			_ = command.Wait()
			return err
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			return fmt.Errorf("compaction worker did not reach failpoint: %s", stderr.String())
		}
		select {
		case <-ctx.Done():
			_ = command.Process.Kill()
			_ = command.Wait()
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	if err := command.Process.Kill(); err != nil {
		return err
	}
	if err := command.Wait(); err == nil {
		return fmt.Errorf("killed compaction worker exited successfully")
	}
	return nil
}

func createRuntimeDir(runID string) (string, error) {
	directory := filepath.Join(evidenceRoot, "retention-dolt-runtime-"+runID[:8])
	if err := os.Mkdir(directory, 0o700); err != nil {
		return "", err
	}
	return directory, nil
}
