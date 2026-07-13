package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
)

func main() {
	var action, runID, producer, payload, failpoint, readyFile, output string
	var repositoryEpoch, producerEpoch, sequence, through uint64
	var startAtNS int64
	var ackSyntheticLab bool
	flag.StringVar(&action, "action", "", "proof, worker, register-worker, compact-worker, or inspect")
	flag.StringVar(&runID, "run-id", "", "synthetic proof UUID")
	flag.StringVar(&producer, "producer", "", "fixed synthetic producer")
	flag.StringVar(&payload, "payload", "", "bounded synthetic payload")
	flag.StringVar(&failpoint, "failpoint", "", "worker failpoint")
	flag.StringVar(&readyFile, "ready-file", "", "failpoint readiness file under the evidence root")
	flag.StringVar(&output, "output", "", "new structured evidence file under the evidence root")
	flag.Uint64Var(&repositoryEpoch, "repository-epoch", 0, "expected repository epoch")
	flag.Uint64Var(&producerEpoch, "producer-epoch", 0, "producer epoch")
	flag.Uint64Var(&sequence, "sequence", 0, "producer sequence")
	flag.Uint64Var(&through, "through", 0, "compaction watermark")
	flag.Int64Var(&startAtNS, "start-at-ns", 0, "worker barrier time")
	flag.BoolVar(&ackSyntheticLab, "ack-synthetic-lab", false, "required acknowledgement before proof setup mutates the attested disposable server")
	flag.Parse()
	parsedRun, err := uuid.Parse(runID)
	if err != nil || flag.NArg() != 0 {
		fatal(fmt.Errorf("run-id must be a UUID and positional arguments are forbidden"))
	}
	runID = strings.ToLower(parsedRun.String())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if action == "proof" && !ackSyntheticLab {
		fatal(fmt.Errorf("proof requires --ack-synthetic-lab"))
	}
	if err := verifyServerControlIdentity(ctx); err != nil {
		fatal(err)
	}
	switch action {
	case "proof":
		if output == "" {
			fatal(fmt.Errorf("proof requires a new output file"))
		}
		report, proofErr := runProof(ctx, runID)
		if proofErr != nil {
			report.Error = sanitizeError(proofErr)
			report.PromotionDecision = "HOLD_PROOF_FAILED"
		}
		if err := writeJSON(output, report); err != nil {
			fatal(err)
		}
		if proofErr != nil || !report.Passed {
			os.Exit(1)
		}
	case "worker":
		if output != "" || repositoryEpoch == 0 || producerEpoch == 0 || sequence == 0 ||
			!strings.HasPrefix(payload, operationPrefix) {
			fatal(fmt.Errorf("invalid bounded worker arguments"))
		}
		allowedFailpoint := failpoint == "" || failpoint == "after-start" ||
			failpoint == "after-mutations-before-commit" || failpoint == "after-dolt-commit-before-response" ||
			failpoint == "before-ledger-cas"
		if !allowedFailpoint || (failpoint != "" && !strings.HasPrefix(readyFile, evidenceRoot+"/")) {
			fatal(fmt.Errorf("invalid worker failpoint"))
		}
		if startAtNS > 0 {
			wait := time.Until(time.Unix(0, startAtNS))
			if wait > 0 {
				select {
				case <-ctx.Done():
					fatal(ctx.Err())
				case <-time.After(wait):
				}
			}
		}
		ring, _ := ringAt(1)
		in, err := newRequest(runID, producer, repositoryEpoch, producerEpoch, sequence, payload, ring)
		if err != nil {
			fatal(err)
		}
		db, err := openDB(labDatabase, 1)
		if err != nil {
			fatal(err)
		}
		defer db.Close()
		result, err := executeOperation(ctx, db, ring, in, failpoint, readyFile)
		if err != nil {
			fatal(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			fatal(err)
		}
	case "compact-worker":
		allowed := failpoint == "after-watermark-before-delete" || failpoint == "after-delete-before-commit"
		if output != "" || repositoryEpoch == 0 || producerEpoch == 0 || through == 0 || !allowed || !isProofProducer(producer) ||
			!strings.HasPrefix(readyFile, evidenceRoot+"/") {
			fatal(fmt.Errorf("invalid bounded compaction worker arguments"))
		}
		db, err := openDB(labDatabase, 1)
		if err != nil {
			fatal(err)
		}
		defer db.Close()
		if err := compactProducer(ctx, db, runID, producer, repositoryEpoch, producerEpoch, through, failpoint, readyFile); err != nil {
			fatal(err)
		}
	case "register-worker":
		if output != "" || repositoryEpoch == 0 || producerEpoch == 0 || sequence != 0 || through != 0 ||
			payload != "" || !isProofProducer(producer) {
			fatal(fmt.Errorf("invalid bounded registration worker arguments"))
		}
		if failpoint != "before-ledger-cas" || !strings.HasPrefix(readyFile, evidenceRoot+"/") {
			fatal(fmt.Errorf("registration worker requires deterministic ledger barrier"))
		}
		if startAtNS > 0 {
			wait := time.Until(time.Unix(0, startAtNS))
			if wait > 0 {
				select {
				case <-ctx.Done():
					fatal(ctx.Err())
				case <-time.After(wait):
				}
			}
		}
		db, err := openDB(labDatabase, 1)
		if err != nil {
			fatal(err)
		}
		defer db.Close()
		result, err := registerProducerWithBarrier(ctx, db, runID, producer, repositoryEpoch, producerEpoch, subjectHash(producer), readyFile)
		if err != nil {
			fatal(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
			fatal(err)
		}
	case "inspect":
		if output == "" || repositoryEpoch == 0 {
			fatal(fmt.Errorf("inspect requires output and expected repository epoch"))
		}
		db, err := openDB(labDatabase, 1)
		if err != nil {
			fatal(err)
		}
		defer db.Close()
		result, err := inspectRun(ctx, db, runID, repositoryEpoch)
		if err != nil {
			fatal(err)
		}
		if err := writeJSON(output, result); err != nil {
			fatal(err)
		}
		if !result.Passed {
			os.Exit(1)
		}
	default:
		fatal(fmt.Errorf("action must be proof, worker, register-worker, compact-worker, or inspect"))
	}
}

func writeJSON(path string, value any) error {
	clean := filepath.Clean(path)
	if !strings.HasPrefix(clean, evidenceRoot+string(os.PathSeparator)) {
		return fmt.Errorf("output path must be under the fixed evidence root")
	}
	file, err := os.OpenFile(clean, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return err
	}
	return file.Sync()
}

func fatal(err error) {
	if err == nil {
		return
	}
	_, _ = io.WriteString(os.Stderr, "retention-dolt: "+sanitizeError(err)+"\n")
	os.Exit(1)
}
