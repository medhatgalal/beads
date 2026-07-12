package main

import (
	"fmt"
	"math"
	"time"
)

const (
	gatePass    = "pass"
	gateFail    = "fail"
	gateUnknown = "unknown"

	// At each profile's minimum graph-complete duration the model expects at
	// least 880 point commands. A 10% band is wider than deterministic renewal
	// variance while remaining narrow enough to reject the observed 18%
	// startup under-drive. The upper bound also prevents a mislabeled stress run
	// from being promoted as the frozen realistic profile. At that same smallest
	// run, five percentage points exceed three binomial standard deviations for
	// read/hot share, and three points do so for the rarer hot-write share.
	loadFidelityLowerRatio = 0.90
	loadFidelityUpperRatio = 1.10
	readMixTolerance       = 0.05
	hotMixTolerance        = 0.05
	hotWriteMixTolerance   = 0.03
)

type gateEvidence struct {
	Status   string `json:"status"`
	Required bool   `json:"required"`
	Evidence string `json:"evidence"`
}

func evaluateGates(cfg config, durationSufficient bool, value report) map[string]gateEvidence {
	gates := map[string]gateEvidence{}
	set := func(name, status, evidence string) {
		gates[name] = gateEvidence{Status: status, Required: true, Evidence: evidence}
	}
	if cfg.Mode == modeClosedLoop {
		set("capacity_mode", gatePass, "realistic closed-loop workload")
	} else {
		set("capacity_mode", gateFail, "open-loop overload is a failure envelope, not capacity evidence")
	}
	if durationSufficient {
		set("profile_duration", gatePass, "run covered at least one complete fixed graph schedule window")
	} else {
		set("profile_duration", gateUnknown, "run was shorter than the profile's fixed graph schedule window")
	}
	if value.Metrics.AccountingBalanced {
		set("outcome_accounting", gatePass, fmt.Sprintf(
			"generated=%d completed=%d dropped=%d unclassified=%d",
			value.Metrics.Generated, value.Metrics.Completed,
			value.Metrics.Dropped, value.Metrics.Unclassified,
		))
	} else {
		set("outcome_accounting", gateFail, fmt.Sprintf(
			"generated=%d does not equal completed+dropped+unclassified=%d",
			value.Metrics.Generated,
			value.Metrics.Completed+value.Metrics.Dropped+value.Metrics.Unclassified,
		))
	}
	if value.Attestation.Required != value.Config.RegisteredDatabases ||
		value.Attestation.Verified != value.Attestation.Required {
		set("cold_inventory", gateFail, "not every registered target has a verified synthetic attestation")
	} else if value.ResourceBefore.Gateway.TargetsSampled != value.Config.RegisteredDatabases ||
		value.ResourceBefore.Gateway.SampleErrors != 0 {
		set("cold_inventory", gateFail, fmt.Sprintf(
			"cold samples=%d errors=%d registered=%d",
			value.ResourceBefore.Gateway.TargetsSampled,
			value.ResourceBefore.Gateway.SampleErrors,
			value.Config.RegisteredDatabases,
		))
	} else {
		set("cold_inventory", gatePass, "all registered targets attested and produced a cold pool sample")
	}
	setCorrectnessGate(gates, value)
	setLoadFidelityGate(gates, cfg, durationSufficient, value)
	setStopBacklogGate(gates, cfg, value)
	setInspectorInputGate(gates, value)
	setLatencyGate(gates, cfg, durationSufficient, value)
	set("utilization", gateUnknown,
		"gateway pool counters omit process CPU, RSS, file descriptors, disk IO, and per-target saturation")
	set("isolation", gateUnknown,
		"origin/destination attribution is measured, but cross-database non-interference requires an independent inspector")
	if value.BoundedEvidence.SampleRecords.Truncated {
		set("independent_database_effect", gateUnknown,
			"sample records are incomplete; an inspector cannot prove every write effect")
	} else {
		set("independent_database_effect", gateUnknown,
			"complete operation identities are available, but no independent inspector verified exact issue, edge, receipt, outbox, and commit effects")
	}
	set("recovery", gateUnknown,
		"interrupted-operation reconciliation and deterministic restart recovery were not exercised by this run")
	return gates
}

func setCorrectnessGate(gates map[string]gateEvidence, value report) {
	set := func(status, evidence string) {
		gates["correctness"] = gateEvidence{Status: status, Required: true, Evidence: evidence}
	}
	if value.Correctness.DuplicateOperationIDs > 0 || value.Correctness.ProjectMismatches > 0 ||
		value.Correctness.TerminalFailures > 0 || value.Correctness.HTTPFailures > 0 ||
		value.Correctness.InvalidJSON > 0 || value.Correctness.DroppedAtAdmission > 0 ||
		value.Correctness.MissingOperationIDs > 0 {
		set(gateFail, fmt.Sprintf("observed correctness/failure counters total=%d", correctnessTotal(value.Correctness)))
		return
	}
	if value.Metrics.Unclassified > 0 || value.Correctness.NonterminalTimeouts > 0 {
		set(gateUnknown, "one or more operations ended without a classified terminal outcome")
		return
	}
	if value.BoundedEvidence.OperationIDProof.Truncated {
		set(gateUnknown, "exact operation-ID duplicate proof was truncated by its bounded store")
		return
	}
	set(gatePass, "all observed outcomes classified and exact operation-ID proof remained complete")
}

func setLoadFidelityGate(
	gates map[string]gateEvidence, cfg config, durationSufficient bool, value report,
) {
	set := func(status, evidence string) {
		gates["load_fidelity"] = gateEvidence{Status: status, Required: true, Evidence: evidence}
	}
	if cfg.Mode != modeClosedLoop {
		set(gateUnknown, "the frozen realistic model applies only to closed-loop mode")
		return
	}
	if !durationSufficient {
		set(gateUnknown, "profile duration was insufficient for a complete fixed graph schedule")
		return
	}
	if value.Metrics.LoadWindowSeconds <= 0 {
		set(gateFail, "load window duration was not positive")
		return
	}
	profile := frozenProfiles[cfg.Profile]
	expectedRate := expectedCommandRate(profile, assumedResponseSeconds)
	pointCommands := value.Metrics.Generated - value.Metrics.Graphs
	if pointCommands < 0 || expectedRate <= 0 {
		set(gateFail, "point-command accounting or frozen expected rate was invalid")
		return
	}
	actualRate := float64(pointCommands) / value.Metrics.LoadWindowSeconds
	ratio := actualRate / expectedRate
	schedule, _ := absoluteGraphSchedule(profile, time.Duration(cfg.DurationSeconds)*time.Second)
	if value.Metrics.Graphs != int64(len(schedule)) {
		set(gateFail, fmt.Sprintf(
			"graph schedule mismatch: completed=%d expected=%d", value.Metrics.Graphs, len(schedule),
		))
		return
	}
	if value.Metrics.ReadCommands+value.Metrics.PointWrites != pointCommands {
		set(gateFail, fmt.Sprintf(
			"point mix accounting mismatch: reads=%d writes=%d point_commands=%d",
			value.Metrics.ReadCommands, value.Metrics.PointWrites, pointCommands,
		))
		return
	}
	if pointCommands == 0 {
		set(gateFail, "point-command count was zero")
		return
	}
	readShare := float64(value.Metrics.ReadCommands) / float64(pointCommands)
	hotShare := float64(value.Metrics.HotAEPointCommands) / float64(pointCommands)
	hotWriteShare := float64(value.Metrics.HotAEPointWrites) / float64(pointCommands)
	expectedHotWriteShare := (1 - profile.ReadFraction) * profile.HotAEShare
	if math.Abs(readShare-profile.ReadFraction) > readMixTolerance ||
		math.Abs(hotShare-profile.HotAEShare) > hotMixTolerance ||
		math.Abs(hotWriteShare-expectedHotWriteShare) > hotWriteMixTolerance {
		set(gateFail, fmt.Sprintf(
			"point mix drift: read=%.3f/%.3f (tol %.2f), hot=%.3f/%.3f (tol %.2f), hot_write=%.3f/%.3f (tol %.2f)",
			readShare, profile.ReadFraction, readMixTolerance,
			hotShare, profile.HotAEShare, hotMixTolerance,
			hotWriteShare, expectedHotWriteShare, hotWriteMixTolerance,
		))
		return
	}
	if ratio < loadFidelityLowerRatio || ratio > loadFidelityUpperRatio {
		set(gateFail, fmt.Sprintf(
			"point load %.3f/s is %.3fx frozen model %.3f/s; required band is %.2f-%.2fx (graphs=%d)",
			actualRate, ratio, expectedRate, loadFidelityLowerRatio, loadFidelityUpperRatio,
			value.Metrics.Graphs,
		))
		return
	}
	set(gatePass, fmt.Sprintf(
		"point load %.3f/s is %.3fx frozen model %.3f/s within %.2f-%.2fx; read/hot/hot-write shares %.3f/%.3f/%.3f; graph schedule completed %d/%d",
		actualRate, ratio, expectedRate, loadFidelityLowerRatio, loadFidelityUpperRatio,
		readShare, hotShare, hotWriteShare, value.Metrics.Graphs, len(schedule),
	))
}

func setStopBacklogGate(gates map[string]gateEvidence, cfg config, value report) {
	set := func(status, evidence string) {
		gates["stop_backlog"] = gateEvidence{Status: status, Required: true, Evidence: evidence}
	}
	if !value.Metrics.StopAccountingValid || value.Metrics.BacklogAtStop < 0 {
		set(gateFail, fmt.Sprintf("invalid negative deadline backlog=%d", value.Metrics.BacklogAtStop))
		return
	}
	if value.Metrics.BacklogAtStop > int64(cfg.MaxInflight) {
		set(gateFail, fmt.Sprintf(
			"deadline backlog=%d exceeds max_inflight=%d and therefore includes hidden client-side queueing; drain=%.3f ms",
			value.Metrics.BacklogAtStop, cfg.MaxInflight, value.Metrics.DrainDurationMS,
		))
		return
	}
	set(gatePass, fmt.Sprintf(
		"deadline backlog=%d is bounded by max_inflight=%d and remained separate from drain=%.3f ms",
		value.Metrics.BacklogAtStop, cfg.MaxInflight, value.Metrics.DrainDurationMS,
	))
}

func setInspectorInputGate(gates map[string]gateEvidence, value report) {
	set := func(status, evidence string) {
		gates["inspector_input"] = gateEvidence{Status: status, Required: true, Evidence: evidence}
	}
	proof := value.BoundedEvidence.SampleRecords
	expected := value.Metrics.Completed + value.Metrics.Unclassified
	if proof.Truncated || proof.Seen != expected || int64(proof.Retained) != expected {
		set(gateFail, fmt.Sprintf(
			"sample records are incomplete: seen=%d retained=%d expected_results=%d limit=%d truncated=%t",
			proof.Seen, proof.Retained, expected, proof.Limit, proof.Truncated,
		))
		return
	}
	if value.Correctness.MissingOperationIDs > 0 {
		set(gateFail, fmt.Sprintf("%d successful writes lack operation IDs", value.Correctness.MissingOperationIDs))
		return
	}
	set(gatePass, fmt.Sprintf("all %d results retained; every successful write carries an operation ID", expected))
}

func setLatencyGate(
	gates map[string]gateEvidence, cfg config, durationSufficient bool, value report,
) {
	set := func(status, evidence string) {
		gates["latency"] = gateEvidence{Status: status, Required: true, Evidence: evidence}
	}
	if cfg.Mode != modeClosedLoop {
		set(gateUnknown, "open-loop latency cannot establish realistic closed-loop capacity")
		return
	}
	if cfg.SimulatedRTTMS != 150 {
		set(gateUnknown, "acceptance latency gate is defined at exactly 150 ms simulated RTT")
		return
	}
	if !durationSufficient {
		set(gateUnknown, "profile duration was insufficient for its fixed graph schedule")
		return
	}
	if value.BoundedEvidence.ResultLatency.Truncated {
		set(gateUnknown, "latency reservoir was truncated; exact acceptance percentiles are unavailable")
		return
	}
	thresholds := map[string]float64{
		"ping": 2000, "list": 2000, "ready": 2000, "show": 2000,
		"create": 3000, "graph": 60000,
	}
	for command, threshold := range thresholds {
		group, exists := value.Metrics.ByCommand[command]
		if !exists || group.Commands == 0 || group.EndToEnd.Count == 0 {
			set(gateUnknown, fmt.Sprintf("required command %s has no complete latency evidence", command))
			return
		}
		if group.EndToEnd.P95MS >= threshold {
			set(gateFail, fmt.Sprintf("%s p95 %.3f ms is not below %.0f ms", command, group.EndToEnd.P95MS, threshold))
			return
		}
	}
	set(gatePass, "all required command p95 values are below the 150 ms RTT acceptance thresholds")
}

func allRequiredGatesPass(gates map[string]gateEvidence) bool {
	if len(gates) == 0 {
		return false
	}
	for _, gate := range gates {
		if gate.Required && gate.Status != gatePass {
			return false
		}
	}
	return true
}
