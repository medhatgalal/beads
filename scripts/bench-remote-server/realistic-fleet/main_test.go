package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFrozenProfileMath(t *testing.T) {
	tests := []struct {
		name    string
		rate    float64
		aeWrite float64
		actors  int
	}{
		{name: "normal", rate: .9782608696, aeWrite: .0733695652, actors: 45},
		{name: "busy", rate: 7.8947368421, aeWrite: 1.2631578947, actors: 150},
		{name: "coordinated-burst", rate: 34.0909090909, aeWrite: 8.5227272727, actors: 375},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			profile := frozenProfiles[test.name]
			if got := expectedCommandRate(profile, 1); math.Abs(got-test.rate) > 1e-9 {
				t.Fatalf("expectedCommandRate() = %.12f, want %.12f", got, test.rate)
			}
			if got := hotAEPointWriteRate(profile, 1); math.Abs(got-test.aeWrite) > 1e-9 {
				t.Fatalf("hotAEPointWriteRate() = %.12f, want %.12f", got, test.aeWrite)
			}
			if got := actorCount(profile); got != test.actors {
				t.Fatalf("actorCount() = %d, want %d", got, test.actors)
			}
		})
	}
}

func TestSteadyStatePhasingMeetsFrozenFiniteWindowRate(t *testing.T) {
	tests := []struct {
		profile  string
		duration time.Duration
	}{
		{profile: "normal", duration: 15 * time.Minute},
		{profile: "busy", duration: 2 * time.Minute},
		{profile: "coordinated-burst", duration: 2 * time.Minute},
	}
	for _, test := range tests {
		t.Run(test.profile, func(t *testing.T) {
			profile := frozenProfiles[test.profile]
			commands := modeledPointCommands(profile, test.duration, defaultSeed)
			actualRate := float64(commands) / test.duration.Seconds()
			expectedRate := expectedCommandRate(profile, assumedResponseSeconds)
			ratio := actualRate / expectedRate
			t.Logf("commands=%d actual=%.3f/s expected=%.3f/s ratio=%.3fx", commands, actualRate, expectedRate, ratio)
			if ratio < loadFidelityLowerRatio || ratio > loadFidelityUpperRatio {
				t.Fatalf(
					"steady-state finite-window rate = %.3f/s (%.3fx), want %.2f-%.2fx %.3f/s",
					actualRate, ratio, loadFidelityLowerRatio, loadFidelityUpperRatio, expectedRate,
				)
			}
		})
	}
}

func TestSteadyStateInitialPhaseIsDeterministicAndSpansModelCycle(t *testing.T) {
	profile := frozenProfiles["busy"]
	cycle := time.Duration(
		(profile.ThinkTimeSeconds + profile.CommandsPerCycle*assumedResponseSeconds) * float64(time.Second),
	)
	first := rand.New(rand.NewSource(deriveSeed(defaultSeed, "actor-timing", 1)))  //nolint:gosec // G404: lab workload RNG is not security-sensitive
	second := rand.New(rand.NewSource(deriveSeed(defaultSeed, "actor-timing", 1))) //nolint:gosec // G404: lab workload RNG is not security-sensitive
	if got, want := steadyStateInitialPhase(first, profile), steadyStateInitialPhase(second, profile); got != want {
		t.Fatalf("initial phase is not deterministic: %s vs %s", got, want)
	}
	minimum, maximum := cycle, time.Duration(0)
	for actor := 0; actor < actorCount(profile); actor++ {
		rng := rand.New(rand.NewSource(deriveSeed(defaultSeed, "actor-timing", actor))) //nolint:gosec // G404: lab workload RNG is not security-sensitive
		phase := steadyStateInitialPhase(rng, profile)
		if phase < 0 || phase >= cycle {
			t.Fatalf("actor %d phase %s outside [0,%s)", actor, phase, cycle)
		}
		if phase < minimum {
			minimum = phase
		}
		if phase > maximum {
			maximum = phase
		}
	}
	if minimum >= cycle/10 || maximum <= cycle*9/10 {
		t.Fatalf("actor phases do not span the model cycle: min=%s max=%s cycle=%s", minimum, maximum, cycle)
	}
}

func TestParseConfigRequiresRealDatabaseAndOneLab(t *testing.T) {
	input := validConfig()
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseConfig(body)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if parsed.Seed != defaultSeed {
		t.Fatalf("Seed = %d, want default %d", parsed.Seed, defaultSeed)
	}
	if got := capacityInterpretation(parsed.Mode); got != "closed_loop_evidence_gated" {
		t.Fatalf("capacityInterpretation() = %q", got)
	}

	badDatabase := validConfig()
	badDatabase.Targets[0].DatabaseID = "beads_realistic_ae"
	body, _ = json.Marshal(badDatabase)
	if _, err := parseConfig(body); err == nil {
		t.Fatal("parseConfig() accepted a label instead of the actual beads_perf_lab_* database")
	}

	badLab := validConfig()
	badLab.Targets[1].LabID = "00000000-0000-4000-8000-000000000099"
	body, _ = json.Marshal(badLab)
	if _, err := parseConfig(body); err == nil {
		t.Fatal("parseConfig() accepted targets from different labs")
	}
}

func TestParseConfigRejectsUnknownAndUnsafeEndpoint(t *testing.T) {
	input := validConfig()
	body, _ := json.Marshal(input)
	body = bytes.Replace(body, []byte(`{"schema_version":1,`), []byte(`{"schema_version":1,"mystery":true,`), 1)
	if _, err := parseConfig(body); err == nil {
		t.Fatal("parseConfig() accepted unknown field")
	}

	input = validConfig()
	input.Targets[0].BaseURL = "https://example.com:443"
	body, _ = json.Marshal(input)
	if _, err := parseConfig(body); err == nil {
		t.Fatal("parseConfig() accepted non-loopback endpoint")
	}
}

func TestOpenLoopIsExplicitNonCapacityMode(t *testing.T) {
	input := validConfig()
	input.Mode = modeOpenLoop
	input.OpenLoopRate = 100
	body, _ := json.Marshal(input)
	parsed, err := parseConfig(body)
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if got := capacityInterpretation(parsed.Mode); got != "non_capacity_failure_envelope_only" {
		t.Fatalf("capacityInterpretation() = %q", got)
	}
	input.OpenLoopRate = 75
	body, _ = json.Marshal(input)
	if _, err := parseConfig(body); err == nil {
		t.Fatal("parseConfig() accepted unsupported overload rate")
	}
}

func TestAttestationVerifiesIdentityAndHMAC(t *testing.T) {
	token := bytes.Repeat([]byte("a"), 32)
	cfg := validConfig().Targets[0]
	server := newAttestationServer(t, cfg, token, nil)
	defer server.Close()
	target := runtimeTargetForServer(t, cfg, token, server)
	defer target.client.CloseIdleConnections()
	if err := attestTarget(context.Background(), target); err != nil {
		t.Fatalf("attestTarget() error = %v", err)
	}

	wrongIdentity := newAttestationServer(t, cfg, token, func(value *attestationResponse) {
		value.DatabaseName = "beads_perf_lab_wrong"
		value.Signature = ""
	})
	defer wrongIdentity.Close()
	badTarget := runtimeTargetForServer(t, cfg, token, wrongIdentity)
	defer badTarget.client.CloseIdleConnections()
	if err := attestTarget(context.Background(), badTarget); err == nil {
		t.Fatal("attestTarget() accepted an HMAC-valid but mismatched database identity")
	}

	wrongSignature := newAttestationServer(t, cfg, token, func(value *attestationResponse) {
		value.Signature = hex.EncodeToString(bytes.Repeat([]byte{0xff}, 32))
	})
	defer wrongSignature.Close()
	badSignatureTarget := runtimeTargetForServer(t, cfg, token, wrongSignature)
	defer badSignatureTarget.client.CloseIdleConnections()
	if err := attestTarget(context.Background(), badSignatureTarget); err == nil {
		t.Fatal("attestTarget() accepted an invalid signature")
	}
}

func TestActiveSelectionSamplingAndPoolsAreDeterministic(t *testing.T) {
	cfg := validConfig()
	first := selectActiveTargets(cfg.Targets, 2, 99)
	second := selectActiveTargets(cfg.Targets, 2, 99)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("active selection differs: %#v vs %#v", first, second)
	}
	hotFound := false
	for _, target := range first {
		hotFound = hotFound || target.HotAE
	}
	if !hotFound {
		t.Fatal("active selection omitted hot ae target")
	}
	rngA := rand.New(rand.NewSource(deriveSeed(99, "actor", 7))) //nolint:gosec // G404: lab workload RNG is not security-sensitive
	rngB := rand.New(rand.NewSource(deriveSeed(99, "actor", 7))) //nolint:gosec // G404: lab workload RNG is not security-sensitive
	thinkA := sampleThink(rngA, 100*time.Second)
	thinkB := sampleThink(rngB, 100*time.Second)
	if thinkA != thinkB || thinkA < 80*time.Second || thinkA > 120*time.Second {
		t.Fatalf("sampleThink() = %s/%s", thinkA, thinkB)
	}

	hot, nonHot := &runtimeTarget{}, &runtimeTarget{}
	rng := rand.New(rand.NewSource(1)) //nolint:gosec // G404: lab workload RNG is not security-sensitive
	for i := 0; i < 50; i++ {
		if got := chooseTarget(rng, 0, []*runtimeTarget{nonHot}, []*runtimeTarget{nonHot}, hot); got != nonHot {
			t.Fatal("zero hot share selected the hot target")
		}
		if got := chooseTarget(rng, 1, []*runtimeTarget{nonHot}, []*runtimeTarget{nonHot}, hot); got != hot {
			t.Fatal("full hot share selected a non-hot target")
		}
	}
}

func TestOriginAndDestinationAttributionRemainSeparate(t *testing.T) {
	acc := newAccumulator(10, 10, 10, "beads_perf_lab_ae")
	acc.generatedOne()
	acc.record(jobResult{
		identity: "actor-000001-cycle-000000001-command-000",
		kind:     "point_write", command: "create", originTeam: "team-a",
		destinationTeam: "team-ae", databaseID: "beads_perf_lab_ae",
		passed: true, operationID: "operation-1", service: 10 * time.Millisecond,
	})
	stop := acc.snapshot()
	metrics, _, _, samples := acc.finish(time.Second, time.Second, 0, stop)
	if metrics.ByOriginTeam["team-a"].Commands != 1 || metrics.ByDestinationTeam["team-ae"].Commands != 1 {
		t.Fatalf("origin/destination metrics = %#v / %#v", metrics.ByOriginTeam, metrics.ByDestinationTeam)
	}
	if len(samples) != 1 || samples[0].OriginTeam != "team-a" || samples[0].DestinationTeam != "team-ae" {
		t.Fatalf("sample attribution = %#v", samples)
	}
}

func TestStableOperationIdentityDoesNotDependOnSchedulingOrdinal(t *testing.T) {
	identity := actorOperationIdentity(7, 11, 2)
	first := job{ordinal: 1, identity: identity, kind: "point_write"}
	second := job{ordinal: 9999, identity: identity, kind: "point_write"}
	firstBody, err := operationBody("run-test-0001", first)
	if err != nil {
		t.Fatal(err)
	}
	secondBody, err := operationBody("run-test-0001", second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBody, secondBody) {
		t.Fatal("operation body changed with global scheduling ordinal")
	}
	if stableIdempotencyKey("run-test-0001", first.identity) != stableIdempotencyKey("run-test-0001", second.identity) {
		t.Fatal("idempotency key changed with global scheduling ordinal")
	}
	if identity == graphOperationIdentity(7) {
		t.Fatal("actor and graph identities collide")
	}
}

func TestSubmitRetriesIdenticalBodyAndKeyAfterAmbiguousTransport(t *testing.T) {
	token := bytes.Repeat([]byte("b"), 32)
	projectID := validConfig().Targets[0].ProjectID
	var attempts atomic.Int32
	var mu sync.Mutex
	var bodies [][]byte
	var keys []string
	server := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		mu.Lock()
		bodies = append(bodies, append([]byte(nil), body...))
		keys = append(keys, request.Header.Get("Idempotency-Key"))
		mu.Unlock()
		if attempts.Add(1) == 1 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Error("response writer cannot simulate ambiguous transport")
				return
			}
			connection, _, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = connection.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(operationState{ID: "op-1", ProjectID: projectID, Status: "succeeded"})
	}))
	defer server.Close()
	target := runtimeTargetForServer(t, validConfig().Targets[0], token, server)
	defer target.client.CloseIdleConnections()
	cfg := validConfig()
	cfg.SimulatedRTTMS = 0
	item := job{
		identity: actorOperationIdentity(1, 2, 3), kind: "point_write", command: "create",
		target: target,
	}
	state, status, err := submitAndPoll(context.Background(), cfg, item)
	if err != nil || status != http.StatusOK || state.Status != "succeeded" {
		t.Fatalf("submitAndPoll() = %#v, %d, %v", state, status, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("retry requests bodies=%d equal=%v keys=%q", len(bodies), len(bodies) == 2 && bytes.Equal(bodies[0], bodies[1]), keys)
	}
}

func TestSubmitHTTPFailureIsClassifiedAndPollAmbiguityIsNot(t *testing.T) {
	token := bytes.Repeat([]byte("c"), 32)
	cfg := validConfig()
	cfg.SimulatedRTTMS = 0
	projectID := cfg.Targets[0].ProjectID

	rejected := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "full", http.StatusTooManyRequests)
	}))
	defer rejected.Close()
	target := runtimeTargetForServer(t, cfg.Targets[0], token, rejected)
	defer target.client.CloseIdleConnections()
	_, status, err := submitAndPoll(context.Background(), cfg, job{
		identity: actorOperationIdentity(1, 1, 1), kind: "point_write", target: target,
	})
	class, unclassified := classifyRequestError(err)
	if status != http.StatusTooManyRequests || class != "http_429" || unclassified {
		t.Fatalf("classified status = %d %q unclassified=%v err=%v", status, class, unclassified, err)
	}

	pollFailure := newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost {
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(operationState{ID: "op-pending", ProjectID: projectID, Status: "pending"})
			return
		}
		http.Error(w, "lost", http.StatusInternalServerError)
	}))
	defer pollFailure.Close()
	pollTarget := runtimeTargetForServer(t, cfg.Targets[0], token, pollFailure)
	defer pollTarget.client.CloseIdleConnections()
	_, _, err = submitAndPoll(context.Background(), cfg, job{
		identity: actorOperationIdentity(1, 1, 2), kind: "point_write", target: pollTarget,
	})
	class, unclassified = classifyRequestError(err)
	if class != "unclassified_ambiguous_poll_status" || !unclassified {
		t.Fatalf("poll classification = %q unclassified=%v err=%v", class, unclassified, err)
	}
}

func TestDrainAndOutcomeAccounting(t *testing.T) {
	acc := newAccumulator(10, 10, 10, "hot")
	for i := 0; i < 4; i++ {
		acc.generatedOne()
	}
	acc.droppedOne()
	acc.record(jobResult{identity: "one", command: "ping", passed: true})
	atStop := acc.snapshot()
	acc.record(jobResult{identity: "two", command: "ping", errorClass: "http_500"})
	acc.record(jobResult{identity: "three", command: "create", unclassified: true, errorClass: "unclassified_ambiguous_submit"})
	metrics, _, _, _ := acc.finish(2*time.Second, 4*time.Second, 2*time.Second, atStop)
	if metrics.Generated != 4 || metrics.Completed != 2 || metrics.Dropped != 1 || metrics.Unclassified != 1 || !metrics.AccountingBalanced {
		t.Fatalf("final accounting = %#v", metrics)
	}
	if metrics.CompletionsAtStop != 1 || metrics.BacklogAtStop != 2 ||
		!metrics.StopAccountingValid || metrics.DrainDurationMS != 2000 {
		t.Fatalf("stop/drain accounting = %#v", metrics)
	}
	if metrics.TrueCompletionPerSecond != .5 {
		t.Fatalf("true completion throughput = %f", metrics.TrueCompletionPerSecond)
	}
}

func TestDeadlineBacklogIsNeverClampedAndIsPromotionGated(t *testing.T) {
	cfg := validConfig()
	value := report{Metrics: runMetrics{
		BacklogAtStop: int64(cfg.MaxInflight), StopAccountingValid: true, DrainDurationMS: 125,
	}}
	gates := map[string]gateEvidence{}
	setStopBacklogGate(gates, cfg, value)
	if gates["stop_backlog"].Status != gatePass {
		t.Fatalf("bounded backlog gate = %#v, want pass", gates["stop_backlog"])
	}

	value.Metrics.BacklogAtStop++
	setStopBacklogGate(gates, cfg, value)
	if gates["stop_backlog"].Status != gateFail {
		t.Fatalf("client-queued backlog gate = %#v, want fail", gates["stop_backlog"])
	}

	acc := newAccumulator(1, 1, 1, "hot")
	metrics, _, _, _ := acc.finish(time.Second, time.Second, 0, accumulatorSnapshot{completed: 1})
	if metrics.BacklogAtStop != -1 || metrics.StopAccountingValid {
		t.Fatalf("invalid stop snapshot was hidden: %#v", metrics)
	}
	value.Metrics = metrics
	setStopBacklogGate(gates, cfg, value)
	if gates["stop_backlog"].Status != gateFail {
		t.Fatalf("negative backlog gate = %#v, want fail", gates["stop_backlog"])
	}
}

func TestLoadFidelityGateRejectsStartupUnderdriveAndGraphDrift(t *testing.T) {
	cfg := validConfig()
	cfg.Profile = "busy"
	cfg.DurationSeconds = 120
	expectedRate := expectedCommandRate(frozenProfiles[cfg.Profile], assumedResponseSeconds)
	pointCommands := int64(math.Round(expectedRate * float64(cfg.DurationSeconds)))
	value := report{Metrics: runMetrics{
		Generated: pointCommands + 1, Graphs: 1, LoadWindowSeconds: float64(cfg.DurationSeconds),
		ReadCommands:       int64(math.Round(float64(pointCommands) * .60)),
		PointWrites:        pointCommands - int64(math.Round(float64(pointCommands)*.60)),
		HotAEPointCommands: int64(math.Round(float64(pointCommands) * .40)),
		HotAEPointWrites:   int64(math.Round(float64(pointCommands) * .16)),
	}}
	gates := map[string]gateEvidence{}
	setLoadFidelityGate(gates, cfg, true, value)
	if gates["load_fidelity"].Status != gatePass {
		t.Fatalf("model-matched load gate = %#v, want pass", gates["load_fidelity"])
	}

	wrongMix := value
	wrongMix.Metrics.ReadCommands = pointCommands
	wrongMix.Metrics.PointWrites = 0
	setLoadFidelityGate(gates, cfg, true, wrongMix)
	if gates["load_fidelity"].Status != gateFail {
		t.Fatalf("wrong-mix load gate = %#v, want fail", gates["load_fidelity"])
	}

	// This is the exact shape of the first live run: 776 point commands over
	// 120 seconds against a 7.895 command/s frozen busy model.
	underdriven := value
	underdriven.Metrics.Generated = 776 + 1
	underdriven.Metrics.ReadCommands = 453
	underdriven.Metrics.PointWrites = 323
	underdriven.Metrics.HotAEPointCommands = 325
	underdriven.Metrics.HotAEPointWrites = 145
	setLoadFidelityGate(gates, cfg, true, underdriven)
	if gates["load_fidelity"].Status != gateFail {
		t.Fatalf("startup-underdriven load gate = %#v, want fail", gates["load_fidelity"])
	}

	graphDrift := value
	graphDrift.Metrics.Generated = pointCommands
	graphDrift.Metrics.Graphs = 0
	setLoadFidelityGate(gates, cfg, true, graphDrift)
	if gates["load_fidelity"].Status != gateFail {
		t.Fatalf("missing-graph load gate = %#v, want fail", gates["load_fidelity"])
	}
}

func TestWriteSamplesCarryOperationIDsAndMustBeComplete(t *testing.T) {
	acc := newAccumulator(2, 2, 2, "hot")
	for _, result := range []jobResult{
		{identity: "read", kind: "read", command: "ping", passed: true},
		{identity: "write", kind: "point_write", command: "create", passed: true, operationID: "operation-1"},
	} {
		acc.generatedOne()
		acc.record(result)
	}
	stop := acc.snapshot()
	metrics, correctness, bounded, samples := acc.finish(time.Second, time.Second, 0, stop)
	if bounded.SampleRecords.Truncated || bounded.SampleRecords.Seen != 2 || bounded.SampleRecords.Retained != 2 {
		t.Fatalf("sample evidence = %#v", bounded.SampleRecords)
	}
	byIdentity := map[string]sampleRecord{}
	for _, sample := range samples {
		byIdentity[sample.Identity] = sample
	}
	if byIdentity["write"].OperationID != "operation-1" || byIdentity["read"].OperationID != "" {
		t.Fatalf("operation IDs = %#v", byIdentity)
	}
	body, err := json.Marshal(samples)
	if err != nil {
		t.Fatal(err)
	}
	var encoded []map[string]any
	if err := json.Unmarshal(body, &encoded); err != nil {
		t.Fatal(err)
	}
	for _, sample := range encoded {
		if _, exists := sample["operation_id"]; !exists {
			t.Fatalf("serialized sample omits operation_id: %s", body)
		}
	}
	value := report{Metrics: metrics, Correctness: correctness, BoundedEvidence: bounded}
	gates := map[string]gateEvidence{}
	setInspectorInputGate(gates, value)
	if gates["inspector_input"].Status != gatePass {
		t.Fatalf("complete inspector input gate = %#v", gates["inspector_input"])
	}

	truncated := newAccumulator(1, 2, 2, "hot")
	for _, result := range []jobResult{
		{identity: "read", kind: "read", command: "ping", passed: true},
		{identity: "write", kind: "point_write", command: "create", passed: true, operationID: "operation-1"},
	} {
		truncated.generatedOne()
		truncated.record(result)
	}
	metrics, correctness, bounded, _ = truncated.finish(time.Second, time.Second, 0, truncated.snapshot())
	value = report{Metrics: metrics, Correctness: correctness, BoundedEvidence: bounded}
	setInspectorInputGate(gates, value)
	if gates["inspector_input"].Status != gateFail || !bounded.SampleRecords.Truncated {
		t.Fatalf("truncated inspector input gate = %#v, evidence=%#v", gates["inspector_input"], bounded.SampleRecords)
	}

	missing := newAccumulator(1, 1, 1, "hot")
	missing.generatedOne()
	missing.record(jobResult{identity: "write", kind: "point_write", command: "create", passed: true})
	metrics, correctness, bounded, _ = missing.finish(time.Second, time.Second, 0, missing.snapshot())
	if correctness.MissingOperationIDs != 1 {
		t.Fatalf("missing operation ID counter = %#v", correctness)
	}
	value = report{Metrics: metrics, Correctness: correctness, BoundedEvidence: bounded}
	setInspectorInputGate(gates, value)
	if gates["inspector_input"].Status != gateFail {
		t.Fatalf("missing operation ID gate = %#v", gates["inspector_input"])
	}
}

func TestBoundedReservoirsAreDeterministicAndTruncationBlocksProof(t *testing.T) {
	forward := newDeterministicReservoir[int](3)
	reverse := newDeterministicReservoir[int](3)
	for i := 0; i < 100; i++ {
		forward.add(fmt.Sprintf("key-%03d", i), i)
	}
	for i := 99; i >= 0; i-- {
		reverse.add(fmt.Sprintf("key-%03d", i), i)
	}
	if got, want := reservoirKeys(forward), reservoirKeys(reverse); !reflect.DeepEqual(got, want) {
		t.Fatalf("reservoir depends on arrival order: %v vs %v", got, want)
	}
	if !forward.evidence().Truncated || forward.evidence().Retained != 3 {
		t.Fatalf("reservoir evidence = %#v", forward.evidence())
	}

	acc := newAccumulator(2, 2, 2, "hot")
	for i := 0; i < 5; i++ {
		acc.generatedOne()
		acc.record(jobResult{
			identity: fmt.Sprintf("result-%d", i), command: "create", kind: "point_write",
			passed: true, operationID: fmt.Sprintf("operation-%d", i),
		})
	}
	stop := acc.snapshot()
	metrics, correctness, bounded, _ := acc.finish(time.Second, time.Second, 0, stop)
	value := minimallyGateableReport(validConfig(), metrics, correctness, bounded)
	gates := evaluateGates(validConfig(), true, value)
	if !bounded.ResultLatency.Truncated || !bounded.OperationIDProof.Truncated {
		t.Fatalf("bounded evidence = %#v", bounded)
	}
	if !bounded.SampleRecords.Truncated {
		t.Fatalf("sample evidence was not marked truncated: %#v", bounded.SampleRecords)
	}
	if gates["correctness"].Status != gateUnknown {
		t.Fatalf("correctness gate = %#v, want unknown", gates["correctness"])
	}
}

func modeledPointCommands(profile workloadProfile, duration time.Duration, seed int64) int {
	commands := 0
	for actor := 0; actor < actorCount(profile); actor++ {
		rng := rand.New(rand.NewSource(deriveSeed(seed, "actor-timing", actor))) //nolint:gosec // G404: lab workload RNG is not security-sensitive
		elapsed := steadyStateInitialPhase(rng, profile)
		for elapsed < duration {
			commandCount := sampleCommandCount(rng, profile.CommandsPerCycle)
			for command := 0; command < commandCount && elapsed < duration; command++ {
				commands++
				elapsed += time.Duration(assumedResponseSeconds * float64(time.Second))
			}
			elapsed += sampleThink(rng, time.Duration(profile.ThinkTimeSeconds*float64(time.Second)))
		}
	}
	return commands
}

func TestClosedLoopNeverPromotesWithUnknownRequiredEvidence(t *testing.T) {
	cfg := validConfig()
	value := minimallyGateableReport(cfg, runMetrics{
		Generated: 6, Completed: 6, Passed: 6, AccountingBalanced: true,
		ByCommand: passingCommandMetrics(),
	}, correctnessCounters{}, boundedEvidence{
		ResultLatency:    boundedStoreEvidence{Seen: 6, Retained: 6, Limit: 100},
		OperationIDProof: boundedStoreEvidence{Seen: 2, Retained: 2, Limit: 100},
	})
	gates := evaluateGates(cfg, true, value)
	for _, name := range []string{"utilization", "isolation", "independent_database_effect", "recovery"} {
		if gates[name].Status != gateUnknown {
			t.Fatalf("gate %s = %#v, want unknown", name, gates[name])
		}
	}
	if allRequiredGatesPass(gates) {
		t.Fatal("unknown required evidence was promoted to pass")
	}
	promotionEligible := cfg.Mode == modeClosedLoop && allRequiredGatesPass(gates)
	if promotionEligible {
		t.Fatal("closed-loop mode alone enabled capacity promotion")
	}
	for name, gate := range gates {
		gate.Status = gatePass
		gates[name] = gate
	}
	if !allRequiredGatesPass(gates) {
		t.Fatal("all explicit required gates passed but aggregate did not")
	}
}

func TestCommandsIncludeShowAndGraphScheduleIsAbsolute(t *testing.T) {
	hot := &runtimeTarget{cfg: validConfig().Targets[0]}
	hot.cfg.FixtureIssueID = "fixture-1"
	profile := frozenProfiles["normal"]
	profile.ReadFraction = 1
	profile.HotAEShare = 1
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // G404: lab workload RNG is not security-sensitive
	commands := map[string]bool{}
	for i := 0; i < 100; i++ {
		item := makePointJob(profile, rng, "team-a", nil, nil, hot, openOperationIdentity(i))
		commands[item.command] = true
	}
	for _, command := range []string{"ping", "list", "ready", "show"} {
		if !commands[command] {
			t.Fatalf("read mix omitted %s: %#v", command, commands)
		}
	}

	short, sufficient := absoluteGraphSchedule(frozenProfiles["coordinated-burst"], 30*time.Second)
	if sufficient || len(short) != 8 || short[0] != 0 || short[len(short)-1] != 28*time.Second {
		t.Fatalf("short burst schedule = %v sufficient=%v", short, sufficient)
	}
	full, sufficient := absoluteGraphSchedule(frozenProfiles["coordinated-burst"], 2*time.Minute)
	if !sufficient || len(full) != 30 || full[len(full)-1] != 116*time.Second {
		t.Fatalf("full burst schedule = %v sufficient=%v", full, sufficient)
	}
	busy, sufficient := absoluteGraphSchedule(frozenProfiles["busy"], 4*time.Minute)
	if !sufficient || !reflect.DeepEqual(busy, []time.Duration{0, 2 * time.Minute}) {
		t.Fatalf("busy schedule = %v sufficient=%v", busy, sufficient)
	}

	body, err := operationBody("run-test-0001", job{identity: graphOperationIdentity(3), kind: "graph"})
	if err != nil {
		t.Fatal(err)
	}
	var request struct {
		Kind    string `json:"kind"`
		Payload struct {
			Nodes []json.RawMessage `json:"nodes"`
			Edges []struct {
				From string `json:"from_key"`
				To   string `json:"to_key"`
			} `json:"edges"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		t.Fatal(err)
	}
	if request.Kind != "graph.apply" || len(request.Payload.Nodes) != 100 || len(request.Payload.Edges) != 200 {
		t.Fatalf("graph request = kind %q nodes %d edges %d", request.Kind, len(request.Payload.Nodes), len(request.Payload.Edges))
	}
	for _, edge := range request.Payload.Edges {
		if edge.From <= edge.To {
			t.Fatalf("edge is not ordered acyclic: %s -> %s", edge.From, edge.To)
		}
	}
}

func newAttestationServer(
	t *testing.T, cfg targetConfig, token []byte, mutate func(*attestationResponse),
) *httptest.Server {
	t.Helper()
	return newHTTPTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/projects/"+cfg.ProjectID+"/attestation" || request.Header.Get("Authorization") != "Bearer "+string(token) ||
			request.Header.Get("X-Beads-Project-ID") != cfg.ProjectID {
			http.Error(w, "bad request", http.StatusForbidden)
			return
		}
		value := attestationResponse{
			Version: attestationVersion, Environment: attestationEnvironment,
			LabID: cfg.LabID, DatabaseName: cfg.DatabaseID, ProjectID: cfg.ProjectID,
		}
		value.Signature = hex.EncodeToString(attestationSignature(token, value))
		if mutate != nil {
			mutate(&value)
			if value.Signature == "" {
				value.Signature = hex.EncodeToString(attestationSignature(token, value))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(value)
	}))
}

func newHTTPTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.Start()
	return server
}

func runtimeTargetForServer(
	t *testing.T, cfg targetConfig, token []byte, server *httptest.Server,
) *runtimeTarget {
	t.Helper()
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	return &runtimeTarget{
		cfg: cfg, base: base, token: append([]byte(nil), token...),
		client: newHTTPClient(2 * time.Second),
	}
}

func reservoirKeys[T any](value *deterministicReservoir[T]) []string {
	entries := value.entries()
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.key)
	}
	sort.Strings(keys)
	return keys
}

func minimallyGateableReport(
	cfg config, metrics runMetrics, correctness correctnessCounters, bounded boundedEvidence,
) report {
	return report{
		Config:      sanitizedConfig{RegisteredDatabases: len(cfg.Targets)},
		Attestation: attestationInventory{Required: len(cfg.Targets), Verified: len(cfg.Targets)},
		Metrics:     metrics, Correctness: correctness, BoundedEvidence: bounded,
		ResourceBefore: resourceSnapshot{Gateway: poolResource{TargetsSampled: len(cfg.Targets)}},
	}
}

func passingCommandMetrics() map[string]metricGroup {
	result := map[string]metricGroup{}
	for _, command := range []string{"ping", "list", "ready", "show", "create", "graph"} {
		result[command] = metricGroup{
			Commands: 1, Passed: 1,
			EndToEnd: latencySummary{Count: 1, P95MS: 1},
		}
	}
	return result
}

func validConfig() config {
	return config{
		SchemaVersion: configSchemaVersion, SyntheticAcknowledgement: syntheticAck,
		RunID: "run-test-0001", Mode: modeClosedLoop, Profile: "normal",
		DurationSeconds: 900, ActiveDatabaseCount: 2, MaxInflight: 32,
		QueueCapacity: 64, SimulatedRTTMS: 150, RequestTimeoutMS: 120000,
		SampleLimit: 100,
		Targets: []targetConfig{
			{TeamID: "team-ae", LabID: "00000000-0000-4000-8000-000000000010", DatabaseID: "beads_perf_lab_ae", ProjectID: "00000000-0000-4000-8000-000000000001", BaseURL: "http://127.0.0.1:7701", TokenFile: "/private/tmp/token-a", HotAE: true, FixtureIssueID: "fixture-ae"},
			{TeamID: "team-a", LabID: "00000000-0000-4000-8000-000000000010", DatabaseID: "beads_perf_lab_repo_a", ProjectID: "00000000-0000-4000-8000-000000000002", BaseURL: "http://127.0.0.1:7702", TokenFile: "/private/tmp/token-b", FixtureIssueID: "fixture-a"},
			{TeamID: "team-b", LabID: "00000000-0000-4000-8000-000000000010", DatabaseID: "beads_perf_lab_repo_b", ProjectID: "00000000-0000-4000-8000-000000000003", BaseURL: "http://127.0.0.1:7703", TokenFile: "/private/tmp/token-c", FixtureIssueID: "fixture-b"},
		},
	}
}
