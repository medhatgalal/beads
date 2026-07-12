package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/rand"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	configSchemaVersion          = 1
	syntheticAck                 = "BEADS_REALISTIC_FLEET_SYNTHETIC_ONLY"
	modeClosedLoop               = "closed-loop"
	modeOpenLoop                 = "open-loop-overload"
	assumedResponseSeconds       = 1.0
	defaultSeed            int64 = 20260712
)

var (
	runIDPattern    = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{7,63}$`)
	databasePattern = regexp.MustCompile(`^beads_perf_lab_[a-z0-9_]{1,96}$`)
	projectPattern  = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$`)
)

type targetConfig struct {
	TeamID         string `json:"team_id"`
	LabID          string `json:"lab_id"`
	DatabaseID     string `json:"database_id"`
	ProjectID      string `json:"project_id"`
	BaseURL        string `json:"base_url"`
	TokenFile      string `json:"token_file"`
	HotAE          bool   `json:"hot_ae,omitempty"`
	FixtureIssueID string `json:"fixture_issue_id,omitempty"`
}

type config struct {
	SchemaVersion            int            `json:"schema_version"`
	SyntheticAcknowledgement string         `json:"synthetic_acknowledgement"`
	RunID                    string         `json:"run_id"`
	Mode                     string         `json:"mode"`
	Profile                  string         `json:"profile"`
	DurationSeconds          int            `json:"duration_seconds"`
	Seed                     int64          `json:"seed"`
	ActiveDatabaseCount      int            `json:"active_database_count"`
	MaxInflight              int            `json:"max_inflight"`
	QueueCapacity            int            `json:"queue_capacity"`
	SimulatedRTTMS           int            `json:"simulated_rtt_ms"`
	RequestTimeoutMS         int            `json:"request_timeout_ms"`
	SampleLimit              int            `json:"sample_limit"`
	OpenLoopRate             int            `json:"open_loop_commands_per_second,omitempty"`
	Targets                  []targetConfig `json:"targets"`
}

type workloadProfile struct {
	Name                    string  `json:"name"`
	RegisteredPeople        int     `json:"registered_people"`
	ActiveFraction          float64 `json:"active_fraction"`
	AgentsPerActivePerson   float64 `json:"agents_per_active_person"`
	ThinkTimeSeconds        float64 `json:"think_time_seconds"`
	CommandsPerCycle        float64 `json:"commands_per_cycle"`
	ReadFraction            float64 `json:"read_fraction"`
	HotAEShare              float64 `json:"hot_ae_share"`
	GraphRatePerHour        float64 `json:"graph_rate_per_hour,omitempty"`
	CoordinatedGraphCount   int     `json:"coordinated_graph_count,omitempty"`
	CoordinatedGraphWindowS int     `json:"coordinated_graph_window_seconds,omitempty"`
}

var frozenProfiles = map[string]workloadProfile{
	"normal": {
		Name: "normal", RegisteredPeople: 300, ActiveFraction: .10,
		AgentsPerActivePerson: 1.5, ThinkTimeSeconds: 90, CommandsPerCycle: 2,
		ReadFraction: .70, HotAEShare: .25, GraphRatePerHour: 4,
	},
	"busy": {
		Name: "busy", RegisteredPeople: 300, ActiveFraction: .25,
		AgentsPerActivePerson: 2, ThinkTimeSeconds: 45, CommandsPerCycle: 2.5,
		ReadFraction: .60, HotAEShare: .40, GraphRatePerHour: 30,
	},
	"coordinated-burst": {
		Name: "coordinated-burst", RegisteredPeople: 300, ActiveFraction: .50,
		AgentsPerActivePerson: 2.5, ThinkTimeSeconds: 30, CommandsPerCycle: 3,
		ReadFraction: .50, HotAEShare: .50,
		CoordinatedGraphCount: 30, CoordinatedGraphWindowS: 120,
	},
}

type workloadModel struct {
	Formula                        string          `json:"formula"`
	AssumedResponseSeconds         float64         `json:"assumed_response_seconds"`
	Profile                        workloadProfile `json:"profile"`
	ActorCount                     int             `json:"actor_count"`
	ExpectedCommandsPerSecond      float64         `json:"expected_commands_per_second"`
	ExpectedHotAEPointWritesPerSec float64         `json:"expected_hot_ae_point_writes_per_second"`
	GraphContract                  string          `json:"graph_contract"`
}

func expectedCommandRate(p workloadProfile, responseSeconds float64) float64 {
	n := float64(p.RegisteredPeople) * p.ActiveFraction * p.AgentsPerActivePerson * p.CommandsPerCycle
	d := p.ThinkTimeSeconds + p.CommandsPerCycle*responseSeconds
	if d <= 0 {
		return 0
	}
	return n / d
}

func hotAEPointWriteRate(p workloadProfile, responseSeconds float64) float64 {
	return expectedCommandRate(p, responseSeconds) * (1 - p.ReadFraction) * p.HotAEShare
}

func actorCount(p workloadProfile) int {
	return int(math.Round(float64(p.RegisteredPeople) * p.ActiveFraction * p.AgentsPerActivePerson))
}

func modelFor(p workloadProfile) workloadModel {
	return workloadModel{
		Formula: "lambda=H*f*A*k/(Z+kR)", AssumedResponseSeconds: assumedResponseSeconds,
		Profile: p, ActorCount: actorCount(p),
		ExpectedCommandsPerSecond:      expectedCommandRate(p, assumedResponseSeconds),
		ExpectedHotAEPointWritesPerSec: hotAEPointWriteRate(p, assumedResponseSeconds),
		GraphContract:                  "100-node/200-edge synthetic-blocking-dag-v1",
	}
}

func parseConfig(body []byte) (config, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var cfg config
	if err := dec.Decode(&cfg); err != nil {
		return config{}, fmt.Errorf("decode config: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return config{}, fmt.Errorf("decode config: trailing JSON")
		}
		return config{}, fmt.Errorf("decode config: %w", err)
	}
	if cfg.Seed == 0 {
		cfg.Seed = defaultSeed
	}
	if err := validateConfig(cfg); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func validateConfig(cfg config) error {
	if cfg.SchemaVersion != configSchemaVersion {
		return fmt.Errorf("schema_version must be %d", configSchemaVersion)
	}
	if cfg.SyntheticAcknowledgement != syntheticAck {
		return fmt.Errorf("synthetic_acknowledgement must authorize isolated synthetic data")
	}
	if !runIDPattern.MatchString(cfg.RunID) {
		return fmt.Errorf("run_id must be 8-64 lowercase alphanumeric or hyphen characters")
	}
	if cfg.Mode != modeClosedLoop && cfg.Mode != modeOpenLoop {
		return fmt.Errorf("mode must be %q or %q", modeClosedLoop, modeOpenLoop)
	}
	if _, ok := frozenProfiles[cfg.Profile]; !ok {
		return fmt.Errorf("profile must be normal, busy, or coordinated-burst")
	}
	if cfg.DurationSeconds < 30 || cfg.DurationSeconds > 7200 {
		return fmt.Errorf("duration_seconds must be between 30 and 7200")
	}
	if cfg.ActiveDatabaseCount < 2 || cfg.ActiveDatabaseCount > len(cfg.Targets) {
		return fmt.Errorf("active_database_count must be between 2 and the target count")
	}
	if cfg.MaxInflight < 1 || cfg.MaxInflight > 256 {
		return fmt.Errorf("max_inflight must be between 1 and 256")
	}
	if cfg.QueueCapacity < cfg.MaxInflight || cfg.QueueCapacity > 100000 {
		return fmt.Errorf("queue_capacity must be at least max_inflight and at most 100000")
	}
	if cfg.SimulatedRTTMS < 0 || cfg.SimulatedRTTMS > 2000 {
		return fmt.Errorf("simulated_rtt_ms must be between 0 and 2000")
	}
	if cfg.RequestTimeoutMS < 1000 || cfg.RequestTimeoutMS > 900000 {
		return fmt.Errorf("request_timeout_ms must be between 1000 and 900000")
	}
	if cfg.SampleLimit < 0 || cfg.SampleLimit > 10000 {
		return fmt.Errorf("sample_limit must be between 0 and 10000")
	}
	if cfg.Mode == modeClosedLoop && cfg.OpenLoopRate != 0 {
		return fmt.Errorf("closed-loop mode must not set open_loop_commands_per_second")
	}
	if cfg.Mode == modeOpenLoop && cfg.OpenLoopRate != 50 && cfg.OpenLoopRate != 100 && cfg.OpenLoopRate != 200 {
		return fmt.Errorf("open-loop overload rate must be 50, 100, or 200 commands/s")
	}
	if len(cfg.Targets) < 2 || len(cfg.Targets) > 500 {
		return fmt.Errorf("targets must contain between 2 and 500 synthetic databases")
	}
	seenDB, seenProject := map[string]struct{}{}, map[string]struct{}{}
	labID := ""
	hotCount := 0
	for i, target := range cfg.Targets {
		if target.TeamID == "" || strings.ContainsAny(target.TeamID, "\r\n") {
			return fmt.Errorf("target %d has invalid team_id", i)
		}
		if !projectPattern.MatchString(target.LabID) {
			return fmt.Errorf("target %d lab_id must be a UUID", i)
		}
		if labID == "" {
			labID = target.LabID
		} else if target.LabID != labID {
			return fmt.Errorf("target %d lab_id differs from the run inventory", i)
		}
		if !databasePattern.MatchString(target.DatabaseID) {
			return fmt.Errorf("target %d database_id must be the actual beads_perf_lab_* database name", i)
		}
		if _, exists := seenDB[target.DatabaseID]; exists {
			return fmt.Errorf("duplicate database_id %q", target.DatabaseID)
		}
		seenDB[target.DatabaseID] = struct{}{}
		if !projectPattern.MatchString(target.ProjectID) {
			return fmt.Errorf("target %d project_id must be a UUID", i)
		}
		if _, exists := seenProject[target.ProjectID]; exists {
			return fmt.Errorf("duplicate project_id %q", target.ProjectID)
		}
		seenProject[target.ProjectID] = struct{}{}
		if err := validateLoopbackOrigin(target.BaseURL); err != nil {
			return fmt.Errorf("target %d base_url: %w", i, err)
		}
		if target.TokenFile == "" {
			return fmt.Errorf("target %d token_file is required", i)
		}
		if len(target.FixtureIssueID) > 200 || strings.ContainsAny(target.FixtureIssueID, "\r\n\x00") {
			return fmt.Errorf("target %d fixture_issue_id is invalid", i)
		}
		if target.HotAE {
			hotCount++
		}
	}
	if hotCount != 1 {
		return fmt.Errorf("exactly one target must set hot_ae")
	}
	return nil
}

func validateLoopbackOrigin(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("must be a plain HTTP loopback origin")
	}
	ip := net.ParseIP(u.Hostname())
	if ip == nil || !ip.IsLoopback() || u.Port() == "" {
		return fmt.Errorf("must use a numeric loopback host and explicit port")
	}
	return nil
}

func capacityInterpretation(mode string) string {
	if mode == modeOpenLoop {
		return "non_capacity_failure_envelope_only"
	}
	return "closed_loop_evidence_gated"
}

func selectActiveTargets(targets []targetConfig, count int, seed int64) []targetConfig {
	var hot targetConfig
	others := make([]targetConfig, 0, len(targets)-1)
	for _, target := range targets {
		if target.HotAE {
			hot = target
		} else {
			others = append(others, target)
		}
	}
	sort.Slice(others, func(i, j int) bool { return others[i].DatabaseID < others[j].DatabaseID })
	rng := rand.New(rand.NewSource(deriveSeed(seed, "active-targets", 0))) //nolint:gosec // G404: lab workload RNG is not security-sensitive
	rng.Shuffle(len(others), func(i, j int) { others[i], others[j] = others[j], others[i] })
	active := append([]targetConfig{hot}, others[:count-1]...)
	sort.Slice(active, func(i, j int) bool { return active[i].DatabaseID < active[j].DatabaseID })
	return active
}

func deriveSeed(master int64, label string, ordinal int) int64 {
	h := fnv.New64a()
	_, _ = fmt.Fprintf(h, "%d|%s|%d", master, label, ordinal)
	return int64(h.Sum64())
}

func sampleThink(rng *rand.Rand, mean time.Duration) time.Duration {
	return time.Duration(float64(mean) * (.8 + rng.Float64()*.4))
}

// steadyStateInitialPhase avoids measuring the synchronized cold start of all
// actors. A uniform phase across the frozen model cycle is exact for a
// constant-period renewal process and is a bounded approximation for this
// runner's narrow think-time jitter. The load-fidelity gate independently
// rejects a finite run whose realized rate still differs materially.
func steadyStateInitialPhase(rng *rand.Rand, p workloadProfile) time.Duration {
	cycleSeconds := p.ThinkTimeSeconds + p.CommandsPerCycle*assumedResponseSeconds
	if cycleSeconds <= 0 {
		return 0
	}
	return time.Duration(rng.Float64() * cycleSeconds * float64(time.Second))
}

func sampleCommandCount(rng *rand.Rand, mean float64) int {
	base := int(math.Floor(mean))
	if rng.Float64() < mean-float64(base) {
		base++
	}
	if base < 1 {
		return 1
	}
	return base
}
