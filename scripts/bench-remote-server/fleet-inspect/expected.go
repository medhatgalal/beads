package main

import (
	"fmt"
	"sort"
	"strings"
)

func deriveExpected(inv inventory, value runnerReport) (expectedRun, error) {
	if err := validateInventoryAgainstReport(inv, value); err != nil {
		return expectedRun{}, err
	}
	if !runIDPattern.MatchString(value.Config.RunID) {
		return expectedRun{}, fmt.Errorf("runner report run_id is invalid")
	}
	if value.Metrics.Generated < 0 || value.Metrics.Generated > maxExpectedOperations ||
		value.Metrics.Generated != value.Metrics.Completed || value.Metrics.Dropped != 0 ||
		value.Metrics.Unclassified != 0 || !value.Metrics.AccountingBalanced ||
		value.Metrics.Passed != value.Metrics.Generated || value.Metrics.Failed != 0 {
		return expectedRun{}, fmt.Errorf("runner outcome accounting is not complete and successful")
	}
	if int64(len(value.Samples)) != value.Metrics.Generated {
		return expectedRun{}, fmt.Errorf("runner samples are incomplete: have %d want %d", len(value.Samples), value.Metrics.Generated)
	}
	sampleEvidence := value.BoundedEvidence.SampleRecords
	if sampleEvidence.Truncated || sampleEvidence.Seen != value.Metrics.Generated || int64(sampleEvidence.Retained) != value.Metrics.Generated {
		return expectedRun{}, fmt.Errorf("runner sample evidence is truncated or incomplete")
	}
	resultEvidence := value.BoundedEvidence.ResultLatency
	if resultEvidence.Truncated || resultEvidence.Seen != value.Metrics.Generated || int64(resultEvidence.Retained) != value.Metrics.Generated {
		return expectedRun{}, fmt.Errorf("runner result evidence is truncated or incomplete")
	}
	writes := value.Metrics.PointWrites + value.Metrics.Graphs
	operationEvidence := value.BoundedEvidence.OperationIDProof
	if writes < 0 || writes > maxExpectedOperations || operationEvidence.Truncated || operationEvidence.Seen != writes || int64(operationEvidence.Retained) != writes {
		return expectedRun{}, fmt.Errorf("runner operation-ID evidence is truncated or incomplete")
	}
	if correctnessTotal(value) != 0 {
		return expectedRun{}, fmt.Errorf("runner correctness counters are nonzero")
	}

	targets := inventoryByDatabase(inv)
	active := make(map[string]struct{}, len(value.Config.ActiveDatabaseIDs))
	for _, databaseID := range value.Config.ActiveDatabaseIDs {
		active[databaseID] = struct{}{}
	}
	result := expectedRun{
		RunID: value.Config.RunID, LabID: value.Config.LabID,
		ByDatabase: make(map[string]*expectedDatabase, len(inv.Targets)),
		Operations: make(map[string]expectedOperation, writes),
	}
	for _, target := range inv.Targets {
		result.ByDatabase[target.DatabaseID] = &expectedDatabase{
			DatabaseID: target.DatabaseID, ProjectID: target.ProjectID,
			Operations: map[string]expectedOperation{}, Titles: map[string]struct{}{},
			Edges: map[string]struct{}{}, Events: map[string]struct{}{},
		}
	}
	seenIdentity := map[string]struct{}{}
	seenOrdinal := map[int64]struct{}{}
	sampleCounts := map[string]int64{}
	readCount, pointCount, graphCount := int64(0), int64(0), int64(0)
	for index, sample := range value.Samples {
		if sample.Identity == "" || len(sample.Identity) > 160 || strings.ContainsAny(sample.Identity, "\x00\r\n") {
			return expectedRun{}, fmt.Errorf("sample %d has invalid identity", index)
		}
		if _, duplicate := seenIdentity[sample.Identity]; duplicate {
			return expectedRun{}, fmt.Errorf("sample %d repeats identity %q", index, sample.Identity)
		}
		if sample.Ordinal < 0 {
			return expectedRun{}, fmt.Errorf("sample %d has a negative ordinal", index)
		}
		if _, duplicate := seenOrdinal[sample.Ordinal]; duplicate {
			return expectedRun{}, fmt.Errorf("sample %d repeats ordinal %d", index, sample.Ordinal)
		}
		seenIdentity[sample.Identity] = struct{}{}
		seenOrdinal[sample.Ordinal] = struct{}{}
		target, ok := targets[sample.DatabaseID]
		if !ok {
			return expectedRun{}, fmt.Errorf("sample %d names database outside inventory", index)
		}
		if _, ok := active[sample.DatabaseID]; !ok {
			return expectedRun{}, fmt.Errorf("sample %d names an inactive database", index)
		}
		if !sample.Passed || sample.Unclassified || sample.ErrorClass != "" || sample.HTTPStatus < 200 || sample.HTTPStatus >= 300 {
			return expectedRun{}, fmt.Errorf("sample %d is not a complete successful result", index)
		}
		sampleCounts[sample.DatabaseID]++
		switch sample.Kind {
		case "read":
			if sample.Command != "ping" && sample.Command != "list" && sample.Command != "ready" && sample.Command != "show" {
				return expectedRun{}, fmt.Errorf("read sample %d has unexpected command", index)
			}
			if sample.OperationID != "" {
				return expectedRun{}, fmt.Errorf("read sample %d unexpectedly has an operation_id", index)
			}
			readCount++
			continue
		case "point_write":
			if sample.Command != "create" {
				return expectedRun{}, fmt.Errorf("point write sample %d has unexpected command", index)
			}
			pointCount++
		case "graph":
			if sample.Command != "graph" {
				return expectedRun{}, fmt.Errorf("graph sample %d has unexpected command", index)
			}
			graphCount++
		default:
			return expectedRun{}, fmt.Errorf("sample %d has unsupported kind %q", index, sample.Kind)
		}
		if !operationIDPattern.MatchString(sample.OperationID) {
			return expectedRun{}, fmt.Errorf("write sample %d has missing or invalid operation_id", index)
		}
		if _, duplicate := result.Operations[sample.OperationID]; duplicate {
			return expectedRun{}, fmt.Errorf("operation_id %q is duplicated", sample.OperationID)
		}
		op := buildExpectedOperation(value.Config.RunID, sample)
		database := result.ByDatabase[target.DatabaseID]
		database.Operations[op.ID] = op
		result.Operations[op.ID] = op
		for _, title := range op.Titles {
			if _, duplicate := database.Titles[title]; duplicate {
				return expectedRun{}, fmt.Errorf("derived duplicate issue title %q", title)
			}
			database.Titles[title] = struct{}{}
			database.Events[eventKey(title, "created")] = struct{}{}
			result.IssueCount++
			result.EventCount++
			if result.IssueCount > maxExpectedIssues {
				return expectedRun{}, fmt.Errorf("derived issue count exceeds inspection bound")
			}
		}
		for _, edge := range op.Edges {
			key := edgeKey(edge.FromTitle, edge.ToTitle, edge.Type)
			if _, duplicate := database.Edges[key]; duplicate {
				return expectedRun{}, fmt.Errorf("derived duplicate dependency edge")
			}
			database.Edges[key] = struct{}{}
			result.EdgeCount++
		}
	}
	if readCount != value.Metrics.ReadCommands || pointCount != value.Metrics.PointWrites || graphCount != value.Metrics.Graphs {
		return expectedRun{}, fmt.Errorf("sample kinds do not match runner exact counters")
	}
	if len(value.Metrics.ByDatabase) != len(sampleCounts) {
		return expectedRun{}, fmt.Errorf("by_database accounting does not cover the complete sample set")
	}
	for databaseID, group := range value.Metrics.ByDatabase {
		database := result.ByDatabase[databaseID]
		if database == nil {
			return expectedRun{}, fmt.Errorf("by_database contains database outside inventory")
		}
		actual := sampleCounts[databaseID]
		if group.Commands != actual || group.Passed != actual || group.Failed != 0 || group.Unclassified != 0 {
			return expectedRun{}, fmt.Errorf("by_database accounting mismatch for %s", databaseID)
		}
	}
	if len(sampleCounts) != len(active) {
		return expectedRun{}, fmt.Errorf("runner samples do not exercise every active database")
	}
	return result, nil
}

func correctnessTotal(value runnerReport) int64 {
	c := value.Correctness
	return c.HTTPFailures + c.InvalidJSON + c.TerminalFailures + c.NonterminalTimeouts +
		c.ProjectMismatches + c.DuplicateOperationIDs + c.MissingOperationIDs + c.DroppedAtAdmission + c.UnclassifiedOutcomes
}

func buildExpectedOperation(runID string, sample runnerSample) expectedOperation {
	prefix := runTitlePrefix(runID)
	op := expectedOperation{ID: sample.OperationID, Identity: sample.Identity, Database: sample.DatabaseID}
	if sample.Kind == "point_write" {
		op.Kind = "issue.create"
		op.Titles = []string{prefix + sample.Identity}
		return op
	}
	op.Kind = "graph.apply"
	op.Titles = make([]string, 100)
	for index := range op.Titles {
		op.Titles[index] = fmt.Sprintf("%s%s node %03d", prefix, sample.Identity, index)
	}
	for distance := 1; len(op.Edges) < 200; distance++ {
		for from := distance; from < len(op.Titles) && len(op.Edges) < 200; from++ {
			op.Edges = append(op.Edges, expectedEdge{
				FromTitle: op.Titles[from], ToTitle: op.Titles[from-distance], Type: "blocks",
			})
		}
	}
	return op
}

func runTitlePrefix(runID string) string {
	return "[beads-perf-lab] realistic " + runID + " "
}

func edgeKey(fromTitle, toTitle, edgeType string) string {
	return strings.Join([]string{fromTitle, toTitle, edgeType}, "\x00")
}

func eventKey(title, eventType string) string {
	return title + "\x00" + eventType
}

func sortedOperationIDs(operations map[string]expectedOperation) []string {
	ids := make([]string, 0, len(operations))
	for id := range operations {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
