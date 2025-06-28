package logstore

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Helper
func compareQuorums(expected, result [][]roachpb.ReplicaID, t *testing.T) {
	if len(expected) != len(result) {
		t.Errorf("Quorums are of unequal length")
	}

	for i := range expected {
		if len(expected[i]) != len(result[i]) {
			t.Errorf("Slice length mismatch at %d: expected %d, result %d", i, len(expected[i]), len(result[i]))
		}

		for j := range result[i] {
			if expected[i][j] != result[i][j] {
				t.Errorf("Values do not match at %d, %d: expected %d, result %d", i, j, expected[i][j], result[i][j])
			}
		}
	}
}

// Test Metronome Helper functions
func TestCountOverlapping(t *testing.T) {
	a := []roachpb.ReplicaID{1, 2, 3}
	b := []roachpb.ReplicaID{1, 4, 5}

	result := countOverlapping(a, b)
	expected := 1

	if result != expected {
		t.Errorf("Not accurate overlap count scenario 1: expected %d, result %d", expected, result)
	}

	a = []roachpb.ReplicaID{1, 2, 3}
	b = []roachpb.ReplicaID{2, 3, 4}

	result = countOverlapping(a, b)
	expected = 2

	if result != expected {
		t.Errorf("Not accurate overlap count scenario 2: expected %d, result %d", expected, result)
	}
}

func TestRebalanceQuorums(t *testing.T) {
	quorums := [][]roachpb.ReplicaID{
		{1, 2, 3},
		{1, 2, 4},
		{1, 2, 5},
		{1, 3, 4},
		{1, 3, 5},
		{1, 4, 5},
		{2, 3, 4},
		{2, 3, 5},
		{2, 4, 5},
		{3, 4, 5},
	}
	RebalanceQuorums(quorums)

	expected := [][]roachpb.ReplicaID{
		{1, 2, 3},
		{1, 4, 5},
		{2, 3, 4},
		{1, 3, 5},
		{1, 2, 4},
		{2, 3, 5},
		{1, 3, 4},
		{1, 2, 5},
		{3, 4, 5},
		{2, 4, 5},
	}

	compareQuorums(expected, quorums, t)
}

func TestSortQuorums(t *testing.T) {
	quorums := [][]roachpb.ReplicaID{
		{1, 2, 3},
		{1, 4, 5},
		{2, 3, 4},
		{1, 3, 5},
		{1, 2, 4},
		{2, 3, 5},
		{1, 3, 4},
		{1, 2, 5},
		{3, 4, 5},
		{2, 4, 5},
	}
	sortQuorums(quorums)

	expected := [][]roachpb.ReplicaID{
		{1, 2, 3},
		{1, 2, 4},
		{1, 2, 5},
		{1, 3, 4},
		{1, 3, 5},
		{1, 4, 5},
		{2, 3, 4},
		{2, 3, 5},
		{2, 4, 5},
		{3, 4, 5},
	}

	compareQuorums(expected, quorums, t)
}

// Test Metronome
func TestMetronomeShouldFlush(t *testing.T) {
	metronome := Metronome{
		replicaID: roachpb.ReplicaID(2),
		schemes: [][]roachpb.ReplicaID{
			{1, 2, 3},
			{1, 4, 5},
			{2, 3, 4},
			{1, 3, 5},
			{1, 2, 4},
			{2, 3, 5},
			{1, 3, 4},
			{1, 2, 5},
			{3, 4, 5},
			{2, 4, 5},
		},
	}
	index := uint64(11)

	result := metronome.shouldFlush(index)
	expected := false

	if expected != result {
		t.Errorf("Telling to flush even though it is not our turn")
	}

	index = uint64(12)

	result = metronome.shouldFlush(index)
	expected = true

	if expected != result {
		t.Errorf("Telling to not flush even though it is our turn")
	}
}

func TestGetMissingIndices(t *testing.T) {
	metronome := Metronome{
		replicaID: roachpb.ReplicaID(1),
		schemes: [][]roachpb.ReplicaID{
			{1, 2, 3},
			{1, 4, 5},
			{2, 3, 4},
			{1, 3, 5},
			{1, 2, 4},
			{2, 3, 5},
			{1, 3, 4},
			{1, 2, 5},
			{3, 4, 5},
			{2, 4, 5},
		},
	}

	// Test normal case without bound violations
	expected := []uint64{
		5, 6, 7, 8, 9, 10,
	}
	actual := metronome.GetMissingIndices(5, 10, 10, nil)

	if len(expected) != len(actual) {
		t.Fatalf("Indices do not match up. Expected %#v Got %#v\n", expected, actual)
	}

	for i := range actual {
		if expected[i] != actual[i] {
			t.Fatalf("Index does not match up. Expected %d Got %d\n", expected[i], actual[i])
		}
	}

	// Test normal case without bound violations but with a filter
	expected = []uint64{
		6, 7, 9, 10,
	}
	actual = metronome.GetMissingIndices(5, 10, 10, []uint64{5, 8})

	if len(expected) != len(actual) {
		t.Fatalf("Indices do not match up in filter test case. Expected %#v Got %#v\n", expected, actual)
	}

	for i := range actual {
		if expected[i] != actual[i] {
			t.Fatalf("Index does not match up in filter test case. Expected %d Got %d\n", expected[i], actual[i])
		}
	}

	// Test case with upper bound violations
	expected = []uint64{
		5, 6, 7, 8, 9,
	}
	actual = metronome.GetMissingIndices(5, 7, 7, nil)

	if len(expected) != len(actual) {
		t.Fatalf("Indices do not match up. Expected %#v Got %#v\n", expected, actual)
	}

	for i := range actual {
		if expected[i] != actual[i] {
			t.Fatalf("Index does not match up. Expected %d Got %d\n", expected[i], actual[i])
		}
	}

	// Test case with lower bound violations
	expected = []uint64{
		8, 9, 10, 11, 12, 13,
	}
	actual = metronome.GetMissingIndices(10, 13, 13, nil)

	if len(expected) != len(actual) {
		t.Fatalf("Indices do not match up. Expected %#v Got %#v\n", expected, actual)
	}

	for i := range actual {
		if expected[i] != actual[i] {
			t.Fatalf("Index does not match up. Expected %d Got %d\n", expected[i], actual[i])
		}
	}

	// Test case with both bound violations
	expected = []uint64{
		2, 3, 4, 5, 6, 7, 8, 9,
	}
	actual = metronome.GetMissingIndices(3, 7, 7, nil)

	if len(expected) != len(actual) {
		t.Fatalf("Indices do not match up. Expected %#v Got %#v\n", expected, actual)
	}

	for i := range actual {
		if expected[i] != actual[i] {
			t.Fatalf("Index does not match up. Expected %d Got %d\n", expected[i], actual[i])
		}
	}

	// Test case of commit being higher
	expected = []uint64{
		24, 27, 29, 30, 31, 32, 33,
	}

	actual = metronome.GetMissingIndices(22, 28, 33, []uint64{22, 23, 25, 26, 28})

	if len(expected) != len(actual) {
		t.Fatalf("Indices do not match up. Expected %v Got %v\n", expected, actual)
	}

	for i := range actual {
		if expected[i] != actual[i] {
			t.Fatalf("Index does not match up. Expected %d Got %d\n", expected[i], actual[i])
		}
	}

	// Test case of commit being lower
	expected = []uint64{
		24, 27, 29,
	}

	actual = metronome.GetMissingIndices(22, 28, 25, []uint64{22, 23, 25, 26, 28})

	if len(expected) != len(actual) {
		t.Fatalf("Indices do not match up. Expected %v Got %v\n", expected, actual)
	}

	for i := range actual {
		if expected[i] != actual[i] {
			t.Fatalf("Index does not match up. Expected %d Got %d\n", expected[i], actual[i])
		}
	}
}

// Test Timeoutqueue
func TestTimeoutQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	tq := newTimeoutQueue(stopper)

	val := 1
	changeVal := func() {
		val = 5
	}

	// Test OnTimeout
	tq.addTimeout(ctx, raftpb.Index(1), time.Duration(10)*time.Millisecond, changeVal)
	time.Sleep(time.Duration(100) * time.Millisecond)

	if val != 5 {
		t.Errorf("Timeout not triggered properly")
	}

	val = 3
	// Test cancellation
	tq.addTimeout(ctx, raftpb.Index(2), time.Duration(10)*time.Millisecond, changeVal)
	tq.cancelTimeout(raftpb.Index(2))
	time.Sleep(time.Duration(100) * time.Millisecond)

	if val != 3 {
		t.Errorf("Timeout cancellation not completed successfully")
	}
}

// Test RaftLogMap
func TestRaftLogMap(t *testing.T) {
	rlm := NewRaftLogMap()
	entries := []raftpb.Entry{
		{Index: 1, Data: []byte{1}, Term: 2},
		{Index: 4, Data: []byte{4}, Term: 2},
		{Index: 5, Data: []byte{5}, Term: 2},
	}

	// Test adding, getting and removing entries
	rlm.Add(entries[:1])
	actual, exists := rlm.Get(1)

	expected := entries[0]
	if !exists || actual.Data[0] != expected.Data[0] {
		t.Fatalf("Added entry with data %d != retrieved entry with data %d\n", expected.Data[0], actual.Data[0])
	}

	rlm.Remove(1)
	if len(rlm.entries) != 0 {
		t.Fatalf("Entry was not deleted properly\n")
	}

	// Test Adding in multiple entries with holes getting last entry and ordering of log
	rlm.Add(entries)

	actual, exists = rlm.GetLast()
	expected = entries[len(entries)-1]
	if !exists || actual.Data[0] != expected.Data[0] {
		t.Fatalf("Failed to retrieve last entry got %d expected %d\n", actual.Data[0], expected.Data[0])
	}

	for i, ent := range rlm.entries {
		if ent.Data[0] != entries[i].Data[0] {
			t.Fatalf("Log is not ordered properly got %d expected %d\n", ent.Data[0], entries[i].Data[0])
		}
	}

	scannedEntries := rlm.GetLog(2, 5)
	if len(scannedEntries) != 1 || scannedEntries[0].Index != 4 {
		t.Fatalf("Failed to retrieve log in range, got %d expected %d\n", scannedEntries[0].Index, 4)
	}

	// Test compaction
	rlm.Compact(3)
	for i, ent := range rlm.entries {
		if ent.Data[0] != entries[i+1].Data[0] {
			t.Fatalf("Compaction did not succeed got %d expected %d\n", ent.Data[0], entries[i+3].Data[0])
		}
	}

	// Test removing stale entries
	additionalEntries := []raftpb.Entry{
		{Index: 7, Data: []byte{7}, Term: 2},
		{Index: 8, Data: []byte{8}, Term: 2},
	}
	rlm.Add(additionalEntries)

	overLappingEntry := []raftpb.Entry{
		{
			Index: 6, Data: []byte{6}, Term: 6,
		},
	}

	rlm.Add(overLappingEntry)
	lastEntry, exists := rlm.GetLast()
	if !exists || lastEntry.Index != overLappingEntry[0].Index || lastEntry.Term != overLappingEntry[0].Term {
		t.Fatalf("RemoveStale did not succeed got %#v expected %#v\n", lastEntry, overLappingEntry)
	}
}

func TestMergeRaftLogs(t *testing.T) {
	// Base RaftLogMap with some missing entries
	rlm := NewRaftLogMap()
	entries := []raftpb.Entry{
		{Index: 3, Data: []byte{3}, Term: 1},
		{Index: 4, Data: []byte{4}, Term: 1},
		{Index: 6, Data: []byte{6}, Term: 1},
		{Index: 8, Data: []byte{8}, Term: 1},
	}
	rlm.Add(entries)

	// Other log has entries that can fill the gaps
	otherEntries := []raftpb.Entry{
		{Index: 1, Data: []byte{1}, Term: 1},
		{Index: 7, Data: []byte{7}, Term: 1},
	}

	rlm.MergeRaftLogs(otherEntries)

	// Expect merged log to start at index 1 and be fully contiguous to index 6
	expectedIndices := []uint64{6, 7, 8}
	expectedData := []byte{6, 7, 8}

	actualLog := rlm.entries
	if len(actualLog) != len(expectedIndices) {
		t.Fatalf("Merged log length mismatch: got %d, expected %d", len(actualLog), len(expectedIndices))
	}

	for i, entry := range actualLog {
		if entry.Index != expectedIndices[i] {
			t.Fatalf("Log index mismatch at pos %d: got %d, expected %d", i, entry.Index, expectedIndices[i])
		}
		if entry.Data[0] != expectedData[i] {
			t.Fatalf("Log data mismatch at pos %d: got %d, expected %d", i, entry.Data[0], expectedData[i])
		}
	}
}

func TestClearLog(t *testing.T) {
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	metronome := InitializeMetronome(1, stopper)

	metronome.AppendEntries([]raftpb.Entry{{Index: 1}, {Index: 4}, {Index: 5}})
	metronome.ClearEntries()

	log := metronome.GetEntries(1, 5)
	if len(log) != 0 {
		t.Fatalf("Failed to clear the log: %#v\n", log)
	}
}
