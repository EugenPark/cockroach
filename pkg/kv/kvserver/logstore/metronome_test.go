package logstore

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

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
	SortQuorums(quorums)

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

func TestMetronomeShouldFlush(t *testing.T) {
	metronome := Metronome{
		ReplicaID: roachpb.ReplicaID(2),
		Schemes: [][]roachpb.ReplicaID{
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

	result := metronome.ShouldFlush(index)
	expected := false

	if expected != result {
		t.Errorf("Telling to flush even though it is not our turn")
	}

	index = uint64(12)

	result = metronome.ShouldFlush(index)
	expected = true

	if expected != result {
		t.Errorf("Telling to not flush even though it is our turn")
	}
}
