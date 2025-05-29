package logstore

import (
	"log"
	"math"
	"slices"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TODO: perhaps make these fields private?
type Metronome struct {
	ReplicaID roachpb.ReplicaID
	Schemes   [][]roachpb.ReplicaID
}

func (m *Metronome) ShouldFlush(raftIndex uint64) bool {
	// If schemes are not initialized flush
	if len(m.Schemes) == 0 {
		return true
	}

	if raftIndex > math.MaxInt {
		// handle overflow, error, or fallback
		log.Fatalf("raftIndex %d is too large for int", raftIndex)
		return true
	}

	index := int(raftIndex)
	scheme := m.Schemes[index%len(m.Schemes)]

	return slices.Contains(scheme, m.ReplicaID)
}

func SortQuorums(quorums [][]roachpb.ReplicaID) {
	// Step 1: sort each quorum slice individually
	for _, quorum := range quorums {
		slices.Sort(quorum)
	}

	// Step 2: sort the outer slice lexicographically
	sort.Slice(quorums, func(i, j int) bool {
		a := quorums[i]
		b := quorums[j]
		for k := 0; k < len(a) && k < len(b); k++ {
			if a[k] < b[k] {
				return true
			}
			if a[k] > b[k] {
				return false
			}
		}
		return len(a) < len(b)
	})
}

func countOverlapping(a, b []roachpb.ReplicaID) int {
	if len(a) != len(b) {
		panic("slices must be of equal length")
	}

	count := 0
	for i := range a {
		if slices.Contains(b, a[i]) {
			count++
		}
	}
	return count
}

func RebalanceQuorums(quorums [][]roachpb.ReplicaID) {
	minIndex := -1
	minCount := int(^uint(0) >> 1)

	for i := range quorums[:len(quorums)-1] {
		for j := i + 1; j < len(quorums); j++ {
			temp := countOverlapping(quorums[j], quorums[i])
			if minIndex == -1 || temp < minCount {
				minCount = temp
				minIndex = j
			}
		}

		quorums[i+1], quorums[minIndex] = quorums[minIndex], quorums[i+1]

		minIndex = -1
		minCount = int(^uint(0) >> 1)
	}
}
