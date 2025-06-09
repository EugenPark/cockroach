package logstore

import (
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type timeout chan struct{}

type timeoutQueue struct {
	queue map[raftpb.Index]timeout
}

func newTimeoutQueue() timeoutQueue {
	return timeoutQueue{
		queue: make(map[raftpb.Index]timeout),
	}
}

func (tq *timeoutQueue) addTimeout(index raftpb.Index, duration time.Duration, onTimeout func()) {

	timer := time.NewTimer(duration)

	timeout := make(chan struct{})
	tq.queue[index] = timeout

	go func() {
		select {
		case <-timer.C:
			onTimeout()
		case <-timeout:
			if !timer.Stop() {
				<-timer.C // Drain the channel to avoid leaks
			}
		}
	}()
}

func (tq *timeoutQueue) cancelTimeout(index raftpb.Index) {
	// This was an index which was flushed so no need to cancel anything
	if tq.queue[index] == nil {
		return
	}

	close(tq.queue[index])
	tq.queue[index] = nil
}

type Metronome struct {
	replicaID        roachpb.ReplicaID
	schemes          [][]roachpb.ReplicaID
	inflightQueue    timeoutQueue
	unflushedEntries map[raftpb.Index]raftpb.Entry
}

type MetronomeEntry struct {
	Entry       raftpb.Entry
	ShouldFlush bool
}

func InitializeMetronome(replicaID roachpb.ReplicaID) Metronome {
	return Metronome{
		replicaID:        replicaID,
		inflightQueue:    newTimeoutQueue(),
		unflushedEntries: make(map[raftpb.Index]raftpb.Entry),
	}
}

func (m *Metronome) AddUnflushedEntry(ent raftpb.Entry) {
	m.unflushedEntries[raftpb.Index(ent.Index)] = ent
}

func (m *Metronome) SetSchemes(schemes [][]roachpb.ReplicaID) {
	m.schemes = schemes
}

func (m *Metronome) GetSchemes() [][]roachpb.ReplicaID {
	return m.schemes
}

func (m *Metronome) Commit(toApply []raftpb.Entry) {
	for _, ent := range toApply {
		m.inflightQueue.cancelTimeout(raftpb.Index(ent.Index))
	}
}

func (m *Metronome) ShouldRebalance(otherScheme []roachpb.ReplicaID) bool {
	// At least three replicas are required in crdb as such a quorum length of 2 is needed
	if len(otherScheme) < 2 {
		return false
	}

	if m.schemes == nil {
		return true
	}

	scheme := m.schemes[0]
	if len(scheme) != len(otherScheme) {
		return true
	}

	freq := make(map[roachpb.ReplicaID]int, len(otherScheme))

	// if the nodes are still the same set then do not rebalance
	for _, v := range scheme {
		freq[v]++
	}

	for _, v := range otherScheme {
		freq[v]--
		if freq[v] < 0 {
			return true
		}
	}

	return false
}

func (m *Metronome) FilterEntries(entries []raftpb.Entry, cb func(ent raftpb.Entry)) []MetronomeEntry {
	min := 200  // milliseconds
	max := 1000 // milliseconds
	randomMs := rand.Intn(max-min+1) + min
	duration := time.Duration(randomMs) * time.Millisecond

	filteredEntries := make([]MetronomeEntry, 0, len(entries))

	for _, ent := range entries {
		shouldFlush := m.shouldFlush(ent.Index)

		filteredEntries = append(filteredEntries, MetronomeEntry{
			Entry:       ent,
			ShouldFlush: shouldFlush,
		})

		if !shouldFlush {
			m.unflushedEntries[raftpb.Index(ent.Index)] = ent
			m.inflightQueue.addTimeout(raftpb.Index(ent.Index), duration, func() {
				cb(ent)
				delete(m.unflushedEntries, raftpb.Index(ent.Index))
			})
		}
	}

	return filteredEntries
}

func (m *Metronome) shouldFlush(raftIndex uint64) bool {
	// If schemes are not initialized flush
	if m.schemes == nil || len(m.schemes) == 0 {
		return true
	}

	if raftIndex > math.MaxInt {
		// When overflow flush to guarantee safety
		fmt.Printf("RaftIndex %d is too large for int\n", raftIndex)
		return true
	}

	index := int(raftIndex)
	scheme := m.schemes[index%len(m.schemes)]

	return slices.Contains(scheme, m.replicaID)
}

func sortQuorums(quorums [][]roachpb.ReplicaID) {
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
	sortQuorums(quorums)

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
