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

// Timeout Logic
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

// RaftLogMap Logic for allowing logs with gaps
type RaftLogMap struct {
	entries []raftpb.Entry
}

func NewRaftLogMap() RaftLogMap {
	return RaftLogMap{
		entries: make([]raftpb.Entry, 0, 0),
	}
}

// Invariant:
// - Entries are always ordered
// - RLM log is ordered at any point
func (rlm *RaftLogMap) Add(entries []raftpb.Entry) {
	if len(entries) == 0 {
		return
	}

	if len(rlm.entries) == 0 || rlm.entries[len(rlm.entries)-1].Index < entries[len(entries)-1].Index {
		// Remove any stale entries
		for i, ent := range rlm.entries {
			if ent.Index >= entries[0].Index && ent.Term < entries[0].Term {
				rlm.entries = rlm.entries[:i]
				break
			}
		}
	}

	// Append new entries
	rlm.entries = append(rlm.entries, entries...)
}

// func (rlm *RaftLogMap) Add(ent raftpb.Entry) {
// 	newEntries := rlm.entries[:0]
// 	for _, ownEnt := range rlm.entries {
// 		// Stale entries
// 		if ownEnt.Index >= ent.Index && ownEnt.Term < ent.Term {
// 			continue
// 		}
//
// 		newEntries = append(newEntries, ownEnt)
// 	}
// 	newEntries = append(newEntries, ent)
// 	rlm.entries = newEntries
// }

func (rlm *RaftLogMap) Remove(index uint64) {
	for i, ent := range rlm.entries {
		if ent.Index == index {
			rlm.entries = slices.Delete(rlm.entries, i, i+1)
			// Invariant: There is only one entry at a time with the same index
			break
		}
	}
}

func (rlm *RaftLogMap) GetLast() (raftpb.Entry, bool) {
	if len(rlm.entries) == 0 {
		return raftpb.Entry{}, false
	}

	ent := rlm.entries[len(rlm.entries)-1]
	return ent, true
}

// Returns entries in [lo, hi)
func (rlm *RaftLogMap) GetLog(lo, hi uint64) []raftpb.Entry {
	var entries []raftpb.Entry

	for _, ent := range rlm.entries {
		if lo <= ent.Index && ent.Index < hi {
			entries = append(entries, ent)
		}
	}

	return entries
}

func (rlm *RaftLogMap) Compact(index uint64) {
	keepEntries := rlm.entries[:0]

	for _, ent := range rlm.entries {
		if ent.Index <= index {
			continue
		}
		keepEntries = append(keepEntries, ent)
	}

	rlm.entries = keepEntries
}

func (rlm *RaftLogMap) MergeRaftLogs(otherEntries []raftpb.Entry) {

	if len(rlm.entries) == 0 || len(otherEntries) == 0 {
		rlm.Add(otherEntries)
		return
	}

	var mergedEntries []raftpb.Entry
	var index uint64
	if rlm.entries[0].Index < otherEntries[0].Index {
		index = rlm.entries[0].Index
	} else {
		index = otherEntries[0].Index
	}

	var maxIndex uint64
	if rlm.entries[len(rlm.entries)-1].Index > otherEntries[len(otherEntries)-1].Index {
		maxIndex = rlm.entries[len(rlm.entries)-1].Index
	} else {
		maxIndex = otherEntries[len(otherEntries)-1].Index
	}

	j := 0
	i := 0

	// Invariant: Logs are ordered otherwise this might run incorrectly
	// or indefinetely
	for index <= maxIndex {
		// Index found in original raftmaplog
		if i < len(rlm.entries) && index == rlm.entries[i].Index {
			mergedEntries = append(mergedEntries, rlm.entries[i])
			i++
			index++
			continue
		}

		// Index found in other raftmaplog
		if j < len(otherEntries) && index == otherEntries[j].Index {
			mergedEntries = append(mergedEntries, otherEntries[j])
			j++
			index++
			continue
		}

		// Did not find any matching entry meaning that we need to
		// clear the entries found so far
		mergedEntries = mergedEntries[:0]
		index++
	}

	rlm.entries = mergedEntries
}

// Metronome Logic
type Metronome struct {
	replicaID        roachpb.ReplicaID
	schemes          [][]roachpb.ReplicaID
	inflightQueue    timeoutQueue
	unflushedEntries RaftLogMap
}

func InitializeMetronome(replicaID roachpb.ReplicaID) *Metronome {
	m := &Metronome{
		replicaID:        replicaID,
		inflightQueue:    newTimeoutQueue(),
		unflushedEntries: NewRaftLogMap(),
	}

	return m
}

func (m *Metronome) GetUnflushedEntries() *RaftLogMap {
	return &m.unflushedEntries
}

func (m *Metronome) SetSchemes(schemes [][]roachpb.ReplicaID) {
	m.schemes = schemes
}

func (m *Metronome) GetSchemes() [][]roachpb.ReplicaID {
	return m.schemes
}

func (m *Metronome) Commit(toApply []raftpb.Entry) {
	if m == nil {
		return
	}

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

func (m *Metronome) FilterEntries(entries []raftpb.Entry, cb func(ent raftpb.Entry)) ([]raftpb.Entry, raftpb.Entry) {
	min := 200  // milliseconds
	max := 1000 // milliseconds
	randomMs := rand.Intn(max-min+1) + min
	duration := time.Duration(randomMs) * time.Millisecond

	unfilteredEntries := make([]raftpb.Entry, 0, len(entries))
	filteredEntries := make([]raftpb.Entry, 0, len(entries))
	lastEntry := entries[len(entries)-1]

	for _, ent := range entries {
		shouldFlush := m.shouldFlush(ent.Index)

		if shouldFlush {
			unfilteredEntries = append(unfilteredEntries, ent)
		} else {
			m.inflightQueue.addTimeout(raftpb.Index(ent.Index), duration, func() {
				cb(ent)
				m.unflushedEntries.Remove(ent.Index)
			})
			filteredEntries = append(filteredEntries, ent)
		}
	}

	m.unflushedEntries.Add(filteredEntries)

	return unfilteredEntries, lastEntry
}

func (m *Metronome) shouldFlush(raftIndex uint64) bool {
	// If schemes are not initialized flush
	if m == nil || m.schemes == nil || len(m.schemes) == 0 {
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
