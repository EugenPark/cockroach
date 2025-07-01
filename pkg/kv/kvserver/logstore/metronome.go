package logstore

import (
	"context"
	"fmt"
	"math"

	"math/rand"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Timeout Logic
type timeout chan struct{}

type timeoutQueue struct {
	queue   map[raftpb.Index]timeout
	stopper *stop.Stopper
}

func newTimeoutQueue(stopper *stop.Stopper) timeoutQueue {
	return timeoutQueue{
		queue:   make(map[raftpb.Index]timeout),
		stopper: stopper,
	}
}

func (tq *timeoutQueue) addTimeout(ctx context.Context, index raftpb.Index, duration time.Duration, onTimeout func()) {
	timer := time.NewTimer(duration)
	timeout := make(chan struct{})
	tq.queue[index] = timeout

	err := tq.stopper.RunAsyncTask(ctx, "metronome-timeout", func(ctx context.Context) {
		select {
		case <-timer.C:
			onTimeout()
		case <-timeout:
			if !timer.Stop() {
				<-timer.C // Drain to prevent goroutine leak
			}
		case <-tq.stopper.ShouldQuiesce():
			// log.Info(ctx, "Stopping the timeout...")
			if !timer.Stop() {
				<-timer.C
			}
		case <-ctx.Done():
			// log.Info(ctx, "Context is done...")
			if !timer.Stop() {
				<-timer.C
			}
		}
	})
	if err != nil {
		log.Warningf(ctx, "Error while running timeout: %s\n", err.Error())
	}
}

func (tq *timeoutQueue) cancelTimeout(index raftpb.Index) {
	// This was an index which was flushed so no need to cancel anything
	tout, ok := tq.queue[index]
	if !ok {
		return
	}

	close(tout)
	delete(tq.queue, index)
}

// RaftLogMap Logic for allowing logs with gaps
type RaftLogMap struct {
	sync.Mutex
	entries []raftpb.Entry
}

func NewRaftLogMap() RaftLogMap {
	return RaftLogMap{
		entries: make([]raftpb.Entry, 0),
	}
}

// TODO: Maybe add a more performant add for the case that entries might be duplicated

// TODO: Write a test for this
func (rlm *RaftLogMap) Sort() {
	slices.SortFunc(rlm.entries, func(a, b raftpb.Entry) int {
		return int(a.Index) - int(b.Index)
	})
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

func (rlm *RaftLogMap) Get(index uint64) (raftpb.Entry, bool) {
	for i, ent := range rlm.entries {
		if ent.Index == index {
			return rlm.entries[i], true
		}
	}
	return raftpb.Entry{}, false
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

	index := min(rlm.entries[0].Index, otherEntries[0].Index)
	maxIndex := max(rlm.entries[len(rlm.entries)-1].Index, otherEntries[len(otherEntries)-1].Index)

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

func InitializeMetronome(replicaID roachpb.ReplicaID, stopper *stop.Stopper) *Metronome {
	m := Metronome{
		replicaID:        replicaID,
		inflightQueue:    newTimeoutQueue(stopper),
		unflushedEntries: NewRaftLogMap(),
	}

	return &m
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

func (m *Metronome) GetMissingIndices(lo, hi, commit uint64, flushedIndices []uint64) []uint64 {
	var missingIndices []uint64

	// Discover higher bounds
	higherBound := hi
	for higherBound < math.MaxUint64 {
		if m.shouldFlush(higherBound + 1) {
			break
		}

		higherBound++
	}

	higherBound = max(higherBound, commit)

	// Iterate from lowerBound to higherBound and find the missing entries
	for i := lo; i <= higherBound; i++ {
		if slices.Contains(flushedIndices, i) {
			continue
		}

		missingIndices = append(missingIndices, i)
	}

	return missingIndices
}

func (m *Metronome) ShouldRebalance(otherScheme []roachpb.ReplicaID) bool {
	if m == nil {
		return false
	}

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

func (m *Metronome) FilterEntries(ctx context.Context, entries []raftpb.Entry, cb func(ent raftpb.Entry)) ([]raftpb.Entry, raftpb.Entry) {
	// log.Infof(ctx, "Filtering Entries\n")
	// TODO: Why does this not work
	min := 50000  // milliseconds
	max := 150000 // milliseconds
	randomMs := rand.Intn(max-min+1) + min
	duration := time.Duration(randomMs) * time.Millisecond

	unfilteredEntries := make([]raftpb.Entry, 0, len(entries))
	filteredEntries := make([]raftpb.Entry, 0, len(entries))
	lastEntry := entries[len(entries)-1]

	// TODO: do not hold lock for that long
	m.unflushedEntries.Lock()
	defer m.unflushedEntries.Unlock()
	for i := range entries {
		copyData := make([]byte, len(entries[i].Data))
		copy(copyData, entries[i].Data)
		ent := entries[i]
		ent.Data = copyData

		shouldFlush := m.shouldFlush(ent.Index)

		if shouldFlush {
			unfilteredEntries = append(unfilteredEntries, ent)
		} else {
			m.inflightQueue.addTimeout(ctx, raftpb.Index(ent.Index), duration, func() {
				cb(ent)
				m.unflushedEntries.Lock()
				defer m.unflushedEntries.Unlock()
				m.unflushedEntries.Remove(ent.Index)
			})
			filteredEntries = append(filteredEntries, ent)
		}
	}

	m.unflushedEntries.Add(filteredEntries)

	return unfilteredEntries, lastEntry
}

func (m *Metronome) GetEntry(index uint64) (raftpb.Entry, bool) {
	m.unflushedEntries.Lock()
	defer m.unflushedEntries.Unlock()

	return m.unflushedEntries.Get(index)
}

func (m *Metronome) GetLastEntry() (raftpb.Entry, bool) {
	m.unflushedEntries.Lock()
	defer m.unflushedEntries.Unlock()

	return m.unflushedEntries.GetLast()
}

func (m *Metronome) AppendEntries(entries []raftpb.Entry) {
	m.unflushedEntries.Lock()
	defer m.unflushedEntries.Unlock()

	m.unflushedEntries.Add(entries)
}

func (m *Metronome) AddRecoveredEntries(entries []raftpb.Entry) {
	m.unflushedEntries.Lock()
	defer m.unflushedEntries.Unlock()

	m.unflushedEntries.Add(entries)
	m.unflushedEntries.Sort()
}

func (m *Metronome) CompactEntries(index uint64) {
	m.unflushedEntries.Lock()
	defer m.unflushedEntries.Unlock()

	m.unflushedEntries.Compact(index)
}

func (m *Metronome) GetEntries(lo, hi uint64) []raftpb.Entry {
	m.unflushedEntries.Lock()
	defer m.unflushedEntries.Unlock()

	log := m.unflushedEntries.GetLog(lo, hi)
	copied := make([]raftpb.Entry, len(log))
	copy(copied, log)

	return copied
}

func (m *Metronome) ClearEntries() {
	m.unflushedEntries.Lock()
	defer m.unflushedEntries.Unlock()

	m.unflushedEntries.entries = make([]raftpb.Entry, 0)
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
	for i := range quorums {
		slices.Sort(quorums[i])
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
