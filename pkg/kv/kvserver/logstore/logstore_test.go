// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/print"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestRaftStorageWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const rangeID = roachpb.RangeID(123)
	schemes := [][]roachpb.ReplicaID{
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
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	metronome := InitializeMetronome(1, stopper)
	metronome.SetSchemes(schemes)
	sl := NewStateLoader(rangeID)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	trunc := kvserverpb.RaftTruncatedState{Index: 100, Term: 20}
	state := RaftState{LastIndex: trunc.Index, LastTerm: trunc.Term}
	var output string

	printCommand := func(name, batch string) {
		output += fmt.Sprintf(">> %s\n%s\nState:%+v RaftTruncatedState:%+v\n",
			name, batch, state, trunc)
	}
	printCommand("init", "")

	writeBatch := func(prepare func(rw storage.ReadWriter)) string {
		t.Helper()
		batch := eng.NewBatch()
		defer batch.Close()
		prepare(batch)
		wb := kvserverpb.WriteBatch{Data: batch.Repr()}
		str, err := print.DecodeWriteBatch(&wb)
		require.NoError(t, err)
		require.NoError(t, batch.Commit(true))
		return str
	}
	stats := func() int64 {
		t.Helper()
		prefix := keys.RaftLogPrefix(rangeID)
		prefixEnd := prefix.PrefixEnd()
		ms, err := storage.ComputeStats(ctx, eng, prefix, prefixEnd, 0 /* nowNanos */)
		require.NoError(t, err)
		return ms.SysBytes
	}

	write := func(name string, hs raftpb.HardState, entries []raftpb.Entry) {
		t.Helper()
		var newState RaftState
		batch := writeBatch(func(rw storage.ReadWriter) {
			require.NoError(t, StoreHardState(ctx, rw, sl, hs))
			var err error
			entriesToFlush, lastEntry := metronome.FilterEntries(ctx, entries, func(ent raftpb.Entry) {})
			newState, err = logAppend(ctx, sl.RaftLogPrefix(), rw, state, lastEntry, entriesToFlush)
			require.NoError(t, err)
		})
		state = newState
		require.Equal(t, stats(), state.ByteSize)
		printCommand(name, batch)
	}
	truncate := func(name string, ts kvserverpb.RaftTruncatedState) {
		t.Helper()
		batch := writeBatch(func(rw storage.ReadWriter) {
			require.NoError(t, Compact(ctx, trunc, ts, sl, rw, metronome))
		})
		trunc = ts
		state.ByteSize = stats()
		printCommand(name, batch)
	}

	write("append (100,103]", raftpb.HardState{
		Term: 21, Vote: 3, Commit: 100, Lead: 3, LeadEpoch: 5,
	}, []raftpb.Entry{
		{Index: 101, Term: 20},
		{Index: 102, Term: 21},
		{Index: 103, Term: 21},
	})
	write("append (101,102] with overlap", raftpb.HardState{
		Term: 22, Commit: 100,
	}, []raftpb.Entry{
		{Index: 102, Term: 22},
	})
	write("append (102,105]", raftpb.HardState{}, []raftpb.Entry{
		{Index: 103, Term: 22},
		{Index: 104, Term: 22},
		{Index: 105, Term: 22},
	})
	truncate("truncate at 103", kvserverpb.RaftTruncatedState{Index: 103, Term: 22})
	truncate("truncate all", kvserverpb.RaftTruncatedState{Index: 105, Term: 22})

	// TODO(pav-kv): print the engine content as well.

	output = strings.ReplaceAll(output, "\n\n", "\n")
	output = strings.ReplaceAll(output, "\n\n", "\n")
	echotest.Require(t, output, filepath.Join("testdata", t.Name()+".txt"))
}

func ents(inds ...uint64) []raftpb.Entry {
	sl := make([]raftpb.Entry, 0, len(inds))
	for _, ind := range inds {
		cmd := kvserverpb.RaftCommand{
			MaxLeaseIndex: kvpb.LeaseAppliedIndex(ind), // just to have something nontrivial in here
		}
		b, err := protoutil.Marshal(&cmd)
		if err != nil {
			panic(err)
		}

		cmdID := kvserverbase.CmdIDKey(fmt.Sprintf("%8d", ind%100000000))

		var data []byte
		typ := raftpb.EntryType(ind % 3)
		switch typ {
		case raftpb.EntryNormal:
			enc := raftlog.EntryEncodingStandardWithAC
			if ind%2 == 0 {
				enc = raftlog.EntryEncodingSideloadedWithAC
			}
			data = raftlog.EncodeCommandBytes(enc, cmdID, b, 0 /* pri */)
		case raftpb.EntryConfChangeV2:
			c := kvserverpb.ConfChangeContext{
				CommandID: string(cmdID),
				Payload:   b,
			}
			ccContext, err := protoutil.Marshal(&c)
			if err != nil {
				panic(err)
			}

			var cc raftpb.ConfChangeV2
			cc.Context = ccContext
			data, err = protoutil.Marshal(&cc)
			if err != nil {
				panic(err)
			}
		case raftpb.EntryConfChange:
			c := kvserverpb.ConfChangeContext{
				CommandID: string(cmdID),
				Payload:   b,
			}
			ccContext, err := protoutil.Marshal(&c)
			if err != nil {
				panic(err)
			}
			var cc raftpb.ConfChange
			cc.Context = ccContext
			data, err = protoutil.Marshal(&cc)
			if err != nil {
				panic(err)
			}
		default:
			panic(typ)
		}
		sl = append(sl, raftpb.Entry{
			Term:  100 + ind, // overflow ok
			Index: ind,
			Type:  typ,
			Data:  data,
		})
	}
	return sl
}

func TestRaftStorageLoad(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const rangeID = roachpb.RangeID(123)
	schemes := [][]roachpb.ReplicaID{
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
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	m := InitializeMetronome(roachpb.ReplicaID(2), stopper)
	m.SetSchemes(schemes)
	sl := NewStateLoader(rangeID)
	entryCache := raftentry.NewCache(2048)
	eng := storage.NewDefaultInMemForTesting()
	sideloaded := newTestingSideloadStorage(eng)
	batch := eng.NewWriteBatch()
	defer eng.Close()

	entries := ents(1, 2, 3, 4, 5)

	filteredEntries, _ := m.FilterEntries(ctx, entries, func(ent raftpb.Entry) {})
	raftLogPrefix := sl.RaftLogPrefix()
	for _, ent := range filteredEntries {
		e, err := raftlog.NewEntry(ent)
		if err != nil {
			t.Fatalf("Error while populating new entry %s\n", err.Error())
		}
		metaB, err := e.ToRawBytes()
		if err != nil {
			t.Fatalf("Error while converting new entry to bytes\n")
		}

		key := keys.RaftLogKeyFromPrefix(raftLogPrefix, kvpb.RaftIndex(ent.Index))
		if err != nil {
			t.Fatalf("Error while creating key\n")
		}
		if err := eng.PutUnversioned(key, metaB); err != nil {
			t.Fatalf("Error while putting value: %s\n", err.Error())
		}
	}

	if err := batch.Commit(true); err != nil {
		t.Fatalf("Error while writing batch %s\n", err.Error())
	}

	ents, _, _, err := LoadEntries(ctx, sl, eng, rangeID, entryCache, sideloaded, kvpb.RaftIndex(1), kvpb.RaftIndex(6), uint64(math.MaxUint64), &BytesAccount{}, m)
	if err != nil {
		t.Fatalf("Failed to load entries with %s\n", err.Error())
	}

	for i, ent := range ents {
		expected := string(entries[i].Data)
		actual := string(ent.Data)
		if expected != actual {
			t.Fatalf("Data not equal expected %s got %s\n", expected, actual)
		}
	}
}
