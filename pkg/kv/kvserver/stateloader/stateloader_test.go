// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stateloader

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

func TestUninitializedReplicaState(t *testing.T) {
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	desc := roachpb.RangeDescriptor{RangeID: 123}
	exp, err := Make(desc.RangeID).Load(context.Background(), eng, &desc)
	require.NoError(t, err)
	act := UninitializedReplicaState(desc.RangeID)
	require.Equal(t, exp, act)
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

func TestLoadLastEntryID(t *testing.T) {
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	batch := eng.NewBatch()
	defer batch.Close()
	reader := eng.NewReader(storage.StandardDurability)
	defer reader.Close()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	desc := roachpb.RangeDescriptor{RangeID: 123}
	metronome := logstore.InitializeMetronome(1, stopper)
	sl := Make(desc.RangeID)

	entries := ents(1, 2, 3, 4, 5)

	raftLogPrefix := sl.RaftLogPrefix()
	for _, ent := range entries {
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

	lastEntryID, err := sl.LoadLastEntryID(context.Background(), reader, kvserverpb.RaftTruncatedState{}, metronome)
	require.NoError(t, err)

	require.Equal(t, logstore.EntryID{
		Index: 5,
		Term:  105,
	}, lastEntryID)

	metronome.AppendEntries([]raftpb.Entry{{
		Term:  106,
		Index: 6,
	}})

	lastEntryID, err = sl.LoadLastEntryID(context.Background(), reader, kvserverpb.RaftTruncatedState{}, metronome)
	require.NoError(t, err)

	require.Equal(t, logstore.EntryID{
		Index: 6,
		Term:  106,
	}, lastEntryID)
}
