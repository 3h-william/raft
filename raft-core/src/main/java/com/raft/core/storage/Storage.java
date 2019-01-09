package com.raft.core.storage;

import raftpb.Raft.*;

/**
 * Created by william on 2018/11/16.
 */

// ../raft/storage.go

// Storage is an interface that may be implemented by the application
// to retrieve log entries from storage.
//
// If any Storage method returns an error, the raft instance will
// become inoperable and refuse to participate in elections; the
// application is responsible for cleanup and recovery in this case.
public interface Storage {

    // InitialState returns the saved HardState and ConfState information.
    StorageState initialState();

    // Entries returns a slice of log entries in the range [lo,hi).
    // MaxSize limits the total size of the log entries returned, but
    // Entries returns at least one entry if any.
    Entry[] entries(Integer lo , Integer hi , Long masSize) throws Throwable;

    // Term returns the term of entry i, which must be in the range
    // [FirstIndex()-1, LastIndex()]. The term of the entry before
    // FirstIndex is retained for matching purposes even though the
    // rest of that entry may not be available.
    Integer term(Integer i) throws Exception;

    // LastIndex returns the index of the last entry in the log.
    Integer lastIndex();

    // FirstIndex returns the index of the first log entry that is
    // possibly available via Entries (older entries have been incorporated
    // into the latest Snapshot; if storage only contains the dummy entry the
    // first log entry is not available).
    Integer firstIndex();

    // Snapshot returns the most recent snapshot.
    // If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
    // so raft state machine could know that Storage needs some time to prepare
    // snapshot and call Snapshot later.
    Snapshot snapshot();


}
