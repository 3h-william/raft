package com.raft.core.storage;

import com.raft.core.ExceptionConstants;
import com.raft.core.utils.RaftUtils;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.ArrayUtils;
import raftpb.Raft.*;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by william on 2018/11/27.
 */


//  ../raft/storage.go
// MemoryStorage implements the Storage interface backed by an
// in-memory array.
public class MemoryStorage implements Storage {

    // Protects access to all fields. Most methods of MemoryStorage are
    // run on the raft goroutine, but Append() is run on an application
    // goroutine.
    //sync.Mutex

    private Lock lock = new ReentrantLock();

    private HardState hardState;
    private Snapshot snapshot;

    // ents[i] has raft log position i+snapshot.Metadata.Index
    private Entry[] ents;


    public MemoryStorage() {
        // When starting from scratch populate the list with a dummy entry at term zero.
        // TODO java语言优化
        ents = new Entry[]{Entry.newBuilder().build()};
        snapshot = Snapshot.newBuilder().build();
        hardState = HardState.newBuilder().build();

    }

    // InitialState implements the Storage interface.
    @Override
    public StorageState initialState() {
        return new StorageState(this.hardState, this.snapshot.getMetadata().getConfState());
    }

    public void setHardState(HardState st) {
        try {
            lock.lock();
            this.hardState = st;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Entry[] entries(Integer lo, Integer hi, Long maxSize) throws Throwable {

        try {
            lock.lock();
            Integer offset = ents[0].getIndex();

            if (lo < offset) {
                throw ExceptionConstants.ErrUnavailable;
            }

            if (hi > lastIndex() + 1) {
                throw new RuntimeException(String.format("entries' hi(%d) is out of bound lastindex(%d)", hi, internalLastIndex()));
            }

            // only contains dummy entries.
            if (ents.length == 1) {
                throw ExceptionConstants.ErrUnavailable;
            }

            Entry[] ents = ArrayUtils.subarray(this.ents, lo - offset, hi - offset);
            return RaftUtils.limitSize(ents, maxSize);

        } catch (Throwable t) {
            throw t;
        } finally {
            lock.unlock();
        }
    }

    // Term implements the Storage interface.
    @Override
    public Integer term(Integer i) throws Exception {
        try {
            lock.lock();
            Integer offset = ents[0].getIndex();
            if (i < offset) {
                throw ExceptionConstants.ErrCompacted;
            }
            if ((i - offset) >= ents.length) {
                throw ExceptionConstants.ErrUnavailable;
            }
            return ents[i - offset].getTerm();
        } catch (Throwable t) {
            throw t;
        } finally {
            lock.unlock();
        }
    }

    // LastIndex implements the Storage interface.
    @Override
    public Integer lastIndex() {
        try {
            lock.lock();
            return internalLastIndex();
        } finally {
            lock.unlock();
        }
    }

    // 内部使用的lastIndex,TODO 并发问题
    public Integer internalLastIndex() {
        return ents[0].getIndex() + ents.length - 1;
    }

    // FirstIndex implements the Storage interface.
    @Override
    public Integer firstIndex() {
        try {
            lock.lock();
            return internalFirstIndex();
        } finally {
            lock.unlock();
        }
    }

    public Integer internalFirstIndex() {
        return ents[0].getIndex() + 1;
    }

    // Snapshot implements the Storage interface.
    @Override
    public Snapshot snapshot() {
        try {
            lock.lock();
            return snapshot;
        } finally {
            lock.unlock();
        }
    }

    // ApplySnapshot overwrites the contents of this Storage object with
    // those of the given snapshot.
    public void applySnapshot(Snapshot snap) throws Throwable {

        try {
            lock.lock();

            //handle check for old snapshot being applied
            Integer msIndex = snapshot.getMetadata().getIndex();
            Integer snapIndex = snap.getMetadata().getIndex();

            if (msIndex >= snapIndex) {
                throw ExceptionConstants.ErrSnapOutOfDate;
            }

            this.snapshot = snap;
            ents = new Entry[]{Entry.newBuilder().setIndex(snap.getMetadata().getIndex()).setTerm(snap.getMetadata().getTerm()).build()};

        } catch (Throwable t) {
            throw t;
        } finally {
            lock.unlock();
        }
    }

    // CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
    // can be used to reconstruct the state at that point.
    // If any configuration changes have been made since the last compaction,
    // the result of the last ApplyConfChange must be passed in.
    public Snapshot createSnapshot(Integer i, ConfState cs, byte[] data) throws Throwable {

        try {
            lock.lock();
            if (i <= snapshot.getMetadata().getIndex()) {
                throw ExceptionConstants.ErrSnapOutOfDate;
            }

            Integer offset = ents[0].getIndex();
            if (i > lastIndex()) {
                throw new RuntimeException(String.format("snapshot %d is out of bound lastindex(%d)", i, internalLastIndex()));
            }

            // TODO 验证， 重新new新的snapshot
            SnapshotMetadata.Builder snapshotBuilder = SnapshotMetadata.newBuilder();
            snapshotBuilder.setIndex(i);
            snapshotBuilder.setTerm(ents[i - offset].getTerm());
            if (null != cs) {
                snapshotBuilder.setConfState(cs);
            }

            Snapshot newSnapshot = Snapshot.newBuilder().setMetadata(snapshotBuilder.build()).setData(ByteString.copyFrom(data)).build();
            return newSnapshot;

        } catch (Throwable t) {
            throw t;
        } finally {
            lock.unlock();
        }
    }

    // Compact discards all log entries prior to compactIndex.
    // It is the application's responsibility to not attempt to compact an index
    // greater than raftLog.applied.
    public void compact(Integer compactIndex) throws Throwable {
        try {
            lock.lock();
            Integer offset = ents[0].getIndex();
            if (compactIndex > internalLastIndex()) {
                throw new RuntimeException(String.format("compact %d is out of bound lastindex(%d)", compactIndex, internalLastIndex()));
            }

            Integer i = compactIndex - offset;
            // 逻辑翻译代码，需要确认准确性 TODO
            ents = ArrayUtils.subarray(ents, i, ents.length);
        } catch (Throwable t) {
            throw t;
        } finally {
            lock.unlock();
        }
    }

    // Append the new entries to storage.
    // TODO (xiangli): ensure the entries are continuous and
    // entries[0].Index > ms.entries[0].Index
    public void append(Entry[] entries) throws Throwable {

        if (entries.length == 0) {
            return;
        }
        try {
            lock.lock();

            Integer first = firstIndex();
            Integer last = entries[0].getIndex() + entries.length - 1;

            // shortcut if there is no new entry.
            if (last < first) {
                return;
            }

            // truncate compacted entries
            if (first > entries[0].getIndex()) {
                entries = ArrayUtils.subarray(entries, first - entries[0].getIndex(), entries.length);
            }

            Integer offset = entries[0].getIndex() - ents[0].getIndex();

            if (ents.length > offset) {
                ents = ArrayUtils.subarray(ents, 0, offset);
                ents = ArrayUtils.addAll(ents, entries);
            } else if (ents.length == offset) {
                ents = ArrayUtils.addAll(ents, entries);
            } else {
                throw new RuntimeException(String.format("missing log entry [last: %d, append at: %d]", internalLastIndex(), entries[0].getIndex()));
            }

        } catch (Throwable t) {
            throw t;
        } finally {
            lock.unlock();
        }

    }
}
