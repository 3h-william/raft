package com.raft.core.log;

import com.raft.core.RaftConstants;
import com.raft.core.ExceptionConstants;
import com.raft.core.storage.Storage;
import com.raft.core.utils.RaftUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raftpb.Raft.*;

import static com.raft.core.RaftConstants.NO_LIMIT;
import static java.util.Objects.*;

/**
 * Created by william on 2018/11/23.
 */

// ../raft/log.go
public class RaftLog {

    public static Logger logger = LoggerFactory.getLogger(Unstable.class);

    // storage contains all stable entries since the last snapshot.
    private Storage storage;

    // unstable contains all unstable entries and snapshot.
    // they will be saved into storage.
    private Unstable unstable;

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    private int committed;

    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    private int applied;

    public RaftLog(Storage storage, Unstable unstable, int committed, int applied) {
        this.storage = storage;
        this.unstable = unstable;
        this.committed = committed;
        this.applied = applied;
    }

    public static Logger getLogger() {
        return logger;
    }

    public static void setLogger(Logger logger) {
        RaftLog.logger = logger;
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public Unstable getUnstable() {
        return unstable;
    }

    public void setUnstable(Unstable unstable) {
        this.unstable = unstable;
    }

    public int getCommitted() {
        return committed;
    }

    public void setCommitted(int committed) {
        this.committed = committed;
    }

    public int getApplied() {
        return applied;
    }

    public void setApplied(int applied) {
        this.applied = applied;
    }

    /**
     * newLog returns log using the given storage. It recovers the log to the state
     * that it just commits and applies the latest snapshot.
     *
     * @param storage
     * @return
     */
    public static RaftLog newLog(Storage storage) {

        if (null == storage) {
            throw new RuntimeException("storage must not be nil");
        }

        RaftLog log = new RaftLog(storage, new Unstable(), 0, 0);

        Integer firstIndex = storage.firstIndex();
        Integer lastIndex = storage.lastIndex();

        // TODO , 0 or null present invalid value
        requireNonNull(firstIndex);
        requireNonNull(lastIndex);

        log.unstable.setOffset(lastIndex + 1);

        // Initialize our committed and applied pointers to the time of the last compaction.
        log.committed = firstIndex - 1;
        log.applied = firstIndex - 1;

        return log;
    }


    @Override
    public String toString() {
        return String.format("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d",
                this.committed,
                this.applied,
                this.unstable.getOffset(),
                this.unstable.getEntries().length
        );
    }


    // maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
    // it returns (last index of new entries, true).
    public AppendResult maybeAppend(int index, int logTerm, int committed, Entry[] ents) throws Throwable {
        if (matchTerm(index, logTerm)) {
            int lastnewi = index + ents.length;
            Integer ci = findConflict(ents);

            if (0 == ci) {
                // empty
            } else if (ci <= this.committed) {
                throw new RuntimeException(String.format("entry %d conflict with committed entry [committed(%d)]", ci, this.committed));
            }
            // default
            else {
                int offset = index + 1;
                append(ArrayUtils.subarray(ents, ci - offset, ents.length));
            }
            commitTo(Math.min(committed, lastnewi));
            return new AppendResult(lastnewi, true);
        }
        return new AppendResult(0, false);
    }

    public class AppendResult {
        public int lastnewi;
        public boolean isOk;

        public AppendResult(int lastnewi, boolean isOk) {
            this.lastnewi = lastnewi;
            this.isOk = isOk;
        }
    }

    public Integer append(Entry[] ents) {

        if (ents.length == 0) {
            return this.lastIndex();
        }

        int after = ents[0].getIndex() - 1;

        if (after < this.committed) {
            throw new RuntimeException(String.format("after(%d) is out of range [committed(%d)]", after, this.committed));
        }

        this.unstable.truncateAndAppend(ents);
        return this.lastIndex();
    }


    public Entry[] unstableEntries() {
        if (this.unstable.getEntries().length == 0) {
            return null;
        }
        return this.unstable.getEntries();
    }


    // findConflict finds the index of the conflict.
    // It returns the first pair of conflicting entries between the existing
    // entries and the given entries, if there are any.
    // If there is no conflicting entries, and the existing entries contains
    // all the given entries, zero will be returned.
    // If there is no conflicting entries, but the given entries contains new
    // entries, the index of the first new entry will be returned.
    // An entry is considered to be conflicting if it has the same index but
    // a different term.
    // The first entry MUST have an index equal to the argument 'from'.
    // The index of the given entries MUST be continuously increasing.
    public Integer findConflict(Entry[] ents) throws Throwable {
        for (Entry ne : ents) {
            if (!matchTerm(ne.getIndex(), ne.getTerm())) {
                if (ne.getIndex() <= this.lastIndex()) {
                    logger.info(String.format("found conflict at index %s [existing term: %s, conflicting term: %s]",
                            ne.getIndex(), term(ne.getIndex()), ne.getTerm()));

                }
                return ne.getIndex();

            }
        }
        return 0;
    }


    // nextEnts returns all the available entries for execution.
    // If applied is smaller than the index of snapshot, it returns all committed
    // entries after the index of snapshot.

    /**
     * 返回所有可以执行的entries
     * 如果applied 比 snapshot的index小，返回所有在snapshot index之后committed的entries
     *
     * @return
     */
    public Entry[] nextEnts() throws Throwable {

        Integer off = Math.max(this.applied + 1, firstIndex());
        if (this.committed + 1 > off) {
            return slice(off, this.committed + 1, NO_LIMIT);
        }
        return null;
    }

    /**
     * 关联 raftLog.nextEnts()
     * 快速的 check方法
     *
     * @return
     */
    public boolean hasNextEnts() {
        Integer off = Math.max(this.applied + 1, firstIndex());
        return this.committed + 1 > off;
    }


    public Snapshot snapshot() {
        if (null != this.unstable.getSnapshot()) {
            return this.unstable.getSnapshot();
        }

        return this.storage.snapshot();
    }

    public Integer firstIndex() {

        Integer unstableFirstIndex = this.unstable.maybeFirstIndex();
        if (0 != unstableFirstIndex) {
            return unstableFirstIndex;
        }

        Integer storageFirstIndex = this.storage.firstIndex();
        if (0 != storageFirstIndex) {
            return storageFirstIndex;
        }

        throw new RuntimeException("get firstIndex error");
    }

    public int lastIndex() {
        Integer unstableLastIndex = this.unstable.maybeLastIndex();
        if (0 != unstableLastIndex) {
            return unstableLastIndex;
        }
        Integer storageLastIndex = this.storage.lastIndex();
        return storageLastIndex;
    }


    public void commitTo(int tocommit) {

        if (this.committed < tocommit) {
            if (lastIndex() < tocommit) {
                throw new RuntimeException(String.format("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, lastIndex()));
            }
            // set
            this.committed = tocommit;
        }
    }


    public void appliedTo(int i) {
        if (i == 0) return;

        if (this.committed < i || i < this.applied) {
            throw new RuntimeException(String.format("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, this.applied, this.committed));
        }
        this.applied = i;
    }

    public void stableTo(int i, int t) {
        this.unstable.stableTo(i, t);
    }

    public void stableSnapTo(int i) {
        this.unstable.stableSnapTo(i);
    }

    public Integer lastTerm() throws Exception {
        return term(lastIndex());
    }

    /**
     * 获取 term
     *
     * @param i
     * @return
     */
    public Integer term(int i) throws Exception {
        // the valid term range is [index of dummy entry, last index]
        int dummyIndex = firstIndex() - 1;

        if (i < dummyIndex || i > lastIndex())
            return 0;

        Integer unstableTerm = this.unstable.maybeTerm(i);
        if (0 != unstableTerm) {
            return unstableTerm;
        }

        Integer storageTerm = this.storage.term(i);
        return storageTerm;
    }


    public Entry[] entries(Integer i, Long maxSize) throws Throwable {
        if (i > lastIndex()) {
            return null;
        }
        return slice(i, lastIndex() + 1, maxSize);
    }


    public Entry[] allEntries() throws Throwable {

        Entry[] ents = entries(firstIndex(), RaftConstants.NO_LIMIT);
        return ents;

        // if err == ErrCompacted { // try again if there was a racing compaction
        // return l.allEntries()
    }

    // isUpToDate determines if the given (lastIndex,term) log is more up-to-date
    // by comparing the index and term of the last entries in the existing logs.
    // If the logs have last entries with different terms, then the log with the
    // later term is more up-to-date. If the logs end with the same term, then
    // whichever log has the larger lastIndex is more up-to-date. If the logs are
    // the same, the given log is up-to-date.

    /**
     * Raft 通过比较日志中最后一个条目的索引和任期号来决定两个日志哪一个更新。如果两个日志的任期号不同，任期号大的更新；如果任期号相同，更长的日志更新。
     * <p>
     * 如果都一样，给定的日志是最新的
     *
     * @param lasti
     * @param term
     * @return
     */
    public boolean isUpToDate(int lasti, int term) throws Exception {

        if (term > lastTerm()) {
            return true; // 任期号大的更新
        }

        if (term == lastTerm()) {
            return lasti >= lastIndex(); // 如果任期号相同，更长的日志更新
        }

        return false;
    }

    public boolean matchTerm(int i, int term) throws Exception {
        Integer t = term(i);
        return t == term;
    }

    public boolean maybeCommit(int maxIndex, int term) throws Exception {
        Integer indexTerm = term(maxIndex); // 修改

        if (maxIndex > this.committed && indexTerm == term) {
            commitTo(maxIndex);
            return true;
        }
        return false;
    }


    public void restore(Snapshot s) {
        logger.info(String.format("log [%s] starts to restore snapshot [index: %d, term: %d]",
                this.toString(), s.getMetadata().getIndex(), s.getMetadata().getTerm()));
        this.committed = s.getMetadata().getIndex();
    }

    public Entry[] slice(Integer lo, Integer hi, Long maxSize) throws Throwable {

        mustCheckOutOfBounds(lo, hi);

        if (lo == hi) {
            return null;
        }


        Entry[] ents = null;

        if (lo < this.unstable.getOffset()) {
            Entry[] storedEnts = this.storage.entries(lo, Math.min(hi, this.unstable.getOffset()), maxSize);

            if (storedEnts.length < Math.min(hi, this.unstable.getOffset()) - lo) {
                return storedEnts;
            }

            ents = storedEnts;

        }

        if (hi > this.unstable.getOffset()) {

            Entry[] unstableEntries = this.unstable.slice(Math.max(lo, this.unstable.getOffset()), hi);
            if (null != ents && ents.length > 0) {
                ents = ArrayUtils.addAll(ents, unstableEntries);
            } else {
                ents = unstableEntries;
            }
        }
        return RaftUtils.limitSize(ents, maxSize);
    }


    /**
     * l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
     *
     * @param lo
     * @param hi
     */
    public void mustCheckOutOfBounds(int lo, int hi) throws Exception {
        if (lo > hi) {
            throw new RuntimeException(String.format("invalid unstable.slice %d > %d", lo, hi));
        }

        int fi = firstIndex();

        if (lo < fi) {
            throw ExceptionConstants.ErrCompacted;
        }

        Integer length = lastIndex() + 1 - fi;

        if (lo < fi || hi > fi + length) {
            throw new RuntimeException(String.format("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, lastIndex()));
        }
    }

}
