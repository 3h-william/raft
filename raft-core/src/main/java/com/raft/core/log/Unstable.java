package com.raft.core.log;

import org.apache.commons.lang3.ArrayUtils;
import raftpb.Raft.*;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//  ../raft/log_unstable.go
public class Unstable {

    public static Logger logger = LoggerFactory.getLogger(Unstable.class);

    private Snapshot snapshot;
    private Entry[] entries = new Entry[0];
    private int offset;

    public Unstable() {
    }


    public Snapshot getSnapshot() {
        return snapshot;
    }

    public void setSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
    }

    public Entry[] getEntries() {
        return entries;
    }

    public void setEntries(Entry[] entries) {
        this.entries = entries;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }


    /**
     * @return </br>
     * returns the index of the first possible entry in entries , if it has a snapshot.
     */
    public Integer maybeFirstIndex() {
        if (null != this.snapshot) {
            return this.snapshot.getMetadata().getIndex() + 1;
        }
        return 0;
    }


    /**
     * maybeLastIndex returns the last index if it has at least one unstable entry or snapshot.
     *
     * @return
     */
    public Integer maybeLastIndex() {

        if (null != this.entries && this.entries.length != 0) {
            return this.offset + this.entries.length - 1;
        }

        if (null != this.snapshot) {
            return this.snapshot.getMetadata().getIndex();
        }
        return 0;
    }

    /**
     * maybeTerm returns the term of the entry at index i, if there is any.
     *
     * @param i
     * @return
     */
    public Integer maybeTerm(int i) {

        if (i < this.offset) {
            if (null == this.snapshot) {
                return 0;
            }

            if (this.snapshot.getMetadata().getIndex() == i) {
                return this.snapshot.getMetadata().getTerm();
            }

            return 0;
        }

        Integer last = maybeLastIndex();
        if (0 == last) {
            return 0;
        }

        if (i > last) {
            return 0;
        }

        return this.entries[i - this.offset].getTerm();
    }

    public void stableTo(Integer i, Integer t) {
        Integer gt = maybeTerm(i);

        if (0 == gt) {
            return;
        }

        // if i < offset, term is matched with the snapshot
        // only update the unstable entries if term is matched with
        // an unstable entry.
        if (gt == t && i >= this.offset) {
            int startPos = i + 1 - this.offset;
            int copyLength = this.entries.length - startPos ;
            Entry[] newEntry = new Entry[copyLength];
            System.arraycopy(this.entries, i + 1 - this.offset, newEntry, 0, copyLength);
            this.entries = newEntry;
            this.offset = i + 1;
            // shrinkEntriesArray  not do now TODO
        }
    }

    public void stableSnapTo(int i) {
        if (null != this.snapshot && i == this.snapshot.getMetadata().getIndex()) {
            this.snapshot = null;
        }
    }

    public void restore(Snapshot s) {
        this.offset = s.getMetadata().getIndex() + 1;
        this.entries = null;
        this.snapshot = s;
    }

    public void truncateAndAppend(Entry[] ents) {
        int after = ents[0].getIndex();

        if (after == this.offset + this.entries.length) {
            // after is the next index in the u.entries
            // directly append
            this.entries = ArrayUtils.addAll(this.entries, ents);
        } else if (after <= this.offset) {
            logger.info(String.format("replace the unstable entries from index %d", after));
            // The log is being truncated to before our current offset
            // portion, so set the offset and replace the entries
            this.offset = after;
            this.entries = ents;
        } else {
            // TODO 验证
            // default
            // truncate to after and copy to u.entries
            // then append
            logger.info(String.format("truncate the unstable entries before index %d", after));
            this.entries = slice(this.offset, after);
            this.entries = ArrayUtils.addAll(this.entries, ents);
        }
    }

    public Entry[] slice(int lo, int hi) {
        mustCheckOutOfBounds(lo, hi);
        return ArrayUtils.subarray(this.entries, lo - this.offset, hi - this.offset);
    }

    /**
     * u.offset <= lo <= hi <= u.offset+len(u.offset)
     *
     * @param lo
     * @param hi
     */
    public void mustCheckOutOfBounds(int lo, int hi) {
        if (lo > hi) {
            throw new RuntimeException(String.format("invalid unstable.slice %d > %d", lo, hi));
        }

        int upper = this.offset + this.entries.length;

        if (lo < this.offset || hi > upper) {
            throw new RuntimeException(String.format("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, this.offset, upper));
        }
    }


    public static void main(String[] args) {


        logger.info("aaa %d", 10);

        int[] x = {1, 2, 3, 4};
        int[] y = {5, 6};

        ;

        int[] newarray = ArrayUtils.addAll(x, y);

        System.out.println(Arrays.toString(ArrayUtils.subarray(x, 2, 3)));

    }
}