package com.raft.core;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Created by william on 2018/12/3.
 */

// progress.go

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
public class Progress {
    // 对于每一个服务器，记录已经复制到该服务器的日志的最高索引值（从 0 开始递增）
    private int match;
    // 对于每一个服务器，记录需要发给它的下一个日志条目的索引（初始化为领导人上一条日志的索引值 +1）
    private int next;


    // State defines how the leader should interact with the follower.
    //
    // When in ProgressStateProbe, leader sends at most one replication message
    // per heartbeat interval. It also probes actual progress of the follower.
    //
    // When in ProgressStateReplicate, leader optimistically increases next
    // to the latest entry sent after sending replication message. This is
    // an optimized state for fast replicating log entries to the follower.
    //
    // When in ProgressStateSnapshot, leader should have sent out snapshot
    // before and stops sending any replication message.
    private RaftConstantType.ProgressStateType state = RaftConstantType.ProgressStateType.ProgressStateProbe; // default

    // Paused is used in ProgressStateProbe.
    // When Paused is true, raft should pause sending replication message to this peer.
    private boolean paused;

    // PendingSnapshot is used in ProgressStateSnapshot.
    // If there is a pending snapshot, the pendingSnapshot will be set to the
    // index of the snapshot. If pendingSnapshot is set, the replication process of
    // this Progress will be paused. raft will not resend snapshot until the pending one
    // is reported to be failed.
    private int pendingSnapshot;

    // RecentActive is true if the progress is recently active. Receiving any messages
    // from the corresponding follower indicates the progress is active.
    // RecentActive can be reset to false after an election timeout.
    private boolean recentActive;


    // inflights is a sliding window for the inflight messages.
    // Each inflight message contains one or more log entries.
    // The max number of entries per message is defined in raft config as MaxSizePerMsg.
    // Thus inflight effectively limits both the number of inflight messages
    // and the bandwidth each Progress can use.
    // When inflights is full, no more message should be sent.
    // When a leader sends out a message, the index of the last
    // entry should be added to inflights. The index MUST be added
    // into inflights in order.
    // When a leader receives a reply, the previous inflights should
    // be freed by calling inflights.freeTo with the index of the last
    // received entry.
    private InFlights ins;

    // IsLearner is true if this progress is tracked for a learner.
    private boolean isLearner;

    public int getMatch() {
        return match;
    }

    public void setMatch(int match) {
        this.match = match;
    }

    public int getNext() {
        return next;
    }

    public void setNext(int next) {
        this.next = next;
    }

    public RaftConstantType.ProgressStateType getState() {
        return state;
    }

    public void setState(RaftConstantType.ProgressStateType state) {
        this.state = state;
    }

    public boolean isPaused() {
        return paused;
    }

    public void setPaused(boolean paused) {
        this.paused = paused;
    }

    public int getPendingSnapshot() {
        return pendingSnapshot;
    }

    public void setPendingSnapshot(int pendingSnapshot) {
        this.pendingSnapshot = pendingSnapshot;
    }

    public boolean isRecentActive() {
        return recentActive;
    }

    public void setRecentActive(boolean recentActive) {
        this.recentActive = recentActive;
    }

    public InFlights getIns() {
        return ins;
    }

    public void setIns(InFlights ins) {
        this.ins = ins;
    }

    public boolean isLearner() {
        return isLearner;
    }

    public void setLearner(boolean learner) {
        isLearner = learner;
    }


    public void resetState(RaftConstantType.ProgressStateType state) {
        this.setPaused(false);
        this.setPendingSnapshot(0);
        this.setState(state);
        this.getIns().reset();
    }

    public void becomeProbe() {
        // If the original state is ProgressStateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if (this.getState() == RaftConstantType.ProgressStateType.ProgressStateSnapshot) {
            Integer pendingSnapshot = this.getPendingSnapshot();
            resetState(RaftConstantType.ProgressStateType.ProgressStateProbe);
            this.setNext(Math.max(this.getMatch() + 1, pendingSnapshot + 1));
        } else {
            resetState(RaftConstantType.ProgressStateType.ProgressStateProbe);
            this.setNext(this.getMatch() + 1);
        }
    }

    public void becomeReplicate() {
        resetState(RaftConstantType.ProgressStateType.ProgressStateReplicate);
        this.setNext(this.getMatch() + 1);
    }

    // maybeUpdate returns false if the given n index comes from an outdated message.
    // Otherwise it updates the progress and returns true.
    public boolean maybeUpdate(int n) {
        boolean updated = false;
        if (this.getMatch() < n) {
            this.setMatch(n);
            updated = true;
            resume();
        }

        if (this.getNext() < n + 1) {
            this.setNext(n + 1);
        }
        return updated;
    }

    public void becomeSnapshot(Integer snapshoti) {
        resetState(RaftConstantType.ProgressStateType.ProgressStateSnapshot);
        this.pendingSnapshot = snapshoti;
    }

    public void optimisticUpdate(Integer n) {
        this.setNext(n + 1);
    }

    // maybeDecrTo returns false if the given to index comes from an out of order message.
    // Otherwise it decreases the progress next index to min(rejected, last) and returns true.
    public boolean maybeDecrTo(int rejected, int last) {
        // the rejection must be stale if the progress has matched and "rejected"
        // is smaller than "match".
        if (this.getState() == RaftConstantType.ProgressStateType.ProgressStateReplicate) {
            // the rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            if (rejected <= this.getMatch()) {
                return false;
            }
            // directly decrease next to match + 1
            this.setNext(this.getMatch() + 1);
            return true;
        }

        // the rejection must be stale if "rejected" does not match next - 1
        if (this.getNext() - 1 != rejected) {
            return false;
        }

        // TODO 优化逻辑
        this.setNext(Math.min(rejected, last + 1));
        if (this.getNext() < 1) {
            this.setNext(1);
        }
        resume();
        return true;
    }

    public void pause() {
        this.setPaused(true);
    }

    public void resume() {
        this.setPaused(false);
    }

    // IsPaused returns whether sending log entries to this node has been
    // paused. A node may be paused because it has rejected recent
    // MsgApps, is currently waiting for a snapshot, or has reached the
    // MaxInflightMsgs limit.
    public boolean isNodePaused() {
        switch (this.getState()) {
            case ProgressStateProbe:
                return this.isPaused();
            case ProgressStateReplicate:
                return ins.full();
            case ProgressStateSnapshot:
                return true;
            default:
                throw new RuntimeException("unexpected state");
        }
    }

    public void snapshotFailure() {
        this.setPendingSnapshot(0);
    }

    // needSnapshotAbort returns true if snapshot progress's Match
    // is equal or higher than the pendingSnapshot.
    public boolean needSnapshotAbort() {
        return this.getState() == RaftConstantType.ProgressStateType.ProgressStateSnapshot && this.getMatch() >= this.getPendingSnapshot();
    }

    @Override
    public String toString() {
        return String.format("next = %d, match = %d, state = %s, waiting = %s, pendingSnapshot = %d",
                this.next, this.match, this.state, isNodePaused(), this.pendingSnapshot);
    }

    public static class InFlights {
        // the starting index in the buffer
        int start = 0;
        // number of inflights in the buffer
        int count = 0;

        // the size of the buffer
        int size;

        // buffer contains the index of the last entry
        // inside one message.
        int[] buffer;


        public static InFlights newInflights(int size) {
            InFlights inFlights = new InFlights();
            inFlights.size = size;
            inFlights.buffer = new int[0];
            return inFlights;
        }


        // add adds an inflight into inflights
        void add(Integer inflight) {
            if (full()) {
                throw new RuntimeException("cannot add into a full inflights");
            }

            Integer next = start + count;
            Integer currentSize = size;

            if (next >= size) {
                next -= size;
            }

            if (next >= buffer.length) {
                growBuf();
            }

            buffer[next] = inflight;
            count++;
        }


        // grow the inflight buffer by doubling up to inflights.size. We grow on demand
        // instead of preallocating to inflights.size to handle systems which have
        // thousands of Raft groups per process.
        // 按照需要扩充 inflight size
        void growBuf() {
            int newSize = buffer.length * 2;
            if (newSize == 0) {
                newSize = 1;
            } else if (newSize > size) { // 限制最大长度是 size
                newSize = size;
            }
            int[] newBuffer = new int[newSize];
            newBuffer = ArrayUtils.addAll(newBuffer, buffer);  // TODO 优化
            buffer = newBuffer;
        }

        // freeTo frees the inflights smaller or equal to the given `to` flight.
        void freeTo(Integer to) {
            if (count == 0 || to < buffer[start]) {
                // out of the left side of the window
                return;

            }

            Integer idx = start;
            int i = 0;

            for (; i < count; i++) {
                if (to < buffer[idx]) { // found the first large inflight
                    break;
                }

                // increase index and maybe rotate
                int currentSize = size;
                idx++;
                if (idx >= currentSize) {
                    idx -= currentSize;
                }

                count -= i;
                start = idx;
                if (count == 0) {
                    // inflights is empty, reset the start index so that we don't grow the
                    // buffer unnecessarily.
                    start = 0;
                }
            }
        }

        void freeFirstOne() {
            freeTo(buffer[start]);
        }

        // full returns true if the inflights is full.
        boolean full() {
            return count == size;
        }

        // resets frees all inflights.
        void reset() {
            count = 0;
            start = 0;
        }

    }
}
