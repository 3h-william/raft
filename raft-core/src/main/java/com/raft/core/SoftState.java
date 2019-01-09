package com.raft.core;

/**
 * Created by william on 2018/12/6.
 */

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.

// node.go  type SoftState struct
public class SoftState {

    private long lead;
    private RaftConstantType.StateType raftState;

    public SoftState(long lead, RaftConstantType.StateType raftState) {
        this.lead = lead;
        this.raftState = raftState;
    }

    public long getLead() {
        return lead;
    }

    public void setLead(long lead) {
        this.lead = lead;
    }

    public RaftConstantType.StateType getRaftState() {
        return raftState;
    }

    public void setRaftState(RaftConstantType.StateType raftState) {
        this.raftState = raftState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SoftState softState = (SoftState) o;

        if (lead != softState.lead) return false;
        return raftState == softState.raftState;
    }

    @Override
    public int hashCode() {
        int result = (int) (lead ^ (lead >>> 32));
        result = 31 * result + (raftState != null ? raftState.hashCode() : 0);
        return result;
    }
}
