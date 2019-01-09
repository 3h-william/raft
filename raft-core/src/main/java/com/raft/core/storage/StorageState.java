package com.raft.core.storage;

import raftpb.Raft.*;

/**
 * Created by william on 2018/11/27.
 */

/**
 *
 * Pair of  pb.HardState & pb.ConfState
 *
 */
public class StorageState {

    private HardState hardState;
    private ConfState confState;

    public StorageState(HardState hardState, ConfState confState) {
        this.hardState = hardState;
        this.confState = confState;
    }

    public HardState getHardState() {
        return hardState;
    }

    public void setHardState(HardState hardState) {
        this.hardState = hardState;
    }

    public ConfState getConfState() {
        return confState;
    }

    public void setConfState(ConfState confState) {
        this.confState = confState;
    }
}
