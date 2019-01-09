package com.raft.core;

import raftpb.Raft.*;

/**
 * Created by william on 2018/11/27.
 */
public class RaftConstants {

    public static long NO_LIMIT = Long.MAX_VALUE;

    // None is a placeholder node ID used when there is no leader.
    public static Long None = 0L;


    public static HardState EMPTY_STATE = HardState.newBuilder().build();
}
