package com.raft.core;

import com.raft.core.Config;
import com.raft.core.RaftConstants;
import com.raft.core.RaftCore;
import com.raft.core.storage.Storage;
import raftpb.Raft;

/**
 * Created by william on 2018/12/19.
 *
 * 提供测试的一些基础公用方法
 */
public class TestUtils {

    public static Config newTestConfig(long id, Long[] peers, int election, int heartbeat, Storage storage) {
        Config config = new Config();
        config.setId(id);
        config.setPeers(peers);
        config.setElectionTick(election);
        config.setHeartbeatTick(heartbeat);
        config.setStorage(storage);
        config.setMaxSizePerMsg(RaftConstants.NO_LIMIT);
        config.setMaxInflightMsgs(256);
        return config;
    }

    public static RaftCore newTestRaft(long id, Long[] peers, int election, int heartbeat, Storage storage) throws Throwable {
        return RaftCore.newRaft(newTestConfig(id, peers, election, heartbeat, storage));
    }

    public static Long[] idsBySize(int size) {
        Long[] ids = new Long[size];
        for (int i = 0; i < ids.length; i++) {
            ids[i] = 1L + i ;
        }
        return ids;
    }


    public static Raft.Message[] readMessages(RaftCore r) {
        Raft.Message[] msgs = r.msgs;
        r.msgs = new Raft.Message[]{};

        if (null== msgs ) {
            return new Raft.Message[0];
        }
        return msgs;
    }

}
