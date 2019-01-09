package com.raft.core.utils;

import com.raft.core.log.Unstable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raftpb.Raft.*;

import java.util.Random;

import static com.raft.core.RaftConstants.NO_LIMIT;

/**
 * Created by william on 2018/11/28.
 */
public class RaftUtils {

    public static Logger logger = LoggerFactory.getLogger(Unstable.class);

    // TODO  limit size 暂时不处理
    public static Entry[] limitSize(Entry[] ents, Long maxSize) {

        if (ents.length == 0) {
            return ents;
        }

        if (maxSize != NO_LIMIT) {
            logger.warn("limit size not process ");
        } else {
            // NOT DO
        }
        return ents;
    }


    public static Integer randomInt(Random random, Integer max) {
        return random.nextInt(max);
    }


    public static boolean isHardStateEqual(HardState a, HardState b) {
        return a.getTerm() == b.getTerm() &&
                a.getVote() == b.getVote() &&
                a.getCommit() == b.getCommit();
    }

    public static boolean isEmptySnap(Snapshot sp) {
        return sp.getMetadata().getIndex() == 0;
    }

    public static MessageType voteRespMsgType(MessageType msgt) {
        switch (msgt) {
            case MsgVote:
                return MessageType.MsgVoteResp;
            case MsgPreVote:
                return MessageType.MsgPreVoteResp;
            default:
                throw new RuntimeException(String.format("not a vote message: %s", msgt));
        }
    }


    public static void main(String[] args) {
        Random random = new Random(1);
        for (int i = 0; i < 100; i++) {
            int x = RaftUtils.randomInt(random, 100);
            System.out.println(x);
        }
    }
}
