package com.raft.core;

/**
 * Created by william on 2018/11/29.
 * <p>
 * <p>
 * 定义所有的Type集合
 */
public class RaftConstantType {

    public enum StateType {

        StateFollower(0),
        StateCandidate(1),
        StateLeader(2),
        StatePreCandidate(3),
        numStates(4);
        private int value;

        StateType(int value) {
            this.value = value;
        }
    }

    public enum ReadOnlyOption {

        // ReadOnlySafe guarantees the linearizability of the read only request by
        // communicating with the quorum. It is the default and suggested option.
        ReadOnlySafe(0),

        // ReadOnlyLeaseBased ensures linearizability of the read only request by
        // relying on the leader lease. It can be affected by clock drift.
        // If the clock drift is unbounded, leader might keep the lease longer than it
        // should (clock can move backward/pause without any bound). ReadIndex is not safe
        // in that case.
        ReadOnlyLeaseBased(1);

        private int value;

        ReadOnlyOption(int value) {
            this.value = value;
        }
    }


    public enum CampaignType {

        // campaignPreElection represents the first phase of a normal election when
        // Config.PreVote is true.
        campaignPreElection("CampaignPreElection"),

        // campaignElection represents a normal (time-based) election (the second phase
        // of the election when Config.PreVote is true).
        campaignElection("CampaignElection"),

        // campaignTransfer represents the type of leader transfer
        campaignTransfer("CampaignTransfer");

        private String value;

        CampaignType(String value) {
            this.value = value;
        }
    }

    public enum ProgressStateType {

        ProgressStateProbe("ProgressStateProbe"),
        ProgressStateReplicate("ProgressStateReplicate"),
        ProgressStateSnapshot("ProgressStateSnapshot");

        private String value;
        ProgressStateType(String value) {
            this.value = value;
        }

    }

}
