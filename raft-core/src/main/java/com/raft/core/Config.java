package com.raft.core;

import com.raft.core.storage.Storage;

/**
 * Created by william on 2018/11/16.
 */
// raft.go  type Config struct
public class Config {

    // ID is the identity of the local raft. ID cannot be 0.
    private long id;

    // peers contains the IDs of all nodes (including self) in the raft cluster. It
    // should only be set when starting a new raft cluster. Restarting raft from
    // previous configuration will panic if peers is set. peer is private and only
    // used for testing right now.
    private Long[] peers;

    // learners contains the IDs of all leaner nodes (including self if the local node is a leaner) in the raft cluster.
    // learners only receives entries from the leader node. It does not vote or promote itself.
    private Long[] learners;

    // ElectionTick is the number of Node.Tick invocations that must pass between
    // elections. That is, if a follower does not receive any message from the
    // leader of current term before ElectionTick has elapsed, it will become
    // candidate and start an election. ElectionTick must be greater than
    // HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
    // unnecessary leader switching.
    private int electionTick;
    // HeartbeatTick is the number of Node.Tick invocations that must pass between
    // heartbeats. That is, a leader sends heartbeat messages to maintain its
    // leadership every HeartbeatTick ticks.
    private int heartbeatTick;

    // Storage is the storage for raft. raft generates entries and states to be
    // stored in storage. raft reads the persisted entries and states out of
    // Storage when it needs. raft reads out the previous state and configuration
    // out of storage when restarting.
    private Storage storage;
    // Applied is the last applied index. It should only be set when restarting
    // raft. raft will not return entries to the application smaller or equal to
    // Applied. If Applied is unset when restarting, raft might return previous
    // applied entries. This is a very application dependent configuration.
    private int applied;

    // MaxSizePerMsg limits the max size of each append message. Smaller value
    // lowers the raft recovery cost(initial probing and message lost during normal
    // operation). On the other side, it might affect the throughput during normal
    // replication. Note: math.MaxUint64 for unlimited, 0 for at most one entry per
    // message.
    private long maxSizePerMsg;
    // MaxInflightMsgs limits the max number of in-flight append messages during
    // optimistic replication phase. The application transportation layer usually
    // has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
    // overflowing that sending buffer. TODO (xiangli): feedback to application to
    // limit the proposal rate?
    private int maxInflightMsgs;

    // CheckQuorum specifies if the leader should check quorum activity. Leader
    // steps down when quorum is not active for an electionTimeout.
    private boolean checkQuorum;

    // PreVote enables the Pre-Vote algorithm described in raft thesis section
    // 9.6. This prevents disruption when a node that has been partitioned away
    // rejoins the cluster.
    private boolean preVote;

    // ReadOnlyOption specifies how the read only request is processed.
    //
    // ReadOnlySafe guarantees the linearizability of the read only request by
    // communicating with the quorum. It is the default and suggested option.
    //
    // ReadOnlyLeaseBased ensures linearizability of the read only request by
    // relying on the leader lease. It can be affected by clock drift.
    // If the clock drift is unbounded, leader might keep the lease longer than it
    // should (clock can move backward/pause without any bound). ReadIndex is not safe
    // in that case.
    // CheckQuorum MUST be enabled if ReadOnlyOption is ReadOnlyLeaseBased.
    private RaftConstantType.ReadOnlyOption readOnlyOption;

    // DisableProposalForwarding set to true means that followers will drop
    // proposals, rather than forwarding them to the leader. One use case for
    // this feature would be in a situation where the Raft leader is used to
    // compute the data of a proposal, for example, adding a timestamp from a
    // hybrid logical clock to data in a monotonically increasing way. Forwarding
    // should be disabled to prevent a follower with an innaccurate hybrid
    // logical clock from assigning the timestamp and then forwarding the data
    // to the leader.
    private boolean disableProposalForwarding;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Long[] getPeers() {
        return peers;
    }

    public void setPeers(Long[] peers) {
        this.peers = peers;
    }

    public Long[] getLearners() {
        return learners;
    }

    public void setLearners(Long[] learners) {
        this.learners = learners;
    }

    public int getElectionTick() {
        return electionTick;
    }

    public void setElectionTick(int electionTick) {
        this.electionTick = electionTick;
    }

    public int getHeartbeatTick() {
        return heartbeatTick;
    }

    public void setHeartbeatTick(int heartbeatTick) {
        this.heartbeatTick = heartbeatTick;
    }

    public Storage getStorage() {
        return storage;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public int getApplied() {
        return applied;
    }

    public void setApplied(int applied) {
        this.applied = applied;
    }

    public long getMaxSizePerMsg() {
        return maxSizePerMsg;
    }

    public void setMaxSizePerMsg(long maxSizePerMsg) {
        this.maxSizePerMsg = maxSizePerMsg;
    }

    public int getMaxInflightMsgs() {
        return maxInflightMsgs;
    }

    public void setMaxInflightMsgs(int maxInflightMsgs) {
        this.maxInflightMsgs = maxInflightMsgs;
    }

    public boolean isCheckQuorum() {
        return checkQuorum;
    }

    public void setCheckQuorum(boolean checkQuorum) {
        this.checkQuorum = checkQuorum;
    }

    public boolean isPreVote() {
        return preVote;
    }

    public void setPreVote(boolean preVote) {
        this.preVote = preVote;
    }

    public RaftConstantType.ReadOnlyOption getReadOnlyOption() {
        return readOnlyOption;
    }

    public void setReadOnlyOption(RaftConstantType.ReadOnlyOption readOnlyOption) {
        this.readOnlyOption = readOnlyOption;
    }

    public boolean isDisableProposalForwarding() {
        return disableProposalForwarding;
    }

    public void setDisableProposalForwarding(boolean disableProposalForwarding) {
        this.disableProposalForwarding = disableProposalForwarding;
    }

    public void validate() throws Throwable{

        if (id == RaftConstants.None) {
            throw new RuntimeException("cannot use none as id");
        }

        if (heartbeatTick <= 0) {
            throw new RuntimeException("election tick must be greater than heartbeat tick");
        }

        if (storage == null) {
            throw new RuntimeException("storage cannot be nil");
        }

        if (maxInflightMsgs <= 0) {
            throw new RuntimeException("max inflight messages must be greater than 0");
        }

        if (readOnlyOption == RaftConstantType.ReadOnlyOption.ReadOnlyLeaseBased && !checkQuorum) {
            throw new RuntimeException("CheckQuorum must be enabled when ReadOnlyOption is ReadOnlyLeaseBased");
        }

    }

}
