package com.raft.core;

import com.raft.core.log.RaftLog;
import com.raft.core.storage.StorageState;
import com.raft.core.utils.RaftUtils;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raftpb.Raft.*;

import java.util.*;

/**
 * Created by william on 2018/11/29.
 */
public class RaftCore {

    private static Logger logger = LoggerFactory.getLogger(RaftCore.class);

    private Random globalRandom = new Random(System.nanoTime());

    public long id;

    public int term;

    public long vote;

    public ReadState[] readStates;

    // the log
    public RaftLog raftLog;

    public int maxInflight;

    public long maxMsgSize;

    public Map<Long, Progress> prs;

    public Map<Long, Progress> learnerPrs;

    public RaftConstantType.StateType state;

    // isLearner is true if the local raft node is a learner.
    public boolean isLearner;

    public Map<Long, Boolean> votes;

    public Message[] msgs;
    // the leader id
    public long lead;
    // leadTransferee is id of the leader transfer target when its value is not zero.
    // Follow the procedure defined in raft thesis 3.10.
    public long leadTransferee;
    // New configuration is ignored if there exists unapplied configuration.
    public boolean pendingConf;

    public ReadOnly readOnly;


    // number of ticks since it reached last electionTimeout when it is leader
    // or candidate.
    // number of ticks since it reached last electionTimeout or received a
    // valid message from current leader when it is a follower.
    public int electionElapsed;

    // number of ticks since it reached last heartbeatTimeout.
    // only leader keeps heartbeatElapsed.
    public int heartbeatElapsed;

    public boolean checkQuorum;

    public boolean preVote;

    public int heartbeatTimeout;

    public int electionTimeout;

    // randomizedElectionTimeout is a random number between
    // [electiontimeout, 2 * electiontimeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    public int randomizedElectionTimeout;

    public boolean disableProposalForwarding;

    // step stepFunc
    public StepFunc stepfunc;

    // tick func()
    public TickFunc tickFunc;


    public interface TickFunc {
        void tick() throws Throwable;
    }

    @FunctionalInterface
    public interface StepFunc {
        void step(RaftCore raft, Message message) throws Throwable;
    }

    @FunctionalInterface
    public interface ForEachProgressFunc {
        void process(long id, Progress pr);
    }

    // TODO
    public StepFunc stepLeader = (r, m) -> {
        // These message types do not require any progress for m.From.
        switch (m.getType()) {
            case MsgBeat:
                r.bcastHeartbeat();
                return;
            case MsgCheckQuorum:
                if (!r.checkQuorumActive()) {
                    logger.info(String.format("%d stepped down to follower since quorum is not active", r.id));
                    r.becomeFollower(r.term, RaftConstants.None);
                }
                return;
            case MsgProp:
                if (m.getEntriesCount() == 0) {
                    throw new RuntimeException(String.format("%d stepped empty MsgProp", r.id));
                }
                if (!r.prs.containsKey(r.id)) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    return;
                }
                if (r.leadTransferee != RaftConstants.None) {
                    logger.debug("%d [term %d] transfer leadership to %d is in progress; dropping proposal", r.id, r.term, r.leadTransferee);
                    return;
                }

                for (int i = 0; i < m.getEntriesCount(); i++) {
                    Entry entry = m.getEntries(i);
                    if (entry.getType() == EntryType.EntryConfChange) {
                        if (r.pendingConf) {
                            logger.info(String.format("propose conf %s ignored since pending unapplied configuration", entry.toString()));
                            m.getEntriesList().set(i, Entry.newBuilder().setType(EntryType.EntryNormal).build());
                        }
                        r.pendingConf = true;
                    }
                }
                r.appendEntry(m.getEntriesList().toArray(new Entry[m.getEntriesCount()]));
                r.bcastAppend();
                return;
            case MsgReadIndex:
                if (r.quorum() > 1) {
                    if (r.raftLog.term(r.raftLog.getCommitted()) != r.term) {
                        // Reject read only request when this leader has not committed any log entry at its term.
                        return;
                    }
                    // thinking: use an interally defined context instead of the user given context.
                    // We can express this in terms of the term and index instead of a user-supplied value.
                    // This would allow multiple reads to piggyback on the same message.
                    switch (r.readOnly.getOption()) {
                        case ReadOnlySafe:
                            r.readOnly.addRequest(r.raftLog.getCommitted(), m);
                            r.bcastHeartbeatWithCtx(m.getEntries(0).getData().toByteArray());
                            break;
                        case ReadOnlyLeaseBased:
                            Integer ri = r.raftLog.getCommitted();
                            // from local member
                            if (m.getFrom() == RaftConstants.None || m.getFrom() == r.id) {
                                ReadState readState = new ReadState(r.raftLog.getCommitted(), m.getEntries(0).getData().toByteArray());
                                r.readStates = ArrayUtils.add(r.readStates, readState);
                            } else {
                                r.send(Message.newBuilder().setTo(m.getFrom()).setType(MessageType.MsgReadIndexResp)
                                        .setIndex(ri).addAllEntries(m.getEntriesList()).build());
                            }
                    }
                } else {
                    ReadState readState = new ReadState(r.raftLog.getCommitted(), m.getEntries(0).getData().toByteArray());
                    r.readStates = ArrayUtils.add(r.readStates, readState);
                }
                return;
        }

        // All other message types require a progress for m.From (pr).
        Progress pr = r.getProgress(m.getFrom());
        if (null == pr) {
            logger.debug(String.format("%d no progress available for %d", r.id, m.getFrom()));
            return;
        }

        switch (m.getType()) {
            case MsgAppResp:
                pr.setRecentActive(true);

                if (m.getReject()) {
                    logger.debug(String.format("%d received msgApp rejection(lastindex: %d) from %d for index %d",
                            r.id, m.getRejectHint(), m.getFrom(), m.getIndex()));
                    if (pr.maybeDecrTo(m.getIndex(), m.getRejectHint())) {
                        logger.debug(String.format("%d decreased progress of %d to [%s]", r.id, m.getFrom(), pr));
                        if (pr.getState() == RaftConstantType.ProgressStateType.ProgressStateReplicate) {
                            pr.becomeProbe();
                        }
                        r.sendAppend(m.getFrom());
                    }
                } else {
                    boolean oldPaused = pr.isPaused();
                    if (pr.maybeUpdate(m.getIndex())) {
                        if (pr.getState() == RaftConstantType.ProgressStateType.ProgressStateProbe) {
                            pr.becomeReplicate();
                        } else if (pr.getState() == RaftConstantType.ProgressStateType.ProgressStateSnapshot && pr.needSnapshotAbort()) {
                            logger.debug("%d snapshot aborted, resumed sending replication messages to %d [%s]", r.id, m.getFrom(), pr);
                            pr.becomeProbe();
                        } else if (pr.getState() == RaftConstantType.ProgressStateType.ProgressStateReplicate) {
                            pr.getIns().freeTo(m.getIndex());
                        }

                        if (r.maybeCommit()) {
                            r.bcastAppend();
                        } else if (oldPaused) {
                            // update() reset the wait state on this node. If we had delayed sending
                            // an update before, send it now.
                            r.sendAppend(m.getFrom());
                        }

                        // Transfer leadership is in progress.
                        if (m.getFrom() == r.leadTransferee && pr.getMatch() == r.raftLog.lastIndex()) {
                            logger.info("%d sent MsgTimeoutNow to %d after received MsgAppResp", r.id, m.getFrom());
                            r.sendTimeoutNow(m.getFrom());
                        }
                    }
                }
                break;
            case MsgHeartbeatResp:
                pr.setRecentActive(true);
                pr.resume();

                // free one slot for the full inflights window to allow progress.
                if (pr.getState() == RaftConstantType.ProgressStateType.ProgressStateReplicate && pr.getIns().full()) {
                    pr.getIns().freeFirstOne();
                }
                if (pr.getMatch() < r.raftLog.lastIndex()) {
                    r.sendAppend(m.getFrom());
                }

                if (r.readOnly.getOption() != RaftConstantType.ReadOnlyOption.ReadOnlySafe || m.getContext().isEmpty()) {
                    return;
                }

                Integer ackCount = r.readOnly.recvAck(m);
                if (ackCount < r.quorum()) {
                    return;
                }

                ReadOnly.ReadIndexStatus[] rss = r.readOnly.advance(m);
                for (ReadOnly.ReadIndexStatus rs : rss) {
                    Message req = rs.getReq();
                    // from local member
                    if (req.getFrom() == RaftConstants.None || req.getFrom() == r.id) {
                        r.readStates = ArrayUtils.add(r.readStates, new ReadState(rs.getIndex(), req.getEntries(0).getData().toByteArray()));
                    } else {
                        r.send(Message.newBuilder().setTo(req.getFrom()).setType(MessageType.MsgReadIndexResp)
                                .setIndex(rs.getIndex()).addAllEntries(req.getEntriesList()).build());
                    }
                }

            case MsgSnapStatus:
                if (pr.getState() != RaftConstantType.ProgressStateType.ProgressStateSnapshot) {
                    return;
                }
                if (!m.getReject()) {
                    pr.becomeProbe();
                    logger.debug(String.format("%d snapshot succeeded, resumed sending replication messages to %d [%s]", r.id, m.getFrom(), pr));
                } else {
                    pr.snapshotFailure();
                    pr.becomeProbe();
                    logger.debug(String.format("%d snapshot failed, resumed sending replication messages to %d [%s]", r.id, m.getFrom(), pr));
                }
                // If snapshot finish, wait for the msgAppResp from the remote node before sending
                // out the next msgApp.
                // If snapshot failure, wait for a heartbeat interval before next try
                pr.pause();
            case MsgUnreachable:
                // During optimistic replication, if the remote becomes unreachable,
                // there is huge probability that a MsgApp is lost.
                if (pr.getState() == RaftConstantType.ProgressStateType.ProgressStateReplicate) {
                    pr.becomeProbe();
                }
                logger.debug(String.format("%d failed to send message to %d because it is unreachable [%s]", r.id, m.getFrom(), pr));
            case MsgTransferLeader:
                if (pr.isLearner()) {
                    logger.debug(String.format("%d is learner. Ignored transferring leadership", r.id));
                    return;
                }
                long leadTransferee = m.getFrom();
                long lastLeadTransferee = r.leadTransferee;
                if (lastLeadTransferee != RaftConstants.None) {
                    if (lastLeadTransferee == leadTransferee) {
                        logger.info(String.format("%d [term %d] transfer leadership to %d is in progress, ignores request to same node %d",
                                r.id, r.term, leadTransferee, leadTransferee));
                        return;
                    }
                    r.abortLeaderTransfer();
                    logger.info(String.format("%d [term %d] abort previous transferring leadership to %d", r.id, r.term, lastLeadTransferee));
                }
                if (leadTransferee == r.id) {
                    logger.debug(String.format("%d is already leader. Ignored transferring leadership to self", r.id));
                    return;
                }

                // Transfer leadership to third party.
                logger.info(String.format("%d [term %d] starts to transfer leadership to %d", r.id, r.term, leadTransferee));
                // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
                r.electionElapsed = 0;
                r.leadTransferee = leadTransferee;
                if (pr.getMatch() == raftLog.lastIndex()) {
                    r.sendTimeoutNow(leadTransferee);
                    logger.info(String.format("%d sends MsgTimeoutNow to %d immediately as %d already has up-to-date log", r.id, leadTransferee, leadTransferee));
                } else {
                    r.sendAppend(leadTransferee);
                }
        }

    };

    // stepCandidate is shared by StateCandidate and StatePreCandidate; the difference is
    // whether they respond to MsgVoteResp or MsgPreVoteResp.
    public StepFunc stepCandidate = (r, m) -> {
        // Only handle vote responses corresponding to our candidacy (while in
        // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
        // our pre-candidate state).
        MessageType myVoteRespType;
        if (r.state == RaftConstantType.StateType.StatePreCandidate) {
            myVoteRespType = MessageType.MsgPreVoteResp;
        } else {
            myVoteRespType = MessageType.MsgVoteResp;
        }

        if (m.getType() == myVoteRespType) {
            Integer gr = r.poll(m.getFrom(), m.getType(), !m.getReject());
            logger.info(String.format("%d [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.getType(), r.votes.size() - gr));
            Integer quorumNum = r.quorum();
            if (quorumNum == gr) {
                if (r.state == RaftConstantType.StateType.StatePreCandidate) {
                    r.campaign(RaftConstantType.CampaignType.campaignElection);
                } else {
                    r.becomeLeader();
                    r.bcastAppend();
                }
            } else if (quorumNum == (r.votes.size() - gr)) {
                r.becomeFollower(r.term, RaftConstants.None);
            }
        }

        switch (m.getType()) {
            case MsgProp:
                logger.info(String.format("%d no leader at term %d; dropping proposal", r.id, r.term));
                break;
            case MsgApp:
                r.becomeFollower(r.term, m.getFrom());
                r.handleAppendEntries(m);
                break;
            case MsgHeartbeat:
                r.becomeFollower(r.term, m.getFrom());
                r.handleHeartbeat(m);
                break;
            case MsgSnap:
                r.becomeFollower(m.getTerm(), m.getFrom());
                r.handleSnapshot(m);
                break;
            case MsgTimeoutNow:
                logger.debug(String.format("%d [term %d state %s] ignored MsgTimeoutNow from %d", r.id, r.term, r.state, m.getFrom()));
                break;
        }
    };

    public StepFunc stepFollower = (r, m) -> {
        switch (m.getType()) {
            case MsgProp:
                if (r.lead == RaftConstants.None) {
                    logger.info(String.format("%d no leader at term %d; dropping proposal", r.id, r.term));
                    return;
                } else if (r.disableProposalForwarding) {
                    logger.info("%d not forwarding to leader %d at term %d; dropping proposal", r.id, r.lead, r.term);
                    return;
                }
                m = m.newBuilderForType().setTo(r.lead).build();
                r.send(m);
                break;
            case MsgApp:
                r.electionElapsed = 0;
                r.lead = m.getFrom();
                r.handleAppendEntries(m);
                break;
            case MsgHeartbeat:
                r.electionElapsed = 0;
                r.lead = m.getFrom();
                r.handleSnapshot(m);
                break;
            case MsgSnap:
                r.electionElapsed = 0;
                r.lead = m.getFrom();
                r.handleSnapshot(m);
                break;
            case MsgTransferLeader:
                if (r.lead == RaftConstants.None) {
                    logger.info(String.format("%d no leader at term %d; dropping leader transfer msg", r.id, r.term));
                    return;
                }
                m = m.newBuilderForType().setTo(r.lead).build();
                r.send(m);
                break;
            case MsgTimeoutNow:
                if (r.promotable()) {
                    logger.info(String.format("%d [term %d] received MsgTimeoutNow from %d and starts an election to get leadership.", r.id, r.term, m.getFrom()));
                    // Leadership transfers never use pre-vote even if r.preVote is true; we
                    // know we are not recovering from a partition so there is no need for the
                    // extra round trip.
                    r.campaign(RaftConstantType.CampaignType.campaignTransfer);
                } else {
                    logger.info(String.format("%d received MsgTimeoutNow from %d but is not promotable", r.id, m.getFrom()));
                }
                break;
            case MsgReadIndex:
                if (r.lead == RaftConstants.None) {
                    logger.info(String.format("%d no leader at term %d; dropping index reading msg", r.id, r.term));
                    return;
                }
                m = m.newBuilderForType().setTo(r.lead).build();
                r.send(m);
                break;
            case MsgReadIndexResp:
                if (m.getEntriesCount() != 1) {
                    logger.error(String.format("%d invalid format of MsgReadIndexResp from %d, entries count: %d", r.id, m.getFrom(), m.getEntriesCount()));
                    return;
                }
                r.readStates = ArrayUtils.add(r.readStates, new ReadState(m.getIndex(), m.getEntries(0).getData().toByteArray()));
                break;
        }
    };

    // tickElection is run by followers and candidates after r.electionTimeout.
    public TickFunc tickElection = () -> {
        this.electionElapsed++;
        if (promotable() && pastElectionTimeout()) {
            electionElapsed = 0;
            step(Message.newBuilder().setFrom(this.id).setType(MessageType.MsgHup).build());
        }
    };

    // tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
    public TickFunc tickHeartbeat = () -> {
        this.heartbeatElapsed++;
        this.electionElapsed++;

        if (this.electionElapsed >= this.electionTimeout) {
            this.electionElapsed = 0;
            if (checkQuorum) {
                step(Message.newBuilder().setFrom(this.id).setType(MessageType.MsgCheckQuorum).build());
            }
            // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
            if (this.state == RaftConstantType.StateType.StateLeader && this.leadTransferee != RaftConstants.None) {
                abortLeaderTransfer();
            }
        }

        if (this.state != RaftConstantType.StateType.StateLeader) {
            return;
        }
        if (this.heartbeatElapsed >= this.heartbeatTimeout) {
            this.heartbeatElapsed = 0;
            step(Message.newBuilder().setFrom(this.id).setType(MessageType.MsgBeat).build());
        }
    };


    public static RaftCore newRaft(Config c) throws Throwable {

        // check
        c.validate();

        RaftLog raftLog = RaftLog.newLog(c.getStorage());

        StorageState storageState = c.getStorage().initialState();
        HardState hs = storageState.getHardState();
        ConfState cs = storageState.getConfState();

        Long[] peers = c.getPeers();
        Long[] learners = c.getLearners();

        if (cs.getNodesCount() > 0 || cs.getLearnersCount() > 0) {
            if (peers.length > 0 || learners.length > 0) {
                // TODO(bdarnell): the peers argument is always nil except in
                // tests; the argument should be removed and these tests should be
                // updated to specify their nodes through a snapshot.
                throw new RuntimeException("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)");
            }

            peers = cs.getNodesList().toArray(new Long[cs.getNodesCount()]); //TODO 优化代码
            learners = cs.getLearnersList().toArray(new Long[cs.getNodesCount()]);

        }

        RaftCore r = new RaftCore();
        r.id = c.getId();
        r.lead = RaftConstants.None;
        r.isLearner = false;
        r.raftLog = raftLog;
        r.maxMsgSize = c.getMaxSizePerMsg();
        r.maxInflight = c.getMaxInflightMsgs();
        r.prs = new HashMap<>();
        r.learnerPrs = new HashMap<>();
        r.electionTimeout = c.getElectionTick();
        r.heartbeatTimeout = c.getHeartbeatTick();
        r.checkQuorum = c.isCheckQuorum();
        r.preVote = c.isPreVote();
        r.readOnly = ReadOnly.newReadOnly(c.getReadOnlyOption());
        r.disableProposalForwarding = c.isDisableProposalForwarding();


        if (null != peers) {
            for (Long p : peers) {
                Progress progress = new Progress();
                progress.setNext(1);
                progress.setIns(Progress.InFlights.newInflights(r.maxInflight));
                r.prs.put(p, progress);
            }
        }

        if (null != learners) {
            for (Long p : learners) {
                if (r.prs.containsKey(p)) {
                    throw new RuntimeException(String.format("node %x is in both learner and peer list", p));
                }
                Progress progress = new Progress();
                progress.setNext(1);
                progress.setIns(Progress.InFlights.newInflights(r.maxInflight));
                progress.setLearner(true);
                r.learnerPrs.put(p, progress);
                if (r.id == p) {
                    r.isLearner = true;
                }
            }
        }

        // 不是空的 HardState , load
        if (RaftUtils.isHardStateEqual(hs, RaftConstants.EMPTY_STATE)) {
            r.loadState(hs);
        }
        if (c.getApplied() > 0) {
            raftLog.appliedTo(c.getApplied());
        }

        r.becomeFollower(r.term, RaftConstants.None);

        List<String> nodesStrs = new ArrayList<>();

        for (Long node : r.nodes()) {
            nodesStrs.add(node + "");
        }

        logger.info(String.format("newRaft %d [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
                r.id, String.join(",", nodesStrs), r.term, r.raftLog.getCommitted(), r.raftLog.getApplied(), r.raftLog.lastIndex(), r.raftLog.lastTerm()));

        return r;
    }

    public boolean hasLeader() {
        return this.lead != RaftConstants.None;
    }

    public SoftState softState() {
        return new SoftState(this.lead, this.state);
    }

    public HardState hardState() {
        return HardState.newBuilder().setTerm(this.term).setVote(this.vote).setCommit(this.raftLog.getCommitted()).build();
    }

    public Integer quorum() {
        return this.prs.size() / 2 + 1;
    }

    public Long[] nodes() {
        List<Long> nodesList = new ArrayList<>();
        this.prs.forEach((k, v) -> {
            nodesList.add(k);
        });

        this.learnerPrs.forEach((k, v) -> {
            nodesList.add(k);
        });
        Collections.sort(nodesList);
        return nodesList.toArray(new Long[nodesList.size()]);
    }

    // send persists state to stable storage and then sends to its mailbox.
    public void send(Message m) {
        Message.Builder messageBuilder = m.toBuilder().setFrom(this.id);
        if (m.getType() == MessageType.MsgVote || m.getType() == MessageType.MsgVoteResp
                || m.getType() == MessageType.MsgPreVote || m.getType() == MessageType.MsgPreVoteResp) {
            if (m.getTerm() == 0) {
                // All {pre-,}campaign messages need to have the term set when
                // sending.
                // - MsgVote: m.Term is the term the node is campaigning for,
                //   non-zero as we increment the term when campaigning.
                // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
                //   granted, non-zero for the same reason MsgVote is
                // - MsgPreVote: m.Term is the term the node will campaign,
                //   non-zero as we use m.Term to indicate the next term we'll be
                //   campaigning for
                // - MsgPreVoteResp: m.Term is the term received in the original
                //   MsgPreVote if the pre-vote was granted, non-zero for the
                //   same reasons MsgPreVote is
                throw new RuntimeException(String.format("term should be set when sending %s", m.getType()));
            }
        } else {
            if (m.getTerm() != 0) {
                throw new RuntimeException(String.format("term should not be set when sending %s (was %d)", m.getType(), m.getTerm()));
            }
            // do not attach term to MsgProp, MsgReadIndex
            // proposals are a way to forward to the leader and
            // should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
            if (m.getType() != MessageType.MsgProp && m.getType() != MessageType.MsgReadIndex) {
                messageBuilder.setTerm(this.term);
            }
        }
        this.msgs = ArrayUtils.add(this.msgs, messageBuilder.build());
    }

    public Progress getProgress(Long id) {
        Progress progress = prs.get(id);
        if (null != progress) {
            return progress;
        }
        return learnerPrs.get(id);
    }

    // sendAppend sends RPC, with entries to the given peer.
    public void sendAppend(Long to) {
        Progress pr = getProgress(to);
        if (pr.isPaused()) {
            return;
        }
        Message.Builder mb = Message.newBuilder();
        mb.setTo(to);
        //
        int term;
        Entry[] ents;
        try {
            // 正常流程
            term = raftLog.term(pr.getNext() - 1);
            ents = raftLog.entries(pr.getNext(), maxMsgSize);
            // ents给默认值
            if (null == ents) {
                ents = new Entry[0];
            }
            // 正常流程
            mb.setType(MessageType.MsgApp);
            mb.setIndex(pr.getNext() - 1);
            mb.setLogTerm(term);
            mb.addAllEntries(Arrays.asList(ents));
            mb.setCommit(raftLog.getCommitted());
            if (ents.length != 0) {
                switch (pr.getState()) {
                    // optimistically increase the next when in ProgressStateReplicate
                    case ProgressStateReplicate:
                        int last = ents[ents.length - 1].getIndex();
                        pr.optimisticUpdate(last);
                        pr.getIns().add(last);
                        break;
                    case ProgressStateProbe:
                        pr.pause();
                        break;
                    default:
                        throw new RuntimeException(String.format("%d is sending append in unhandled state %s", this.id, pr.getState()));
                }
            }
        } catch (Throwable t) {
            // 异常流程
            logger.warn("sendAppend erfailedror, send snapshot", t);
            // send snapshot if we failed to get term or entries
            if (!pr.isRecentActive()) {
                logger.debug(String.format("ignore sending snapshot to %s since it is not recently active", to));
                return;
            }
            mb.setType(MessageType.MsgSnap);
            Snapshot snapshot = raftLog.snapshot();
            if (RaftUtils.isEmptySnap(snapshot)) {
                throw new RuntimeException("need non-empty snapshot");
            }
            mb.setSnapshot(snapshot);
            Integer sindex = snapshot.getMetadata().getIndex();
            Integer sterm = snapshot.getMetadata().getTerm();

            logger.debug(String.format("%s [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
                    this.id, this.raftLog.firstIndex(), this.raftLog.getCommitted(), sindex, sterm, to, pr));

            pr.becomeSnapshot(sindex);
            logger.debug(String.format("%d paused sending replication messages to %d [%s]", this.id, to, pr));
        }
        send(mb.build());
    }

    // sendHeartbeat sends an empty MsgApp
    public void sendHeartbeat(Long to, byte[] ctx) {
        // Attach the commit as min(to.matched, r.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        Integer commit = Math.min(getProgress(to).getMatch(), this.raftLog.getCommitted());
        Message m = Message.newBuilder().setTo(to).setType(MessageType.MsgHeartbeat).setCommit(commit).setContext(ByteString.copyFrom(ctx)).build();
        send(m);
    }

    public void forEachProgress(ForEachProgressFunc f) {
        for (Map.Entry<Long, Progress> entry : this.prs.entrySet()) {
            f.process(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<Long, Progress> entry : this.learnerPrs.entrySet()) {
            f.process(entry.getKey(), entry.getValue());
        }
    }

    // bcastAppend sends RPC, with entries to all peers that are not up-to-date
    // according to the progress recorded in r.prs.
    public void bcastAppend() {
        forEachProgress((id, pr) -> {
            if (id == this.id) {
                return;
            }
            sendAppend(id);
        });
    }

    public void bcastHeartbeat() {
        String lastCtx = this.readOnly.lastPendingRequestCtx();
        if (StringUtils.isEmpty(lastCtx)) {
            bcastHeartbeatWithCtx(ByteString.EMPTY.toByteArray()); // TODO 验证，传一个byte[0]
        } else {
            bcastHeartbeatWithCtx(lastCtx.getBytes());
        }
    }

    public void bcastHeartbeatWithCtx(byte[] ctx) {
        forEachProgress((id, Progress) -> {
            if (id == this.id) {
                return;
            }
            sendHeartbeat(id, ctx);
        });
    }

    // maybeCommit attempts to advance the commit index. Returns true if
    // the commit index changed (in which case the caller should call
    // r.bcastAppend).
    public boolean maybeCommit() throws Exception {
        // TODO(bmizerany): optimize.. Currently naive
        List<Integer> mis = new ArrayList<>();
        for (Map.Entry<Long, Progress> entry : this.prs.entrySet()) {
            mis.add(entry.getValue().getMatch());
        }
        // 这里反转获取的id quorum 的原因  TODO
        // order desc ， 排序，倒序
        Collections.sort(mis, Collections.reverseOrder());
        int mci = mis.get(quorum() - 1);
        return raftLog.maybeCommit(mci, this.term);
    }

    public void reset(Integer term) {
        if (this.term != term) {
            this.term = term;
            this.vote = RaftConstants.None;
        }
        this.lead = RaftConstants.None;
        this.electionElapsed = 0;
        this.heartbeatElapsed = 0;
        resetRandomizedElectionTimeout();
        abortLeaderTransfer();

        this.votes = new HashMap<>();
        forEachProgress((id, pr) -> {
            boolean isLearner = pr.isLearner();
            pr = new Progress();
            pr.setNext(this.raftLog.lastIndex() + 1);
            pr.setIns(Progress.InFlights.newInflights(this.maxInflight));
            pr.setLearner(isLearner);
            if (id == this.id) {
                pr.setMatch(raftLog.lastIndex());
            }
        });

        this.pendingConf = false;
        this.readOnly = ReadOnly.newReadOnly(this.readOnly.getOption());
    }


    public void appendEntry(Entry[] es) throws Exception {
        Integer li = this.raftLog.lastIndex();
        for (int i = 0; i < es.length; i++) {
            es[i] = es[i].toBuilder().setTerm(this.term).setIndex(li + 1 + i).build(); //TODO验证 pb的指向性
        }
        this.raftLog.append(es);
        this.getProgress(this.id).maybeUpdate(this.raftLog.lastIndex());
        // Regardless of maybeCommit's return, our caller will call bcastAppend.
        this.maybeCommit();
    }

    public void becomeFollower(Integer term, Long lead) {
        this.stepfunc = stepFollower;
        reset(term);
        this.tickFunc = tickElection;
        this.lead = lead;
        this.state = RaftConstantType.StateType.StateFollower;
        logger.info(String.format("%d became follower at term %d", this.id, this.term));
    }

    public void becomeCandidate() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (this.state == RaftConstantType.StateType.StateLeader) {
            throw new RuntimeException("invalid transition [leader -> candidate]");
        }
        this.stepfunc = stepCandidate;
        reset(this.term + 1);
        this.tickFunc = tickElection;
        this.vote = this.id;
        this.state = RaftConstantType.StateType.StateCandidate;
        logger.info(String.format("%d became candidate at term %d", id, this.term));
    }

    public void becomePreCandidate() {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (this.state == RaftConstantType.StateType.StateLeader) {
            throw new RuntimeException("invalid transition [leader -> pre-candidate]");
        }
        // Becoming a pre-candidate changes our step functions and state,
        // but doesn't change anything else. In particular it does not increase
        // r.Term or change r.Vote.
        this.stepfunc = stepCandidate;
        this.votes = new HashMap<>();
        this.tickFunc = this.tickElection;
        this.state = RaftConstantType.StateType.StatePreCandidate;
        logger.info(String.format("%d became pre-candidate at term %d", id, this.term));
    }

    public void becomeLeader() throws Exception {
        // TODO(xiangli) remove the panic when the raft implementation is stable
        if (this.state == RaftConstantType.StateType.StateFollower) {
            throw new RuntimeException("invalid transition [follower -> leader]");
        }

        this.stepfunc = stepLeader;
        reset(this.term);
        this.tickFunc = tickHeartbeat;
        this.lead = this.id;
        this.state = RaftConstantType.StateType.StateLeader;
        Entry[] ents;
        try {
            ents = this.raftLog.entries(this.raftLog.getCommitted() + 1, RaftConstants.NO_LIMIT);
        } catch (Throwable t) {
            throw new RuntimeException("unexpected error getting uncommitted entries", t);
        }

        Integer nconf = numOfPendingConf(ents);
        if (nconf > 1) {
            throw new RuntimeException("unexpected multiple uncommitted config entry");
        }
        if (nconf == 1) {
            pendingConf = true;
        }
        appendEntry(new Entry[]{Entry.newBuilder().setData(ByteString.EMPTY).build()});
        logger.info(String.format("%d became leader at term %d", this.id, this.term));
    }

    public void campaign(RaftConstantType.CampaignType t) throws Exception {
        Integer currentTerm;
        MessageType voteMsg;
        if (t == RaftConstantType.CampaignType.campaignPreElection) {
            becomeCandidate();
            voteMsg = MessageType.MsgPreVote;
            // PreVote RPCs are sent for the next term before we've incremented r.Term.
            currentTerm = this.term + 1;
        } else {
            becomeCandidate();
            voteMsg = MessageType.MsgVote;
            currentTerm = this.term;
        }

        if (quorum() == poll(this.id, RaftUtils.voteRespMsgType(voteMsg), true)) {
            // We won the election after voting for ourselves (which must mean that
            // this is a single-node cluster). Advance to the next state.
            if (t == RaftConstantType.CampaignType.campaignPreElection) {
                campaign(RaftConstantType.CampaignType.campaignElection);
            } else {
                becomeLeader();
            }
            return;
        }

        for (Map.Entry<Long, Progress> progress : this.prs.entrySet()) {
            long id = progress.getKey();
            if (id == this.id) {
                continue;
            }
            logger.info(String.format("%d [logterm: %d, index: %d] sent %s request to %d at term %d",
                    this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), voteMsg, id, this.term));
            String ctx = "";
            if (t == RaftConstantType.CampaignType.campaignTransfer) {
                ctx = RaftConstantType.CampaignType.campaignTransfer.toString();
            }

            send(Message.newBuilder().setTerm(currentTerm).setTo(progress.getKey()).setType(voteMsg)
                    .setIndex(this.raftLog.lastIndex()).setLogTerm(raftLog.lastTerm()).setContext(ByteString.copyFromUtf8(ctx)).build());
        }

    }

    public Integer poll(Long id, MessageType t, boolean v) {
        int granted = 0;

        if (v) {
            logger.info(String.format("%d received %s from %d at term %d", this.id, t, id, this.term));
        } else {
            logger.info(String.format("%d received %s rejection from %d at term %d", this.id, t, id, this.term));
        }

        if (!this.votes.containsKey(id)) {
            this.votes.put(id, v);
        }

        for (Map.Entry<Long, Boolean> vote : votes.entrySet()) {
            if (vote.getValue()) {
                granted++;
            }
        }
        return granted;
    }


    public void step(Message m) throws Throwable {
        // Handle the message term, which may result in our stepping down to a follower.
        if (m.getTerm() == 0) {
            // local message
        }
        // 收到消息的Term 大于本地 Term
        else if (m.getTerm() > this.term) {
            if (m.getType() == MessageType.MsgVote || m.getType() == MessageType.MsgPreVote) {
                boolean force = RaftConstantType.CampaignType.campaignTransfer.toString().equals(m.getContext().toStringUtf8());
                boolean inLease = checkQuorum && this.lead != RaftConstants.None && electionElapsed < electionTimeout;
                if (!force && inLease) {
                    // If a server receives a RequestVote request within the minimum election timeout
                    // of hearing from a current leader, it does not update its term or grant its vote
                    logger.info("%d [term: %d] received a %d message with higher term from %d [term: %d]", this.id, this.term, m.getType(), m.getFrom(), m.getTerm());
                    return;
                }
            }

            if (m.getType() == MessageType.MsgPreVote) {
                // Never change our term in response to a PreVote
            } else if (m.getType() == MessageType.MsgPreVoteResp && !m.getReject()) {
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
            } else {
                logger.info(String.format("%d [term: %d] received a %s message with higher term from %d [term: %d]",
                        this.id, this.term, m.getType(), m.getFrom(), m.getTerm()));
                if (m.getType() == MessageType.MsgApp || m.getType() == MessageType.MsgHeartbeat || m.getType() == MessageType.MsgSnap) {
                    becomeFollower(m.getTerm(), m.getFrom());
                } else {
                    becomeFollower(m.getTerm(), RaftConstants.None);
                }
            }
        }
        // 收到消息的Term 小于本地 Term
        else if (m.getTerm() < this.term) {
            if (checkQuorum && (m.getType() == MessageType.MsgHeartbeat || m.getType() == MessageType.MsgApp)) {
                // We have received messages from a leader at a lower term. It is possible
                // that these messages were simply delayed in the network, but this could
                // also mean that this node has advanced its term number during a network
                // partition, and it is now unable to either win an election or to rejoin
                // the majority on the old term. If checkQuorum is false, this will be
                // handled by incrementing term numbers in response to MsgVote with a
                // higher term, but if checkQuorum is true we may not advance the term on
                // MsgVote and must generate other messages to advance the term. The net
                // result of these two features is to minimize the disruption caused by
                // nodes that have been removed from the cluster's configuration: a
                // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
                // but it will not receive MsgApp or MsgHeartbeat, so it will not create
                // disruptive term increases
                send(Message.newBuilder().setTo(m.getFrom()).setType(MessageType.MsgAppResp).build());
            } else {
                logger.info(String.format("%d [term: %d] ignored a %s message with lower term from %d [term: %d]",
                        this.id, this.term, m.getType(), m.getFrom(), m.getTerm()));
            }
            return;
        }

        //
        switch (m.getType()) {
            case MsgHup:
                if (this.state != RaftConstantType.StateType.StateLeader) {
                    Entry[] ents = this.raftLog.slice(this.raftLog.getApplied() + 1, this.raftLog.getCommitted() + 1, RaftConstants.NO_LIMIT);
                    Integer n = numOfPendingConf(ents);
                    if (n != 0 && this.raftLog.getCommitted() > this.raftLog.getApplied()) {
                        logger.warn(String.format("%d cannot campaign at term %d since there are still %d pending configuration changes to apply",
                                this.id, this.term, n));
                        return;
                    }
                    logger.info(String.format("%d is starting a new election at term %d", this.id, this.term));
                    if (this.preVote) {
                        campaign(RaftConstantType.CampaignType.campaignPreElection);
                    } else {
                        campaign(RaftConstantType.CampaignType.campaignElection);
                    }
                } else {
                    logger.debug("%d ignoring MsgHup because already leader", this.id);
                }
                break;
            // MsgVote 和 MsgPreVote 一同处理
            case MsgVote:
            case MsgPreVote:
                if (this.isLearner) {
                    // TODO: learner may need to vote, in case of node down when confchange.
                    logger.info(String.format("%d [logterm: %d, index: %d, vote: %d] ignored %s from %d [logterm: %d, index: %d] at term %d: learner can not vote",
                            this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(), this.vote, m.getType(), m.getFrom(), m.getLogTerm(), m.getIndex(), this.term));
                    return;
                }
                // The m.Term > r.Term clause is for MsgPreVote. For MsgVote m.Term should always equal r.Term.
                if ((this.vote == RaftConstants.None || m.getTerm() > this.term || this.vote == m.getFrom())
                        && this.raftLog.isUpToDate(m.getIndex(), m.getLogTerm())) {

                    logger.info(String.format("%d [logterm: %d, index: %d, vote: %d] cast %s for %d [logterm: %d, index: %d] at term %d",
                            this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(),
                            this.vote, m.getType(), m.getFrom(), m.getLogTerm(), m.getIndex(), this.term));
                    // When responding to Msg{Pre,}Vote messages we include the term
                    // from the message, not the local term. To see why consider the
                    // case where a single node was previously partitioned away and
                    // it's local term is now of date. If we include the local term
                    // (recall that for pre-votes we don't update the local term), the
                    // (pre-)campaigning node on the other end will proceed to ignore
                    // the message (it ignores all out of date messages).
                    // The term in the original message and current local term are the
                    // same in the case of regular votes, but different for pre-votes.
                    send(Message.newBuilder().setTo(m.getFrom()).setTerm(m.getTerm()).setType(RaftUtils.voteRespMsgType(m.getType())).build());
                    if (m.getType() == MessageType.MsgVote) {
                        // only recored real votes
                        this.electionElapsed = 0;
                        this.vote = m.getFrom();
                    }
                } else {
                    logger.info(String.format("%d [logterm: %d, index: %d, vote: %x] rejected %s from %d [logterm: %d, index: %d] at term %d",
                            this.id, this.raftLog.lastTerm(), this.raftLog.lastIndex(),
                            this.vote, m.getType(), m.getFrom(), m.getLogTerm(), m.getIndex(), this.term));
                    send(Message.newBuilder().setTo(m.getFrom()).setTerm(m.getTerm()).setType(RaftUtils.voteRespMsgType(m.getType())).setReject(true).build());
                }
                break;

            default:
                this.stepfunc.step(this, m);
        }
    }


    public void handleAppendEntries(Message m) throws Throwable {
        if (m.getIndex() < this.raftLog.getCommitted()) {
            send(Message.newBuilder().setTo(m.getFrom()).setType(MessageType.MsgAppResp).setIndex(this.raftLog.getCommitted()).build());
            return;
        }
        //  Integer mlastIndex
        RaftLog.AppendResult appendResult = this.raftLog.maybeAppend(m.getIndex(), m.getLogTerm(), m.getCommit(), m.getEntriesList().toArray(new Entry[m.getEntriesCount()]));
        Integer mlastIndex = appendResult.lastnewi;
        boolean ok = appendResult.isOk;

        if (ok) {
            send(Message.newBuilder().setTo(m.getFrom()).setType(MessageType.MsgAppResp).setIndex(mlastIndex).build());
        } else {
            logger.debug(String.format("%d [logterm: %d, index: %d] rejected msgApp [logterm: %d, index: %d] from %d"
                    , this.id, this.raftLog.term(m.getIndex()), m.getIndex(), m.getLogTerm(), m.getIndex(), m.getFrom()));
            send(Message.newBuilder().setTo(m.getFrom()).setType(MessageType.MsgAppResp)
                    .setIndex(m.getIndex()).setReject(true).setRejectHint(this.raftLog.lastIndex()).build());
        }
    }

    public void handleHeartbeat(Message m) {
        this.raftLog.commitTo(m.getCommit());
        send(Message.newBuilder().setTo(m.getFrom()).setType(MessageType.MsgHeartbeatResp).setContext(m.getContext()).build());
    }

    public void handleSnapshot(Message m) throws Exception {
        Integer sindex = m.getSnapshot().getMetadata().getIndex();
        Integer sterm = m.getSnapshot().getMetadata().getTerm();

        if (restore(m.getSnapshot())) {
            logger.info(String.format("%d [commit: %d] restored snapshot [index: %d, term: %d]",
                    this.id, this.raftLog.getCommitted(), sindex, sterm));
            send(Message.newBuilder().setTo(m.getFrom()).setType(MessageType.MsgAppResp).setIndex(this.raftLog.lastIndex()).build());
        } else {
            logger.info(String.format("%d [commit: %d] ignored snapshot [index: %d, term: %d]",
                    this.id, this.raftLog.getCommitted(), sindex, sterm));
            send(Message.newBuilder().setTo(m.getFrom()).setType(MessageType.MsgAppResp).setIndex(this.raftLog.getCommitted()).build());
        }
    }

    // restore recovers the state machine from a snapshot. It restores the log and the
    // configuration of state machine.
    public boolean restore(Snapshot s) throws Exception {
        if (s.getMetadata().getIndex() <= this.raftLog.getCommitted()) {
            return false;
        }

        if (this.raftLog.matchTerm(s.getMetadata().getIndex(), s.getMetadata().getTerm())) {
            logger.info(String.format("%d [commit: %d, lastindex: %d, lastterm: %d] fast-forwarded commit to snapshot [index: %d, term: %d]",
                    this.id, this.raftLog.getCommitted(), this.raftLog.lastIndex(), this.raftLog.lastTerm(), s.getMetadata().getIndex(), s.getMetadata().getTerm()));

            this.raftLog.commitTo(s.getMetadata().getIndex());
            return false;
        }

        // The normal peer can't become learner.
        if (!this.isLearner) {
            for (Long id : s.getMetadata().getConfState().getLearnersList()) {
                if (this.id == id) {
                    logger.error(String.format("%d can't become learner when restores snapshot [index: %d, term: %d]",
                            this.id, s.getMetadata().getIndex(), s.getMetadata().getTerm()));
                    return false;
                }
            }
        }
        logger.info(String.format("%x [commit: %d, lastindex: %d, lastterm: %d] starts to restore snapshot [index: %d, term: %d]",
                this.id, this.raftLog.getCommitted(), this.raftLog.lastIndex(), this.raftLog.lastTerm(), s.getMetadata().getIndex(), s.getMetadata().getTerm()));
        this.raftLog.restore(s);
        this.prs = new HashMap<>();
        this.learnerPrs = new HashMap<>();
        restoreNode(s.getMetadata().getConfState().getNodesList(), false);
        restoreNode(s.getMetadata().getConfState().getLearnersList(), true);
        return true;
    }

    public void restoreNode(List<Long> nodes, boolean isLearner) {
        for (Long n : nodes) {
            int match = 0;
            int next = this.raftLog.lastIndex() + 1;
            if (n == this.id) {
                match = next - 1;
                this.isLearner = isLearner;
            }
            setProgress(n, match, next, isLearner);
            logger.info(String.format("%d restored progress of %d [%s]", this.id, n, getProgress(n)));
        }
    }

    // promotable
    // promotable indicates whether state machine can be promoted to leader,
    // which is true when its own id is in progress list.
    public boolean promotable() {
        return this.prs.containsKey(this.id);
    }

    public void addNode(Long id) {
        addNodeOrLearnerNode(id, false);
    }

    public void addLearner(Long id) {
        addNodeOrLearnerNode(id, true);
    }

    public void addNodeOrLearnerNode(Long id, boolean isLearner) {
        this.pendingConf = false;
        Progress pr = getProgress(id);
        if (null == pr) {
            setProgress(id, 0, this.raftLog.lastIndex() + 1, isLearner);
        } else {
            // can only change Learner to Voter
            if (isLearner && !pr.isLearner()) {
                logger.info(String.format("%d ignored addLeaner: do not support changing %d from raft peer to learner.", this.id, id));
                return;
            }
            if (isLearner == pr.isLearner()) {
                // Ignore any redundant addNode calls (which can happen because the
                // initial bootstrapping entries are applied twice).
                return;
            }

            // change Learner to Voter, use origin Learner progress
            this.learnerPrs.remove(id);
            pr.setLearner(false);
            this.prs.put(id, pr);
        }

        if (this.id == id) {
            this.isLearner = isLearner;
        }

        // When a node is first added, we should mark it as recently active.
        // Otherwise, CheckQuorum may cause us to step down if it is invoked
        // before the added node has a chance to communicate with us.
        pr = getProgress(id);
        pr.setRecentActive(true);
    }

    public void removeNode(Long id) throws Throwable {
        delProgress(id);
        this.pendingConf = false;

        // do not try to commit or abort transferring if there is no nodes in the cluster.
        if (prs.size() == 0 && learnerPrs.size() == 0) {
            return;
        }

        // The quorum size is now smaller, so see if any pending entries can
        // be committed.
        if (maybeCommit()) {
            bcastAppend();
        }
        // If the removed node is the leadTransferee, then abort the leadership transferring.
        if (this.state == RaftConstantType.StateType.StateLeader && this.leadTransferee == id) {
            abortLeaderTransfer();
        }
    }

    public void resetPendingConf() {
        this.pendingConf = false;
    }

    public void setProgress(Long id, Integer match, Integer next, boolean isLearner) {
        // 非learner设置
        if (!isLearner) {
            this.learnerPrs.remove(id);
            Progress progress = new Progress();
            progress.setNext(next);
            progress.setMatch(match);
            progress.setIns(Progress.InFlights.newInflights(this.maxInflight));
            prs.put(id, progress);
            return;
        }
        // 设置learner && 但是在非learners的 map中
        if (this.prs.containsKey(id)) {
            throw new RuntimeException(String.format("%s unexpected changing from voter to learner for %s", this.id, id));
        }
        // learner设置
        Progress progress = new Progress();
        progress.setNext(next);
        progress.setMatch(match);
        progress.setIns(Progress.InFlights.newInflights(this.maxInflight));
        progress.setLearner(true);
        this.learnerPrs.put(id, progress);
    }

    public void delProgress(Long id) {
        this.prs.remove(id);
        this.learnerPrs.remove(id);
    }

    public void loadState(HardState state) {
        if (state.getCommit() < this.raftLog.getCommitted() || state.getCommit() > this.raftLog.lastIndex()) {
            throw new RuntimeException(String.format("%x state.commit %d is out of range [%d, %d]",
                    this.id, state.getCommit(), this.raftLog.getCommitted(), this.raftLog.lastIndex()));
        }
        this.raftLog.setCommitted(state.getCommit());
        this.term = state.getTerm();
        this.vote = state.getVote();
    }

    // pastElectionTimeout returns true iff r.electionElapsed is greater
    // than or equal to the randomized election timeout in
    // [electiontimeout, 2 * electiontimeout - 1].  // TODO 含义是什么?
    public boolean pastElectionTimeout() {
        return this.electionElapsed >= this.randomizedElectionTimeout;
    }

    public void resetRandomizedElectionTimeout() {
        this.randomizedElectionTimeout = this.electionTimeout + RaftUtils.randomInt(this.globalRandom, this.electionTimeout);
    }

    // checkQuorumActive returns true if the quorum is active from
    // the view of the local raft state machine. Otherwise, it returns
    // false.
    // checkQuorumActive also resets all RecentActive to false.
    public boolean checkQuorumActive() {
        class ActVal {
            Integer act = 0;
        }
        final ActVal actVal = new ActVal();

        forEachProgress((long id, Progress pr) -> {
            if (id == this.id) { // self is always active
                actVal.act++;
            }

            if (pr.isRecentActive() && !pr.isLearner()) {
                actVal.act++;
            }
            pr.setRecentActive(false);
        });
        return actVal.act >= quorum();
    }

    public void sendTimeoutNow(Long to) throws Throwable {
        Message msg = Message.newBuilder().setTo(to).setType(MessageType.MsgTimeoutNow).build();
        send(msg);
    }

    public void abortLeaderTransfer() {
        this.leadTransferee = RaftConstants.None;
    }

    public Integer numOfPendingConf(Entry[] ents) {
        int n = 0;
        if (null != ents) {
            for (Entry ent : ents) {
                if (ent.getType() == EntryType.EntryConfChange) {
                    n++;
                }
            }
        }
        return n;
    }

}
