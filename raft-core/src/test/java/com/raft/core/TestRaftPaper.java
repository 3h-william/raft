package com.raft.core;

import com.google.common.collect.ImmutableMap;
import com.raft.core.storage.MemoryStorage;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;
import raftpb.Raft.*;

import java.util.*;

/**
 * Created by william on 2018/12/19.
 */
public class TestRaftPaper {


    @Test
    public void TestFollowerUpdateTermFromMessage() throws Throwable {
        testUpdateTermFromMessage(RaftConstantType.StateType.StateFollower);
    }

    @Test
    public void TestCandidateUpdateTermFromMessage() throws Throwable {
        testUpdateTermFromMessage(RaftConstantType.StateType.StateCandidate);
    }

    @Test
    public void TestLeaderUpdateTermFromMessage() throws Throwable {
        testUpdateTermFromMessage(RaftConstantType.StateType.StateLeader);
    }

    // testUpdateTermFromMessage tests that if one server’s current term is
    // smaller than the other’s, then it updates its current term to the larger
    // value. If a candidate or leader discovers that its term is out of date,
    // it immediately reverts to follower state.
    // Reference: section 5.1
    public void testUpdateTermFromMessage(RaftConstantType.StateType state) throws Throwable {
        RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, new MemoryStorage());
        switch (state) {
            case StateFollower:
                r.becomeFollower(1, 2L);
                break;
            case StateCandidate:
                r.becomeCandidate();
                break;
            case StateLeader:
                r.becomeCandidate();
                r.becomeLeader();
                break;
        }

        r.step(raftpb.Raft.Message.newBuilder().setType(raftpb.Raft.MessageType.MsgApp).setTerm(2).build());
        Assert.assertEquals(r.term, 2);
        Assert.assertEquals(r.state, RaftConstantType.StateType.StateFollower);
    }


    // TestRejectStaleTermMessage tests that if a server receives a request with
    // a stale term number, it rejects the request.
    // Our implementation ignores the request instead.
    // Reference: section 5.1
    @Test
    public void TestRejectStaleTermMessage() throws Throwable {
        class Called {
            boolean val = false;
        }
        final Called called = new Called();
        RaftCore.StepFunc fakeStep = (r, m) -> {
            called.val = true;
        };
        RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, new MemoryStorage());
        r.stepfunc = fakeStep;
        r.loadState(HardState.newBuilder().setTerm(2).build());

        r.step(Message.newBuilder().setType(MessageType.MsgApp).setTerm(r.term - 1).build());
        Assert.assertEquals(called.val, false);
    }

    // TestStartAsFollower tests that when servers start up, they begin as followers.
    // Reference: section 5.2
    @Test
    public void TestStartAsFollower() throws Throwable {
        RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, new MemoryStorage());
        Assert.assertEquals(r.state, RaftConstantType.StateType.StateFollower);
    }

    // TestLeaderBcastBeat tests that if the leader receives a heartbeat tick,
    // it will send a msgApp with m.Index = 0, m.LogTerm=0 and empty entries as
    // heartbeat to all followers.
    // Reference: section 5.2
    @Test
    public void TestLeaderBcastBeat() throws Throwable {
        // heartbeat interval
        Integer hi = 1;
        RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, hi, new MemoryStorage());
        r.becomeCandidate();
        r.becomeLeader();
        for (int i = 0; i < 10; i++) {
            r.appendEntry(new Entry[]{Entry.newBuilder().setIndex(i + 1).build()});
        }
        for (int i = 0; i < hi; i++) {
            r.tickFunc.tick();
        }
        Message[] msgs = TestUtils.readMessages(r);
        Arrays.sort(msgs, new MessageSort());
        //  Java Protobuf 的 equal 必须默认值也进行处理
        Message[] wmsgs = new Message[]{
                Message.newBuilder().setFrom(1).setTo(2).setTerm(1).setType(MessageType.MsgHeartbeat).setCommit(0).setContext(ByteString.EMPTY).build(),
                Message.newBuilder().setFrom(1).setTo(3).setTerm(1).setType(MessageType.MsgHeartbeat).setCommit(0).setContext(ByteString.EMPTY).build()
        };
        Assert.assertArrayEquals(wmsgs, msgs);
    }

    @Test
    public void TestFollowerStartElection() throws Throwable {
        testNonleaderStartElection(RaftConstantType.StateType.StateFollower);
    }

    @Test
    public void TestCandidateStartNewElection() throws Throwable {
        testNonleaderStartElection(RaftConstantType.StateType.StateCandidate);
    }

    // testNonleaderStartElection tests that if a follower receives no communication
    // over election timeout, it begins an election to choose a new leader. It
    // increments its current term and transitions to candidate state. It then
    // votes for itself and issues RequestVote RPCs in parallel to each of the
    // other servers in the cluster.
    // Reference: section 5.2
    // Also if a candidate fails to obtain a majority, it will time out and
    // start a new election by incrementing its term and initiating another
    // round of RequestVote RPCs.
    // Reference: section 5.2
    public void testNonleaderStartElection(RaftConstantType.StateType state) throws Throwable {

        Integer et = 10;
        RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, et, 1, new MemoryStorage());
        switch (state) {
            case StateFollower:
                r.becomeFollower(1, 2L);
                break;
            case StateCandidate:
                r.becomeCandidate();
                break;
        }
        for (int i = 1; i < 2 * et; i++) {
            r.tickFunc.tick();
        }
        Assert.assertEquals(r.term, 2);
        Assert.assertEquals(r.state, RaftConstantType.StateType.StateCandidate);

        Message[] msgs = TestUtils.readMessages(r);
        Arrays.sort(msgs, new MessageSort());

        Message[] wmsgs = new Message[]{
                Message.newBuilder().setFrom(1).setTo(2).setTerm(2).setType(MessageType.MsgVote).setIndex(0)
                        .setLogTerm(0).setContext(ByteString.EMPTY).build(),
                Message.newBuilder().setFrom(1).setTo(3).setTerm(2).setType(MessageType.MsgVote).setIndex(0)
                        .setLogTerm(0).setContext(ByteString.EMPTY).build()
        };
        Assert.assertArrayEquals(wmsgs, msgs);
    }


    // TestLeaderElectionInOneRoundRPC tests all cases that may happen in
    // leader election during one round of RequestVote RPC:
    // a) it wins the election
    // b) it loses the election
    // c) it is unclear about the result
    // Reference: section 5.2
    @Test
    public void TestLeaderElectionInOneRoundRPC() throws Throwable {
        class TestStruct {
            public int size;
            public Map<Long, Boolean> votes;
            public RaftConstantType.StateType state;

            public TestStruct(int size, Map<Long, Boolean> votes, RaftConstantType.StateType state) {
                this.size = size;
                this.votes = votes;
                this.state = state;
            }
        }

        ArrayList<TestStruct> tests = new ArrayList<>();

        // win the election when receiving votes from a majority of the servers
        tests.add(new TestStruct(1, ImmutableMap.of(), RaftConstantType.StateType.StateLeader));
        tests.add(new TestStruct(3, ImmutableMap.of(2L, true, 3L, true), RaftConstantType.StateType.StateLeader));
        tests.add(new TestStruct(3, ImmutableMap.of(2L, true), RaftConstantType.StateType.StateLeader));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, true, 3L, true, 4L, true, 5L, true), RaftConstantType.StateType.StateLeader));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, true, 3L, true, 4L, true), RaftConstantType.StateType.StateLeader));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, true, 3L, true), RaftConstantType.StateType.StateLeader));

        // return to follower state if it receives vote denial from a majority
        tests.add(new TestStruct(3, ImmutableMap.of(2L, false, 3L, false), RaftConstantType.StateType.StateFollower));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, false, 3L, false, 4L, false, 5L, false), RaftConstantType.StateType.StateFollower));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, true, 3L, false, 4L, false, 5L, false), RaftConstantType.StateType.StateFollower));

        // stay in candidate if it does not obtain the majority
        tests.add(new TestStruct(3, ImmutableMap.of(), RaftConstantType.StateType.StateCandidate));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, true), RaftConstantType.StateType.StateCandidate));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, false, 3L, false), RaftConstantType.StateType.StateCandidate));
        tests.add(new TestStruct(5, ImmutableMap.of(), RaftConstantType.StateType.StateCandidate));

        for (int i = 0; i < tests.size(); i++) {
            TestStruct tt = tests.get(i);
            RaftCore r = TestUtils.newTestRaft(1, TestUtils.idsBySize(tt.size), 10, 1, new MemoryStorage());
            r.step(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgHup).build());
            for (Map.Entry<Long, Boolean> entry : tt.votes.entrySet()) {
                Long id = entry.getKey();
                Boolean vote = entry.getValue();
                r.step(Message.newBuilder().setFrom(id).setTo(1).setType(MessageType.MsgVoteResp).setReject(!vote).build());
            }
            Assert.assertEquals(r.state, tt.state);
            Assert.assertEquals(r.term, 1);
        }
    }


    // TestFollowerVote tests that each follower will vote for at most one
    // candidate in a given term, on a first-come-first-served basis.
    // Reference: section 5.2
    @Test
    public void TestFollowerVote() throws Throwable {
        class TestStruct {
            public long vote;
            public long nvote;
            public boolean wreject;

            public TestStruct(long vote, long nvote, boolean wreject) {
                this.vote = vote;
                this.nvote = nvote;
                this.wreject = wreject;
            }
        }

        ArrayList<TestStruct> tests = new ArrayList<>();
        tests.add(new TestStruct(RaftConstants.None, 1, false));
        tests.add(new TestStruct(RaftConstants.None, 2, false));
        tests.add(new TestStruct(1, 1, false));
        tests.add(new TestStruct(2, 2, false));
        tests.add(new TestStruct(1, 2, true));
        tests.add(new TestStruct(2, 1, true));


        for (int i = 0; i < tests.size(); i++) {
            TestStruct tt = tests.get(i);
            RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, new MemoryStorage());

            r.loadState(HardState.newBuilder().setTerm(1).setVote(tt.vote).build());

            r.step(Message.newBuilder().setFrom(tt.nvote).setTo(1).setTerm(1).setType(MessageType.MsgVote).build());

            Message[] msgs = TestUtils.readMessages(r);

            Message[] wmsgs = new Message[]{
                    Message.newBuilder().setFrom(1).setTo(tt.nvote).setTerm(1).setType(MessageType.MsgVoteResp).setReject(tt.wreject).build()
            };

            Assert.assertEquals(wmsgs[0].getFrom(), msgs[0].getFrom());
            Assert.assertEquals(wmsgs[0].getTo(), msgs[0].getTo());
            Assert.assertEquals(wmsgs[0].getTerm(), msgs[0].getTerm());
            Assert.assertEquals(wmsgs[0].getType(), msgs[0].getType());
            Assert.assertEquals(wmsgs[0].getReject(), msgs[0].getReject());
        }
    }


    // TestCandidateFallback tests that while waiting for votes,
    // if a candidate receives an AppendEntries RPC from another server claiming
    // to be leader whose term is at least as large as the candidate's current term,
    // it recognizes the leader as legitimate and returns to follower state.
    // Reference: section 5.2
    @Test
    public void TestCandidateFallback() throws Throwable {
        Message[] tests = new Message[]{
                Message.newBuilder().setFrom(2).setTo(1).setTerm(1).setType(MessageType.MsgApp).build(),
                Message.newBuilder().setFrom(2).setTo(1).setTerm(2).setType(MessageType.MsgApp).build()
        };

        for (int i = 0; i < tests.length; i++) {
            Message tt = tests[i];

            RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, new MemoryStorage());
            r.step(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgHup).build());
            Assert.assertEquals(RaftConstantType.StateType.StateCandidate, r.state);

            r.step(tt);
            Assert.assertEquals(RaftConstantType.StateType.StateFollower, r.state);
            Assert.assertEquals(tt.getTerm(), r.term);
        }
    }


    @Test
    public void TestFollowerElectionTimeoutRandomized() throws Throwable {
        testNonleaderElectionTimeoutRandomized(RaftConstantType.StateType.StateFollower);
    }


    @Test
    public void TestCandidateElectionTimeoutRandomized() throws Throwable {
        testNonleaderElectionTimeoutRandomized(RaftConstantType.StateType.StateCandidate);
    }


    // testNonleaderElectionTimeoutRandomized tests that election timeout for
    // follower or candidate is randomized.
    // Reference: section 5.2
    public void testNonleaderElectionTimeoutRandomized(RaftConstantType.StateType state) throws Throwable {
        int et = 10;
        RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, et, 1, new MemoryStorage());
        Map<Integer, Boolean> timeouts = new HashMap<>();

        for (int round = 0; round < 50 * et; round++) {

            switch (state) {
                case StateFollower:
                    r.becomeFollower(r.term + 1, 2L);
                    break;
                case StateCandidate:
                    r.becomeCandidate();
                    break;
            }

            int time = 0;
            Message[] mgs = TestUtils.readMessages(r);
            while (null == mgs || mgs.length == 0) {
                r.tickFunc.tick();
                time++;
                mgs = TestUtils.readMessages(r);
            }
            timeouts.put(time, true);
        }
        for (int d = et + 1; d < 2 * et; d++) {
            Assert.assertEquals(String.format("timeout in %d ticks should happen", d), true, timeouts.get(d));
        }

    }

    // testNonleadersElectionTimeoutNonconflict tests that in most cases only a
    // single server(follower or candidate) will time out, which reduces the
    // likelihood of split vote in the new election.
    // Reference: section 5.2

    // TODO 暂时不测试
    public void testNonleadersElectionTimeoutNonconflict(RaftConstantType.StateType stateType) throws Throwable {

        // TODO
    }

    // TestLeaderStartReplication tests that when receiving client proposals,
    // the leader appends the proposal to its log as a new entry, then issues
    // AppendEntries RPCs in parallel to each of the other servers to replicate
    // the entry. Also, when sending an AppendEntries RPC, the leader includes
    // the index and term of the entry in its log that immediately precedes
    // the new entries.
    // Also, it writes the new entry into stable storage.
    // Reference: section 5.3
    @Test
    public void TestLeaderStartReplication() throws Throwable {
        MemoryStorage s = new MemoryStorage();
        RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        commitNoopEntry(r, s);
        int li = r.raftLog.lastIndex();
        r.step(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgProp)
                .addAllEntries(Arrays.asList(new Entry[]{Entry.newBuilder().setData(ByteString.copyFromUtf8("some data")).build()}))
                .build());

        Assert.assertEquals(li + 1, r.raftLog.lastIndex());
        Assert.assertEquals(li, r.raftLog.getCommitted());

        Message[] msgs = TestUtils.readMessages(r);
        Arrays.sort(msgs, new MessageSort());

        Entry[] wents = new Entry[]{Entry.newBuilder().setIndex(li + 1).setTerm(1).setData(ByteString.copyFromUtf8("some data")).build()};

        Message[] wmsgs = new Message[]{
                Message.newBuilder().setFrom(1).setTo(2).setTerm(1).setType(MessageType.MsgApp).setIndex(li).setLogTerm(1)
                        .addAllEntries(Arrays.asList(wents)).setCommit(li).build(),
                Message.newBuilder().setFrom(1).setTo(3).setTerm(1).setType(MessageType.MsgApp).setIndex(li).setLogTerm(1)
                        .addAllEntries(Arrays.asList(wents)).setCommit(li).build()
        };

        Assert.assertArrayEquals(wmsgs, msgs);
        Assert.assertArrayEquals(wents, r.raftLog.unstableEntries());
    }


    // TestLeaderCommitEntry tests that when the entry has been safely replicated,
    // the leader gives out the applied entries, which can be applied to its state
    // machine.
    // Also, the leader keeps track of the highest index it knows to be committed,
    // and it includes that index in future AppendEntries RPCs so that the other
    // servers eventually find out.
    // Reference: section 5.3
    @Test
    public void TestLeaderCommitEntry() throws Throwable {

        MemoryStorage s = new MemoryStorage();
        RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, s);
        r.becomeCandidate();
        r.becomeLeader();
        commitNoopEntry(r, s);
        int li = r.raftLog.lastIndex();
        r.step(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgProp)
                .addAllEntries(Arrays.asList(new Entry[]{Entry.newBuilder().setData(ByteString.copyFromUtf8("some data")).build()}))
                .build());

        for (Message m : TestUtils.readMessages(r)) {
            r.step(acceptAndReply(m));
        }

        Assert.assertEquals(li + 1, r.raftLog.getCommitted());
        Entry[] wents = new Entry[]{Entry.newBuilder().setIndex(li + 1).setTerm(1).setData(ByteString.copyFromUtf8("some data")).build()};
        Assert.assertArrayEquals(wents, r.raftLog.nextEnts());

        Message[] msgs = TestUtils.readMessages(r);
        Arrays.sort(msgs, new MessageSort());

        for (int i = 0; i < msgs.length; i++) {
            Message m = msgs[i];
            int w = i + 2;
            Assert.assertEquals(w, m.getTo());
            Assert.assertEquals(MessageType.MsgApp, m.getType());
            Assert.assertEquals(li + 1, m.getCommit());
        }
    }


    // TestLeaderAcknowledgeCommit tests that a log entry is committed once the
    // leader that created the entry has replicated it on a majority of the servers.
    // Reference: section 5.3
    @Test
    public void TestLeaderAcknowledgeCommit() throws Throwable {

        class TestStruct {
            int size;
            Map<Long, Boolean> acceptors;
            boolean wack;

            public TestStruct(int size, Map<Long, Boolean> acceptors, boolean wack) {
                this.size = size;
                this.acceptors = acceptors;
                this.wack = wack;
            }
        }
        List<TestStruct> tests = new ArrayList<>();
        tests.add(new TestStruct(1, ImmutableMap.of(), true));
        tests.add(new TestStruct(3, ImmutableMap.of(), false));
        tests.add(new TestStruct(3, ImmutableMap.of(2L, true), true));
        tests.add(new TestStruct(3, ImmutableMap.of(2L, true, 3L, true), true));
        tests.add(new TestStruct(5, ImmutableMap.of(), false));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, true), false));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, true, 3L, true), true));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, true, 3L, true, 4L, true), true));
        tests.add(new TestStruct(5, ImmutableMap.of(2L, true, 3L, true, 4L, true, 5L, true), true));


        for (int i = 0; i < tests.size(); i++) {
            TestStruct tt = tests.get(i);
            MemoryStorage s = new MemoryStorage();
            RaftCore r = TestUtils.newTestRaft(1, TestUtils.idsBySize(tt.size), 10, 1, s);
            r.becomeCandidate();
            r.becomeLeader();
            commitNoopEntry(r, s);
            Integer li = r.raftLog.lastIndex();
            r.step(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgProp)
                    .addAllEntries(Arrays.asList(new Entry[]{Entry.newBuilder().setData(ByteString.copyFromUtf8("some data")).build()}))
                    .build());

            for (Message m : TestUtils.readMessages(r)) {
                if (tt.acceptors.containsKey(m.getTo()) && tt.acceptors.get(m.getTo())) {
                    r.step(acceptAndReply(m));
                }
            }
            Assert.assertEquals(tt.wack, r.raftLog.getCommitted() > li);
        }
    }

    // TestLeaderCommitPrecedingEntries tests that when leader commits a log entry,
    // it also commits all preceding entries in the leader’s log, including
    // entries created by previous leaders.
    // Also, it applies the entry to its local state machine (in log order).
    // Reference: section 5.3
    @Test
    public void TestLeaderCommitPrecedingEntries() throws Throwable {
        Entry[][] tests = new Entry[][]{
                new Entry[]{},
                {Entry.newBuilder().setTerm(2).setIndex(1).build()},
                {Entry.newBuilder().setTerm(1).setIndex(1).build(), Entry.newBuilder().setTerm(2).setIndex(2).build()},
                {Entry.newBuilder().setTerm(1).setIndex(1).build()},
        };

        for (int i = 0; i < tests.length; i++) {
            Entry[] tt = tests[i];
            MemoryStorage storage = new MemoryStorage();
            storage.append(tt);
            RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, storage);
            r.loadState(HardState.newBuilder().setTerm(2).build());
            r.becomeCandidate();
            r.becomeLeader();
            r.step(Message.newBuilder().setFrom(1).setTo(1).setType(MessageType.MsgProp)
                    .addAllEntries(Arrays.asList(new Entry[]{Entry.newBuilder().setData(ByteString.copyFromUtf8("some data")).build()})).build());

            for (Message m : TestUtils.readMessages(r)) {
                r.step(acceptAndReply(m));
            }
            Integer li = tt.length;
            Entry[] wents = ArrayUtils.addAll(tt, Entry.newBuilder().setTerm(3).setIndex(li + 1).setData(ByteString.EMPTY).build()
                    , Entry.newBuilder().setTerm(3).setIndex(li + 2).setData(ByteString.copyFromUtf8("some data")).build());

            Assert.assertArrayEquals(wents, r.raftLog.nextEnts());
        }
    }


    // TestFollowerCommitEntry tests that once a follower learns that a log entry
    // is committed, it applies the entry to its local state machine (in log order).
    // Reference: section 5.3
    @Test
    public void TestFollowerCommitEntry() throws Throwable {

        class TestsStruct {
            Entry[] ents;
            int commit;

            public TestsStruct(Entry[] ents, int commit) {
                this.ents = ents;
                this.commit = commit;
            }
        }

        List<TestsStruct> tests = new ArrayList<>();
        tests.add(new TestsStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).setData(ByteString.copyFromUtf8("some data")).build()}, 1));

        tests.add(new TestsStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).setData(ByteString.copyFromUtf8("some data")).build()
                , Entry.newBuilder().setTerm(1).setIndex(2).setData(ByteString.copyFromUtf8("some data2")).build()}, 2));

        tests.add(new TestsStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).setData(ByteString.copyFromUtf8("some data2")).build()
                , Entry.newBuilder().setTerm(1).setIndex(2).setData(ByteString.copyFromUtf8("some data")).build()}, 2));

        tests.add(new TestsStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).setData(ByteString.copyFromUtf8("some data")).build()
                , Entry.newBuilder().setTerm(1).setIndex(2).setData(ByteString.copyFromUtf8("some data2")).build()}, 1));


        for (int i = 0; i < tests.size(); i++) {

            TestsStruct tt = tests.get(i);
            RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, new MemoryStorage());
            r.becomeFollower(1, 2L);

            r.step(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgApp).setTerm(1)
                    .addAllEntries(Arrays.asList(tt.ents)).setCommit(tt.commit).build());

            Assert.assertEquals(tt.commit, r.raftLog.getCommitted());
            Entry[] wents = ArrayUtils.subarray(tt.ents, 0, tt.commit);
            Assert.assertArrayEquals(wents, r.raftLog.nextEnts());
        }
    }

    // TestFollowerCheckMsgApp tests that if the follower does not find an
    // entry in its log with the same index and term as the one in AppendEntries RPC,
    // then it refuses the new entries. Otherwise it replies that it accepts the
    // append entries.
    // Reference: section 5.3
    @Test
    public void TestFollowerCheckMsgApp() throws Throwable {

        Entry[] ents = new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build(), Entry.newBuilder().setTerm(2).setIndex(2).build()};

        class TestStruct {
            int term;
            int index;
            int windex;
            boolean wreject;
            int wrejectHint;

            public TestStruct(int term, int index, int windex, boolean wreject, int wrejectHint) {
                this.term = term;
                this.index = index;
                this.windex = windex;
                this.wreject = wreject;
                this.wrejectHint = wrejectHint;
            }
        }

        List<TestStruct> tests = new ArrayList<>();
        // match with committed entries
        tests.add(new TestStruct(0, 0, 1, false, 0));
        tests.add(new TestStruct(ents[0].getTerm(), ents[0].getIndex(), 1, false, 0));
        // match with uncommitted entries
        tests.add(new TestStruct(ents[1].getTerm(), ents[1].getIndex(), 2, false, 0));
        // unmatch with existing entry
        tests.add(new TestStruct(ents[0].getTerm(), ents[1].getIndex(), ents[1].getIndex(), true, 2));
        // unexisting entry
        tests.add(new TestStruct(ents[1].getTerm() + 1, ents[1].getIndex() + 1, ents[1].getIndex() + 1, true, 2));


        for (TestStruct tt : tests) {
            MemoryStorage storage = new MemoryStorage();
            storage.append(ents);
            RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, storage);
            r.loadState(HardState.newBuilder().setCommit(1).build());
            r.becomeFollower(2, 2L);

            r.step(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgApp).setTerm(2).setLogTerm(tt.term).setIndex(tt.index).build());
            Message[] msgs = TestUtils.readMessages(r);
            Message[] wmsgs;
            if (tt.wreject) {
                wmsgs = new Message[]{Message.newBuilder().setFrom(1).setTo(2).setType(MessageType.MsgAppResp)
                        .setTerm(2).setIndex(tt.windex).setReject(tt.wreject).setRejectHint(tt.wrejectHint).build()};
            } else {
                wmsgs = new Message[]{Message.newBuilder().setFrom(1).setTo(2).setType(MessageType.MsgAppResp)
                        .setTerm(2).setIndex(tt.windex).build()};
            }
            Assert.assertArrayEquals(wmsgs, msgs);
        }
    }


    // TestFollowerAppendEntries tests that when AppendEntries RPC is valid,
    // the follower will delete the existing conflict entry and all that follow it,
    // and append any new entries not already in the log.
    // Also, it writes the new entry into stable storage.
    // Reference: section 5.3
    @Test
    public void TestFollowerAppendEntries() throws Throwable {

        class TestStruct {
            int index;
            int term;
            Entry[] ents;
            Entry[] wents;
            Entry[] wunstable;

            public TestStruct(int index, int term, Entry[] ents, Entry[] wents, Entry[] wunstable) {
                this.index = index;
                this.term = term;
                this.ents = ents;
                this.wents = wents;
                this.wunstable = wunstable;
            }
        }


        List<TestStruct> tests = new ArrayList<>();

        tests.add(new TestStruct(2, 2,
                        new Entry[]{Entry.newBuilder().setTerm(3).setIndex(3).build()},
                        new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build(), Entry.newBuilder().setTerm(2).setIndex(2).build(), Entry.newBuilder().setTerm(3).setIndex(3).build()},
                        new Entry[]{Entry.newBuilder().setTerm(3).setIndex(3).build()}
                )
        );

        tests.add(new TestStruct(1, 1,
                        new Entry[]{Entry.newBuilder().setTerm(3).setIndex(2).build(), Entry.newBuilder().setTerm(4).setIndex(3).build()},
                        new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build(), Entry.newBuilder().setTerm(3).setIndex(2).build(), Entry.newBuilder().setTerm(4).setIndex(3).build()},
                        new Entry[]{Entry.newBuilder().setTerm(3).setIndex(2).build(), Entry.newBuilder().setTerm(4).setIndex(3).build()}
                )
        );

        tests.add(new TestStruct(0, 0,
                        new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build()},
                        new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build(), Entry.newBuilder().setTerm(2).setIndex(2).build()},
                        null
                )
        );

        tests.add(new TestStruct(0, 0,
                        new Entry[]{Entry.newBuilder().setTerm(3).setIndex(1).build()},
                        new Entry[]{Entry.newBuilder().setTerm(3).setIndex(1).build()},
                        new Entry[]{Entry.newBuilder().setTerm(3).setIndex(1).build()}
                )
        );


        for (TestStruct tt : tests) {
            MemoryStorage storage = new MemoryStorage();
            storage.append(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build(), Entry.newBuilder().setTerm(2).setIndex(2).build()});
            RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, storage);
            r.becomeFollower(2, 2L);

            r.step(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgApp).setTerm(2).setLogTerm(tt.term).setIndex(tt.index)
                    .addAllEntries(Arrays.asList(tt.ents)).build()
            );

            Assert.assertArrayEquals(tt.wents, r.raftLog.allEntries());
            Assert.assertArrayEquals(tt.wunstable, r.raftLog.unstableEntries());
        }
    }

    // TestLeaderSyncFollowerLog tests that the leader could bring a follower's log
    // into consistency with its own.
    // Reference: section 5.3, figure 7
    public void TestLeaderSyncFollowerLog() throws Throwable {
        Entry[] ents = new Entry[]{
                Entry.newBuilder().build(),
                Entry.newBuilder().setTerm(1).setIndex(1).build(),
                Entry.newBuilder().setTerm(1).setIndex(2).build(),
                Entry.newBuilder().setTerm(1).setIndex(3).build(),

                Entry.newBuilder().setTerm(4).setIndex(4).build(), Entry.newBuilder().setTerm(4).setIndex(5).build(),
                Entry.newBuilder().setTerm(5).setIndex(6).build(), Entry.newBuilder().setTerm(5).setIndex(7).build(),

                Entry.newBuilder().setTerm(6).setIndex(8).build(),
                Entry.newBuilder().setTerm(6).setIndex(9).build(),
                Entry.newBuilder().setTerm(6).setIndex(10).build()
        };

        Integer term = 8;

        Entry[][] tests = new Entry[][]{
                new Entry[]{
                        Entry.newBuilder().build(),
                        Entry.newBuilder().setTerm(1).setIndex(1).build(),
                        Entry.newBuilder().setTerm(1).setIndex(2).build(),
                        Entry.newBuilder().setTerm(1).setIndex(3).build(),

                        Entry.newBuilder().setTerm(4).setIndex(4).build(),
                        Entry.newBuilder().setTerm(4).setIndex(5).build(),
                        Entry.newBuilder().setTerm(5).setIndex(6).build(),
                        Entry.newBuilder().setTerm(5).setIndex(7).build(),

                        Entry.newBuilder().setTerm(6).setIndex(8).build(),
                        Entry.newBuilder().setTerm(6).setIndex(9).build()
                },

                new Entry[]{
                        Entry.newBuilder().build(),
                        Entry.newBuilder().setTerm(1).setIndex(1).build(),
                        Entry.newBuilder().setTerm(1).setIndex(2).build(),
                        Entry.newBuilder().setTerm(1).setIndex(3).build(),
                        Entry.newBuilder().setTerm(4).setIndex(4).build()
                },

                new Entry[]{
                        Entry.newBuilder().build(),
                        Entry.newBuilder().setTerm(1).setIndex(1).build(),
                        Entry.newBuilder().setTerm(1).setIndex(2).build(),
                        Entry.newBuilder().setTerm(1).setIndex(3).build(),

                        Entry.newBuilder().setTerm(4).setIndex(4).build(),
                        Entry.newBuilder().setTerm(4).setIndex(5).build(),
                        Entry.newBuilder().setTerm(5).setIndex(6).build(),
                        Entry.newBuilder().setTerm(5).setIndex(7).build(),

                        Entry.newBuilder().setTerm(6).setIndex(8).build(),
                        Entry.newBuilder().setTerm(6).setIndex(9).build(),
                        Entry.newBuilder().setTerm(6).setIndex(10).build(),
                        Entry.newBuilder().setTerm(6).setIndex(11).build()
                },

                new Entry[]{
                        Entry.newBuilder().build(),
                        Entry.newBuilder().setTerm(1).setIndex(1).build(),
                        Entry.newBuilder().setTerm(1).setIndex(2).build(),
                        Entry.newBuilder().setTerm(1).setIndex(3).build(),

                        Entry.newBuilder().setTerm(4).setIndex(4).build(),
                        Entry.newBuilder().setTerm(4).setIndex(5).build(),
                        Entry.newBuilder().setTerm(5).setIndex(6).build(),
                        Entry.newBuilder().setTerm(5).setIndex(7).build(),

                        Entry.newBuilder().setTerm(6).setIndex(8).build(),
                        Entry.newBuilder().setTerm(6).setIndex(9).build(),
                        Entry.newBuilder().setTerm(6).setIndex(10).build(),
                        Entry.newBuilder().setTerm(7).setIndex(11).build(),
                        Entry.newBuilder().setTerm(7).setIndex(12).build()
                },

                new Entry[]{
                        Entry.newBuilder().build(),
                        Entry.newBuilder().setTerm(1).setIndex(1).build(),
                        Entry.newBuilder().setTerm(1).setIndex(2).build(),
                        Entry.newBuilder().setTerm(1).setIndex(3).build(),

                        Entry.newBuilder().setTerm(4).setIndex(4).build(),
                        Entry.newBuilder().setTerm(4).setIndex(5).build(),
                        Entry.newBuilder().setTerm(4).setIndex(6).build(),
                        Entry.newBuilder().setTerm(4).setIndex(7).build()
                },

                new Entry[]{
                        Entry.newBuilder().build(),
                        Entry.newBuilder().setTerm(1).setIndex(1).build(),
                        Entry.newBuilder().setTerm(1).setIndex(2).build(),
                        Entry.newBuilder().setTerm(1).setIndex(3).build(),
                        Entry.newBuilder().setTerm(2).setIndex(4).build(),
                        Entry.newBuilder().setTerm(2).setIndex(5).build(),
                        Entry.newBuilder().setTerm(2).setIndex(6).build(),
                        Entry.newBuilder().setTerm(3).setIndex(7).build(),
                        Entry.newBuilder().setTerm(3).setIndex(8).build(),
                        Entry.newBuilder().setTerm(3).setIndex(9).build(),
                        Entry.newBuilder().setTerm(3).setIndex(10).build(),
                        Entry.newBuilder().setTerm(3).setIndex(11).build()
                },
        };

        for (Entry[] tt : tests) {

            MemoryStorage leadStorage = new MemoryStorage();
            leadStorage.append(ents);

            RaftCore lead = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, leadStorage);
            lead.loadState(HardState.newBuilder().setCommit(lead.raftLog.lastIndex()).setTerm(term).build());

            MemoryStorage followerStorage = new MemoryStorage();
            followerStorage.append(tt);

            RaftCore follower = TestUtils.newTestRaft(2, new Long[]{1L, 2L, 3L}, 10, 1, followerStorage);
            follower.loadState(HardState.newBuilder().setTerm(term - 1).build());


            // TODO  测试network
        }

    }

    // TestVoteRequest tests that the vote request includes information about the candidate’s log
    // and are sent to all of the other nodes.
    // Reference: section 5.4.1
    @Test
    public void TestVoteRequest() throws Throwable {
        class TestStruct {
            Entry[] ents;
            int wterm;

            public TestStruct(Entry[] ents, int wterm) {
                this.ents = ents;
                this.wterm = wterm;
            }
        }

        List<TestStruct> tests = new ArrayList<>();
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build()}, 2));
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build(), Entry.newBuilder().setTerm(2).setIndex(2).build()}, 3));

        for (TestStruct tt : tests) {
            RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L, 3L}, 10, 1, new MemoryStorage());
            r.step(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgApp).setTerm(tt.wterm - 1).setLogTerm(0).setIndex(0)
                    .addAllEntries(Arrays.asList(tt.ents)).build());

            TestUtils.readMessages(r);

            for (int i = 1; i < r.electionTimeout * 2; i++) {
                r.tickElection.tick();
            }

            Message[] msgs = TestUtils.readMessages(r);
            Arrays.sort(msgs, new MessageSort());

            Assert.assertEquals(2, msgs.length);

            int i = 0;
            for (Message m : msgs) {
                Assert.assertEquals(MessageType.MsgVote, m.getType());
                Assert.assertEquals(i + 2, m.getTo());
                Assert.assertEquals(tt.wterm, m.getTerm());

                int windex = tt.ents[tt.ents.length - 1].getIndex();
                int wlogterm = tt.ents[tt.ents.length - 1].getTerm();
                Assert.assertEquals(windex, m.getIndex());
                Assert.assertEquals(wlogterm, m.getLogTerm());
                i++;
            }
        }
    }


    // TestVoter tests the voter denies its vote if its own log is more up-to-date
    // than that of the candidate.
    // Reference: section 5.4.1
    @Test
    public void TestVoter() throws Throwable {
        class TestStruct {
            Entry[] ents;
            int logterm;
            int index;
            boolean wreject;

            public TestStruct(Entry[] ents, int logterm, int index, boolean wreject) {
                this.ents = ents;
                this.logterm = logterm;
                this.index = index;
                this.wreject = wreject;
            }
        }

        List<TestStruct> tests = new ArrayList();
        // same logterm
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build()}, 1, 1, false));
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build()}, 1, 2, false));
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build()
                , Entry.newBuilder().setTerm(1).setIndex(2).build()
        }, 1, 1, true));

        // candidate higher logterm
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build()}, 2, 1, false));
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build()}, 2, 2, false));
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(1).setIndex(1).build(),
                Entry.newBuilder().setTerm(1).setIndex(2).build()}, 2, 1, false));

        // voter higher logterm
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(2).setIndex(1).build()}, 1, 1, true));
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(2).setIndex(1).build()}, 1, 2, true));
        tests.add(new TestStruct(new Entry[]{Entry.newBuilder().setTerm(2).setIndex(1).build(),
                Entry.newBuilder().setTerm(1).setIndex(2).build()}, 1, 1, true));


        for (TestStruct tt : tests) {
            MemoryStorage storage = new MemoryStorage();
            storage.append(tt.ents);
            RaftCore r = TestUtils.newTestRaft(1, new Long[]{1L, 2L}, 10, 1, storage);
            r.step(Message.newBuilder().setFrom(2).setTo(1).setType(MessageType.MsgVote).setTerm(3).setLogTerm(tt.logterm).setIndex(tt.index).build());

            Message[] msgs = TestUtils.readMessages(r);
            Assert.assertEquals(1, msgs.length);

            Message m = msgs[0];
            Assert.assertEquals(MessageType.MsgVoteResp, m.getType());
            Assert.assertEquals(tt.wreject, m.getReject());
        }

    }

    // TestLeaderOnlyCommitsLogFromCurrentTerm tests that only log entries from the leader’s
    // current term are committed by counting replicas.
    // Reference: section 5.4.2
    public void TestLeaderOnlyCommitsLogFromCurrentTerm() {
        // TODO
    }

    public void commitNoopEntry(RaftCore r, MemoryStorage s) throws Throwable {
        if (r.state != RaftConstantType.StateType.StateLeader) {
            throw new RuntimeException("it should only be used when it is the leader");
        }
        r.bcastAppend();

        Message[] msgs = TestUtils.readMessages(r);
        if (null != msgs) {
            for (Message m : msgs) {
                if (m.getType() != MessageType.MsgApp || m.getEntriesCount() != 1 || m.getEntries(0).getData() != ByteString.EMPTY) {
                    System.out.println(m.getEntries(0).getData());
                    throw new RuntimeException("not a message to append noop entry");
                }
                r.step(acceptAndReply(m));
            }
        }
        // ignore further messages to refresh followers' commit index
        TestUtils.readMessages(r);
        s.append(r.raftLog.unstableEntries());
        r.raftLog.appliedTo(r.raftLog.getCommitted());
        r.raftLog.stableTo(r.raftLog.lastIndex(), r.raftLog.lastTerm());
    }

    public Message acceptAndReply(Message m) {
        if (m.getType() != MessageType.MsgApp) {
            throw new RuntimeException("type should be MsgApp");
        }
        return Message.newBuilder().setFrom(m.getTo()).setTo(m.getFrom())
                .setTerm(m.getTerm()).setType(MessageType.MsgAppResp).setIndex(m.getIndex() + m.getEntriesCount()).build();

    }

    class MessageSort implements Comparator<Message> {
        @Override
        public int compare(Message o1, Message o2) {
            return o1.toString().compareTo(o2.toString());
        }
    }

    public static void main(String[] args) {

        Message.Builder mb = Message.newBuilder();
        mb.setIndex(1);
        Entry[] ents = new Entry[]{Entry.newBuilder().setTerm(11).build()};

        mb.addAllEntries(Arrays.asList(ents));
        Message m = mb.build();

        Message.Builder nmb = m.toBuilder();

        Message nm = nmb.build();

        System.out.println(nm.getEntriesCount());

    }
}