package com.raft.core;

import org.apache.commons.lang3.ArrayUtils;
import raftpb.Raft.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by william on 2018/11/30.
 */

// read_only.go
public class ReadOnly {

    public class ReadIndexStatus {
        // 请求
        private Message req;
        // 收到 read only 请求时候的commit信息
        private Integer index;
        // 记录收到 followers的 响应信息
        private HashMap<Long, Object> acks = new HashMap<>();

        public ReadIndexStatus(Message req, Integer index, HashMap<Long, Object> acks) {
            this.req = req;
            this.index = index;
            this.acks = acks;
        }

        public Message getReq() {
            return req;
        }

        public void setReq(Message req) {
            this.req = req;
        }

        public Integer getIndex() {
            return index;
        }

        public void setIndex(Integer index) {
            this.index = index;
        }

        public HashMap<Long, Object> getAcks() {
            return acks;
        }

        public void setAcks(HashMap<Long, Object> acks) {
            this.acks = acks;
        }
    }

    private RaftConstantType.ReadOnlyOption option;


    // key = ctx ,value =
    private Map<String, ReadIndexStatus> pendingReadIndex;

    // string {ctx1,ctx2 ...  }
    private String[] readIndexQueue = new String[0];

    public ReadOnly(RaftConstantType.ReadOnlyOption option, Map<String, ReadIndexStatus> pendingReadIndex, String[] readIndexQueue) {
        this.option = option;
        this.pendingReadIndex = pendingReadIndex;
        this.readIndexQueue = readIndexQueue;
    }

    public RaftConstantType.ReadOnlyOption getOption() {
        return option;
    }

    public void setOption(RaftConstantType.ReadOnlyOption option) {
        this.option = option;
    }

    public Map<String, ReadIndexStatus> getPendingReadIndex() {
        return pendingReadIndex;
    }

    public void setPendingReadIndex(Map<String, ReadIndexStatus> pendingReadIndex) {
        this.pendingReadIndex = pendingReadIndex;
    }

    public String[] getReadIndexQueue() {
        return readIndexQueue;
    }

    public void setReadIndexQueue(String[] readIndexQueue) {
        this.readIndexQueue = readIndexQueue;
    }

    public static ReadOnly newReadOnly(RaftConstantType.ReadOnlyOption readOnlyOption) {
        return new ReadOnly(readOnlyOption, new HashMap<>(), new String[0]);
    }

    // addRequest adds a read only request into readonly struct.
    // `index` is the commit index of the raft state machine when it received
    // the read only request.
    // `m` is the original read only request message from the local or remote node.
    public void addRequest(Integer index, Message m) {

        String ctx = m.getEntries(0).getData().toString();
        if (pendingReadIndex.containsKey(ctx)) {
            return;
        }

        ReadIndexStatus readIndexStatus = new ReadIndexStatus(m, index, new HashMap<>());
        pendingReadIndex.put(ctx, readIndexStatus);

        readIndexQueue = ArrayUtils.addAll(readIndexQueue, ctx);
    }


    // recvAck notifies the readonly struct that the raft state machine received
    // an acknowledgment of the heartbeat that attached with the read only request context.
    public Integer recvAck(Message m) {

        String ctx = m.getContext().toString();
        if (!pendingReadIndex.containsKey(ctx)) {
            return 0;
        }

        ReadIndexStatus rs = pendingReadIndex.get(ctx);
        rs.acks.put(m.getFrom(), null);

        // add one to include an ack from local node
        return rs.acks.size() + 1;
    }


    // advance advances the read only request queue kept by the readonly struct.
    // It dequeues the requests until it finds the read only request that has
    // the same context as the given `m`.
    public ReadIndexStatus[] advance(Message m) throws Throwable {

        int i = 0;
        boolean found = false;

        String ctx = m.getContext().toString();
        ReadIndexStatus[] rss = new ReadIndexStatus[0];

        for (String okctx : readIndexQueue) {
            i++;
            if (!pendingReadIndex.containsKey(okctx)) {
                throw new RuntimeException("cannot find corresponding read state from pending map");
            }

            ReadIndexStatus rs = pendingReadIndex.get(okctx);
            rss = ArrayUtils.add(rss, rs);

            if (okctx == ctx) {
                found = true;
                break;
            }
        }

        if (found) {
            readIndexQueue = ArrayUtils.subarray(readIndexQueue, i, readIndexQueue.length);
            for (ReadIndexStatus rs : rss) {
                // 删除rss收集的数据
                pendingReadIndex.remove(rs.req.getEntries(0).getData().toString());
            }
            return rss;
        }
        return null;
    }

    // lastPendingRequestCtx returns the context of the last pending read only
    // request in readonly struct.
    public String lastPendingRequestCtx() {
        if (readIndexQueue.length == 0) {
            return "";
        }
        return readIndexQueue[readIndexQueue.length - 1];
    }

}


// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
class ReadState {
    private int index;
    private byte[] requestCtx;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public byte[] getRequestCtx() {
        return requestCtx;
    }

    public void setRequestCtx(byte[] requestCtx) {
        this.requestCtx = requestCtx;
    }

    public ReadState(int index, byte[] requestCtx) {
        this.index = index;
        this.requestCtx = requestCtx;
    }

    public ReadState() {
    }
}