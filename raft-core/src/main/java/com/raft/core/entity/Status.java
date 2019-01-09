//package com.distributiontool.raft.core.entity;
//
//import raftpb.Raft;
//
//import java.util.Map;
//
///**
// * Created by william on 2018/11/16.
// */
//
//// status.go
//public class Status {
//
//    private long id;
//
//    private Raft.HardState hardState;
//
//    private SoftState softState;
//
//    private long applied;
//
//    private Map<Integer, Progress> progressMap;
//
//    private long leadTransferee;
//
//
//    public long getId() {
//        return id;
//    }
//
//    public void setId(long id) {
//        this.id = id;
//    }
//
//    public Raft.HardState getHardState() {
//        return hardState;
//    }
//
//    public void setHardState(Raft.HardState hardState) {
//        this.hardState = hardState;
//    }
//
//    public SoftState getSoftState() {
//        return softState;
//    }
//
//    public void setSoftState(SoftState softState) {
//        this.softState = softState;
//    }
//
//    public long getApplied() {
//        return applied;
//    }
//
//    public void setApplied(long applied) {
//        this.applied = applied;
//    }
//
//    public Map<Integer, Progress> getProgressMap() {
//        return progressMap;
//    }
//
//    public void setProgressMap(Map<Integer, Progress> progressMap) {
//        this.progressMap = progressMap;
//    }
//
//    public long getLeadTransferee() {
//        return leadTransferee;
//    }
//
//    public void setLeadTransferee(long leadTransferee) {
//        this.leadTransferee = leadTransferee;
//    }
//}
