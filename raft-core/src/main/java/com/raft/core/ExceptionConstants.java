package com.raft.core;

/**
 * Created by william on 2018/11/27.
 */
public class ExceptionConstants {

    public static Exception ErrCompacted = new RuntimeException("requested index is unavailable due to compaction");

    public static Exception ErrSnapOutOfDate = new RuntimeException("requested index is older than the existing snapshot");

    public static Exception ErrUnavailable = new RuntimeException("requested entry at index is unavailable");

    public static Exception ErrSnapshotTemporarilyUnavailable = new RuntimeException("snapshot is temporarily unavailable");


}
