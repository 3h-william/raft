package com.raft.core.log;

import org.junit.Assert;
import raftpb.Raft.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * Created by william on 2018/11/23.
 */
public class TestUnstable {


    @Test
    public void TestUnstableMaybeFirstIndex() {

        //Entry entry = Entry.newBuilder().

        class TestStruct {
            Entry[] entries;
            int offset;
            Snapshot snapshot;
            boolean wok;
            Integer windex;

            public TestStruct(Entry[] entries, int offset, Snapshot snapshot, boolean wok, Integer windex) {
                this.entries = entries;
                this.offset = offset;
                this.snapshot = snapshot;
                this.wok = wok;
                this.windex = windex;
            }
        }


        List<TestStruct> testStructs = new ArrayList<>();
        // no snapshot
        testStructs.add(new TestStruct(
                new Entry[]{Entry.newBuilder().setIndex(5).setTerm(1).build()},
                5,
                null,
                false,
                0
        ));
        testStructs.add(new TestStruct(
                new Entry[]{},
                0,
                null,
                false,
                0
        ));
        // has snapshot
        testStructs.add(new TestStruct(
                new Entry[]{Entry.newBuilder().setIndex(5).setTerm(1).build()},
                5,
                Snapshot.newBuilder().setMetadata(SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(),
                true,
                5
        ));
        testStructs.add(new TestStruct(
                new Entry[]{},
                5,
                Snapshot.newBuilder().setMetadata(SnapshotMetadata.newBuilder().setIndex(4).setTerm(1).build()).build(),
                true,
                5
        ));


        for (int i = 0; i < testStructs.size(); i++) {

            TestStruct testStruct = testStructs.get(i);

            Unstable unstable = new Unstable();
            unstable.setEntries(testStruct.entries);
            unstable.setOffset(testStruct.offset);
            unstable.setSnapshot(testStruct.snapshot);

            Integer index = unstable.maybeFirstIndex();

            boolean ok;

            if (0 == index) ok = false;
            else ok = true;

            Assert.assertEquals(ok , testStruct.wok);

            Assert.assertEquals(index ,testStruct.windex);

        }

    }


    public static void main(String[] args) {


    }
}
