package com.raft.core.log;

import com.raft.core.RaftConstants;
import com.raft.core.storage.MemoryStorage;
import org.junit.Assert;
import org.junit.Test;
import raftpb.Raft.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by william on 2018/11/27.
 */
public class TestRaftLog {

    @Test
    public void TestAppend() throws Throwable {
        // test struct
        class TestStruct {
            Entry[] ents;
            int windex;
            Entry[] wents;
            int wunstable;

            public TestStruct(Entry[] ents, int windex, Entry[] wents, int wunstable) {
                this.ents = ents;
                this.windex = windex;
                this.wents = wents;
                this.wunstable = wunstable;
            }
        }

        List<TestStruct> testStructs = new ArrayList<>();
        testStructs.add(new TestStruct(
                new Entry[]{},
                2,
                new Entry[]{Entry.newBuilder().setIndex(1).setTerm(1).build(), Entry.newBuilder().setIndex(2).setTerm(2).build()},
                3
        ));
        testStructs.add(new TestStruct(
                new Entry[]{Entry.newBuilder().setIndex(3).setTerm(2).build()},
                3,
                new Entry[]{Entry.newBuilder().setIndex(1).setTerm(1).build(), Entry.newBuilder().setIndex(2).setTerm(2).build(),
                        Entry.newBuilder().setIndex(3).setTerm(2).build()},
                3
        ));
        // conflicts with index 1
        testStructs.add(new TestStruct(
                new Entry[]{Entry.newBuilder().setIndex(1).setTerm(2).build()},
                1,
                new Entry[]{Entry.newBuilder().setIndex(1).setTerm(2).build()},
                1
        ));
        // conflicts with index 2
        testStructs.add(new TestStruct(
                new Entry[]{Entry.newBuilder().setIndex(2).setTerm(3).build(), Entry.newBuilder().setIndex(3).setTerm(3).build()},
                3,
                new Entry[]{Entry.newBuilder().setIndex(1).setTerm(1).build(), Entry.newBuilder().setIndex(2).setTerm(3).build(),
                        Entry.newBuilder().setIndex(3).setTerm(3).build()},
                2
        ));

        Entry previousEnts[] = new Entry[]{
                Entry.newBuilder().setIndex(1).setTerm(1).build(),
                Entry.newBuilder().setIndex(2).setTerm(2).build()
        };

        for (int i = 0; i < testStructs.size(); i++) {
            TestStruct testStruct = testStructs.get(i);
            MemoryStorage storage = new MemoryStorage();
            storage.append(previousEnts);
            RaftLog raftLog = RaftLog.newLog(storage);
            int index = raftLog.append(testStruct.ents);
            Assert.assertEquals(index, testStruct.windex);

            Entry[] g = raftLog.entries(1, RaftConstants.NO_LIMIT);
            Assert.assertArrayEquals(g, testStruct.wents);
            Assert.assertEquals(raftLog.getUnstable().getOffset(), testStruct.wunstable);
        }
    }


}
