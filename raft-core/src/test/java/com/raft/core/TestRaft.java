package com.raft.core;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by william on 2018/12/18.
 */
public class TestRaft {

    @Test
    public void TestProgressBecomeProbe() {
        int match = 1;
        class TestStruct {
            Progress p;
            int wnext;

            public TestStruct(Progress p, int wnext) {
                this.p = p;
                this.wnext = wnext;
            }
        }
        List<TestStruct> testStructs = new ArrayList<>();
        Progress p1 = new Progress();
        p1.setState(RaftConstantType.ProgressStateType.ProgressStateReplicate);
        p1.setMatch(match);
        p1.setNext(5);
        p1.setIns(Progress.InFlights.newInflights(256));
        testStructs.add(new TestStruct(p1, 2));

        Progress p2 = new Progress();
        p2.setState(RaftConstantType.ProgressStateType.ProgressStateSnapshot);
        p2.setMatch(match);
        p2.setNext(5);
        p2.setPendingSnapshot(10);
        p2.setIns(Progress.InFlights.newInflights(256));
        testStructs.add(new TestStruct(p2, 11));

        Progress p3 = new Progress();
        p3.setState(RaftConstantType.ProgressStateType.ProgressStateSnapshot);
        p3.setMatch(match);
        p3.setNext(5);
        p3.setPendingSnapshot(0);
        p3.setIns(Progress.InFlights.newInflights(256));
        testStructs.add(new TestStruct(p3, 2));


        for (int i = 0; i < testStructs.size(); i++) {
            TestStruct tt = testStructs.get(i);
            tt.p.becomeProbe();
            Assert.assertEquals(tt.p.getState(), RaftConstantType.ProgressStateType.ProgressStateProbe);
            Assert.assertEquals(tt.p.getMatch(), match);
            Assert.assertEquals(tt.p.getNext(), tt.wnext);
        }

    }


}
