# Raft in Java

重写了etcd golang 的代码  [Raft](http://github.com/etcd-io/etcd/tree/master/raft/) 

# Current status

目前只是功能上的实现 Raft的核心协议，并不包含网络传输等， 测试用例覆盖的是 raft_paper 的用例 [TestRaftPaper](https://github.com/3h-william/raft/blob/master/raft-core/src/test/java/com/raft/core/TestRaftPaper.java)