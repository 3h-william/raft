# Raft in Java

重写了etcd golang 的代码  [Raft](http://github.com/etcd-io/etcd/tree/master/raft/) 

# Current status

目前只是功能上的实现 Raft的核心协议功能，并不包含网络传输等， 测试用例覆盖的是 raft_paper 的用例 [TestRaftPaper]