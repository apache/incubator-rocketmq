# Dledger快速搭建
---
### 前言
该文档主要介绍如何快速构建和部署基于 DLedger 的可以自动容灾切换的 RocketMQ 集群。

详细的新集群部署和旧集群升级指南请参考 [部署指南](deploy_guide.md)。

### 1. 源码构建
构建分为两个部分，需要先构建 DLedger，然后 构建 RocketMQ

#### 1.1 构建 DLedger

`git clone https://github.com/openmessaging/openmessaging-storage-dledger.git`

`cd openmessaging-storage-dledger`

`mvn clean install -DskipTests`

#### 1.2 构建 RocketMQ

`git clone https://github.com/apache/rocketmq.git`

`cd rocketmq`

`git checkout -b store_with_dledger origin/store_with_dledger`

`mvn -Prelease-all -DskipTests clean install -U`

### 2. 快速部署

在构建成功后

`cd distribution/target/apache-rocketmq`

`sh bin/dledger/fast-try.sh start`

如果上面的步骤执行成功，可以通过 mqadmin 运维命令查看集群状态。

`sh bin/mqadmin clusterList -n 127.0.0.1:9876`

顺利的话，会看到如下内容：

![ClusterList](https://img.alicdn.com/5476e8b07b923/TB11Z.ZyCzqK1RjSZFLXXcn2XXa)

（BID 为 0 的表示 Master，其余都是 Follower）

启动成功，现在可以向集群收发消息，并进行容灾切换测试了。

关闭快速集群，可以执行：

`sh bin/dledger/fast-try.sh stop`

快速部署，默认配置在 conf/dledger 里面，默认的存储路径在 /tmp/rmqstore。


### 3. 容灾切换

部署成功，杀掉 Leader 之后（在上面的例子中，杀掉端口 30931 所在的进程），等待约 10s 左右，用 clusterList 命令查看集群，就会发现 Leader 切换到另一个节点了。





