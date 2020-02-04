
RocketMQ message queue cluster mainly includes four roles: NameServer, Broker (Master/Slave), Producer and Consumer. The basic communication process is as follows:
(1) After Broker start-up, it needs to complete one operation: register itself to NameServer, and then report Topic routing information to NameServer at regular intervals of 30 seconds.
(2) When message Producer sends a message as a client, it needs to obtain routing information from the local cache TopicPublishInfoTable according to the Topic of the message. If not, it will be retrieved from NameServer and update to local cache, at the same time, Producer will retrieve routing information from NameServer every 30 seconds by default.
(3) Message Producer chooses a queue to send the message according to the routing information obtained in 2); Broker receives the message and records it in disk as the receiver of the message.
(4) After message Consumer gets the routing information according to 2) and complete the load balancing of the client, then select one or several message queues to pull messages and consume them.

From 1) ~ 3) above, we can see that both Producer, Broker and NameServer communicate with each other(only part of MQ communication is mentioned here), so how to design a good network communication module is very important in MQ. It will determine the overall messaging capability and final performance of the RocketMQ cluster.

rocketmq-remoting module is the module responsible for network communication in RocketMQ message queue. It is relied on and referenced by almost all other modules (such as rocketmq-client,rocketmq-broker,rocketmq-namesrv) that need network communication. In order to realize the efficient data request and reception between the client and the server, the RocketMQ message queue defines the communication protocol and extends the communication module on the basis of Netty.

### 1 Remoting Communication Class Structure
![](https://github.com/apache/rocketmq/raw/develop/docs/cn/image/rocketmq_design_3.png)
### 2 Protocol Design and Code
When a message is sent between Client and Server, a protocol convention is needed for the message sent, so it is necessary to customize the message protocol of RocketMQ. At the same time, in order to efficiently transmit messages and read the received messages, it is necessary to encode and decode the messages. In RocketMQ, the RemotingCommand class encapsulates all data content in the process of message transmission, which includes not only all data structures, but also encoding and decoding operations.

Header field | Type | Request desc | Response desc
--- | --- | --- | --- |
code |int | Request  code. answering business processing is different according to different requests code | Response code. 0 means success, and non-zero means errors.
language | LanguageCode | Language implemented by the requester | Language implemented by the responder
version | int | Version of Request Equation | Version of Response Equation
opaque | int |Equivalent to reqeustId, the different request identification codes on the same connection correspond to those in the response message| The response returns directly without modification
flag | int | Sign, used to distinguish between ordinary RPC or oneway RPC | Sign, used to distinguish between ordinary RPC or oneway RPC
remark | String | Transfer custom text information | Transfer custom text information 
extFields | HashMap<String, String> | Request custom extension information| Response custom extension information
![](https://github.com/apache/rocketmq/raw/develop/docs/cn/image/rocketmq_design_4.png)
From the above figure, the transport content can be divided into four parts: 

 (1) Message length: total length, four bytes of storage, occupying an int type; 
 
(2) Serialization type header length: occupying an int type. The first byte represents the serialization type, and the last three bytes represent the header length；

(3) Header data: serialized header data;

(4) Message body data: binary byte data content of message body;
### 3 Message Communication Mode and Procedure
There are three main ways to support communication in RocketMQ message queue: synchronous (sync), asynchronous (async), one-way (oneway). The "one-way" communication mode is relatively simple and is generally used in sending heartbeat packets without paying attention to its Response. Here, mainly introduce the asynchronous communication flow of RocketMQ.
![](https://github.com/apache/rocketmq/raw/develop/docs/cn/image/rocketmq_design_5.png)
### 4 Reactor Multithread Design
The RPC communication of RocketMQ uses Netty component as the underlying communication library, and also follows the Reactor multithread model. At the same time, some extensions and optimizations are made on it.
![](https://github.com/apache/rocketmq/raw/develop/docs/cn/image/rocketmq_design_6.png)
Above block diagram can roughly understand the Reactor multi-thread model of NettyRemotingServer in RocketMQ. A Reactor main thread (eventLoopGroupBoss, is 1 above) is responsible for listening to TCP network connection requests, establishing connections, creating SocketChannel, and registering on selector. The source code of RocketMQ automatically selects NIO and Epoll according to the type of OS. Then listen to real network data. After you get the network data, you throw it to the Worker thread pool (eventLoopGroupSelector, is the "N" above, the default is 3 in the source code). You need to do SSL verification, codec, idle check, network connection management before you really execute the business logic. These tasks to defaultEventExecutorGroup (that is, "M1" above, the default set to 8 in the source code) to do. The processing business operations are executed in the business thread pool. According to the RomotingCommand business request code, the corresponding processor is found in the processorTable local cache variable and encapsulated into the task, and then submitted to the corresponding business processor processing thread pool for execution (sendMessageExecutor,). Take sending a message, for example, the "M2" above. The thread pool continues to increase in several steps from entry to business logic, which is related to the complexity of each step. The more complex the thread pool is, the wider the concurrent channel is required.
Number of thread | Name of thread | Desc of thread
 --- | --- | --- 
1 | NettyBoss_%d | Reactor Main thread
N | NettyServerEPOLLSelector_%d_%d | Reactor thread pool
M1 | NettyServerCodecThread_%d | Worker thread pool
M2 | RemotingExecutorThread_%d | bussiness processor thread pool


