### Producer
----
##### 1 Message Sending Tips
###### 1.1 The Use of Tags
One application instance should use one topic as much as possible and the subtype of messages can be marked by tags. Tag provides extra flexibility to users. In the consume subscribing process, the messages filtering can only be handled by using tags when the tags are specified in the message sending process: `message.setTags("TagA")`.
###### 1.2 The Use of Keys
A business  key can be set in one message and it will be easier to look up the message on a broker server to diagnose issues during development. Each message will be created index(hash index) by server, instance can query the content of this message by topic and key and who consumes the message.Because of the hash index, make sure that the key should be unique in order to avoid potential hash index conflict.
``` java
// Order Id
String orderId = "20034568923546";
message.setKeys(orderId);
```
###### 1.3 The Log Print
When sending a message,no matter success or fail, a message log must be printed which contains SendResult and Key. It is assumed that we will always get SEND_OK if no exception is thrown. Below is a list of descriptions about each status:
* SEND_OK

SEND_OK means sending message successfully. SEND_OK does not mean it is reliable. To make sure no message would be lost, you should also enable SYNC_MASTER or SYNC_FLUSH.
* FLUSH_DISK_TIMEOUT

FLUSH_DISK_TIMEOUT means sending message successfully but the Broker flushing the disk with timeout. In this kind of condition, the Broker has saved this message in memory, this message will be lost only if the Broker was down. The FlushDiskType and SyncFlushTimeout could be specified in MessageStoreConfig. If the Broker set MessageStoreConfig’s FlushDiskType=SYNC_FLUSH(default is ASYNC_FLUSH), and the Broker doesn’t finish flushing the disk within MessageStoreConfig’s syncFlushTimeout(default is 5 secs), you will get this status.
* FLUSH_SLAVE_TIMEOUT

FLUSH_SLAVE_TIMEOUT means sending messages successfully but the slave Broker does not finish synchronizing with the master. If the Broker’s role is SYNC_MASTER(default is ASYNC_MASTER), and the slave Broker doesn’t finish synchronizing with the master within the MessageStoreConfig’s syncFlushTimeout(default is 5 secs), you will get this status.
* SLAVE_NOT_AVAILABLE

SLAVE_NOT_AVAILABLE means sending messages successfully but no slave Broker configured. If the Broker’s role is SYNC_MASTER(default is ASYNC_MASTER), but no slave Broker is configured, you will get this status.

##### 2 Operations on Message Sending failed
The send method of Producer can be retried, the retry  process is illustrated below:
* The method will retry at most 2 times(2 times in synchronous mode, 0 times in asynchronous mode).
* If sending failed, it will turn to the next Broker. This strategy will be executed when the total costing time is less then sendMsgTimeout(default is 10 seconds).
* The retry method will be terminated if timeout exception was thrown when sending messages to Broker.

The strategy above could make sure message sending successfully to a certain degree. Some more retry strategies, such as we could try to save the message to database if calling the send synchronous method failed and then retry by background thread's timed tasks, which will make sure the message is sent to Broker,could be improved if asking for high reliability business requirement. 

The reasons why the retry strategy using database have not integrated by the RocketMQ client will be explained below: Firstly, the design mode of the RocketMQ client is stateless mode. It means that the client is designed to be horizontally scalable at each level and the consumption of the client to physical resources is only CPU, memory and network. Then, if a key-value memory module is integrated by the client itself, the Asyn-Saving strategy will be utilized in consideration of the high resource consumption of the Syn-Saving strategy. However, given that operations staff does not manage the client shutoff, some special commands, such as kill -9, may be used which will lead to the lost of message because of no saving in time. Furthermore, the physical resource running Producer is not appropriate to save some significant data because of low reliability. Above all, the retry process should be controlled by program itself.

##### 3 Send Messages in One-way Mode
The message sending is usually a process like below: 
* Client sends request to sever.
* Sever handles request
* Sever returns response to client

The total costing time of sending one message is the sum of costing time of three steps above. Some situations demand that total costing time must be in a quite low level, however, do not take reliable performance into consideration, such as log collection. This kind of application could be called in one-way mode, which means client sends request but not wait for response. In this kind of mode, the cost of sending request is only a call of system operation which means one operation writing data to client socket buffer. Generally, the time cost of this process will be controlled n microseconds level.
