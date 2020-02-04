## Load Balancing
Load balancing in RocketMQ is accomplished on Client side. Specifically, it can be divided into load balancing at Producer side when sending messages and load balancing at Constumer side when subscribing messages.

### 1 Producer Load Balancing
When the Producer sends a message, it will first find the specified TopicPublishInfo according to Topic. After getting the routing information of TopicPublishInfo, the RocketMQ client will select a queue (MessageQueue) from the messageQueue List in TopicPublishInfo  to send the message by default.Specific fault-tolerant strategies are defined in the MQFaultStrategy class.
Here is a sendLatencyFaultEnable switch variable, which, if turned on, filters out the Broker agent of not available on the basis of randomly gradually increasing modular arithmetic selection. The so-called "latencyFault Tolerance" refers to a certain period of time to avoid previous failures. For example, if the latency of the last request exceeds 550 Lms, it will evade 3000 Lms; if it exceeds 1000L, it will evade 60000 L; if it is closed, it will choose a queue (MessageQueue) to send messages by randomly gradually increasing modular arithmetic, and the latencyFault Tolerance mechanism is the key to achieve high availability of message sending.

### 2 Consumer Load Balancing
In RocketMQ, the two consumption modes (Push/Pull) on the Consumer side are both based on the pull mode to get the message, while in the Push mode it is only a kind of encapsulation of the pull mode, which is essentially implemented as the message pulling thread after pulling a batch of messages from the server. After submitting to the message consuming thread pool, it continues to try again to pull the message to the server. If the message is not pulled, the pull is delayed and continues. In both pull mode based consumption patterns (Push/Pull), the Consumer needs to know which message queue - queue from the Broker side to get the message. Therefore, it is necessary to do load balancing on the Consumer side, that is, which Consumer consumption is allocated to the same ConsumerGroup by more than one MessageQueue on the Broker side.

 1， Heartbeat Packet Sending on Consumer side
After Consumer is started, it continuously sends heartbeat packets to all Broker instances in the RocketMQ cluster via timing task (which contains the message consumption group name, subscription relationship collection,Message communication mode and the value of the client id,etc). After receiving the heartbeat message from Consumer, Broker side maintains it in Consumer Manager's local caching variable—consumerTable, At the same time, the encapsulated client network channel information is stored in the local caching variable—channelInfoTable, which can provide metadata information for the later load balancing of Consumer.
2，Core Class for Load Balancing on Consumer side—RebalanceImpl
Starting the MQClientInstance instance in the startup process of the Consumer instance will complete the start of the load balancing service thread-RebalanceService (executed every 20 s). By looking at the source code, we can find that the run () method of the RebalanceService thread calls the rebalanceByTopic () method of the RebalanceImpl class, which is the core of the Consumer end load balancing. Here, rebalanceByTopic () method will do different logical processing depending on whether the consumer communication type is "broadcast mode" or "cluster mode". Here we mainly look at the main processing flow in cluster mode:
(1) Get the message consumption queue set (mqSet) under the Topic from the local cache variable—topicSubscribeInfoTable of the rebalanceImpl instance.
(2) Call mQClientFactory. findConsumerIdList () method to send a RPC communication request to Broker side to obtain the consumer Id list under the consumer group based on the parameters of topic and consumer group (consumer table constructed by Broker side based on the heartbeat data reported by the front consumer side responds and returns, business request code: GET_CONSUMER_LIST_BY_GROUP);
(3) First, the message consumption queue and the consumer Id under Topic are sorted, then the message queue to be pulled is calculated by using the message queue allocation strategy algorithm (default: the average allocation algorithm of the message queue). The average allocation algorithm here is similar to the paging algorithm. It ranks all MessageQueues like records. It ranks all consumers like pages. It calculates the average size of each page and the range of each page record. Finally, it traverses the whole range and calculates the records that the current consumer should allocate to (MessageQueue here).
![Image text](https://github.com/apache/rocketmq/raw/develop/docs/cn/image/rocketmq_design_8.png)
(4) Then, the updateProcessQueueTableInRebalance () method is invoked, which first compares the allocated message queue set (mqSet) with processQueueTable for filtering.
![Image text](https://github.com/apache/rocketmq/raw/develop/docs/cn/image/rocketmq_design_9.png)

 - The red part of the processQueueTable annotation in the figure above
   indicates that it is not included with the assigned message queue set
   mqSet. Set the Dropped attribute to true for these queues, and then
   check whether these queues can remove the processQueueTable cache
   variable or not. The removeUnnecessaryMessageQueue () method is
   executed here, that is, check every 1s to see if the locks of the
   current consumption processing queue can be retrieved and return true
   if they are retrieved. If the lock of the current consumer processing
   queue is still not available after waiting for 1s, it returns false.
   If true is returned, the corresponding Entry is removed from the
   processQueueTable cache variable.
 - The green section in processQueueTable above represents the
   intersection with the assigned message queue set mqSet. Determine
   whether the ProcessQueue has expired, regardless of Pull mode, if it
   is Push mode, set the Dropped attribute to true, and call the
   removeUnnecessaryMessageQueue () method to try to remove Entry as
   above;
   
Finally, a ProcessQueue object is created for each MessageQueue in the filtered message queue set (mqSet) and stored in the processQueueTable queue of RebalanceImpl (where the computePullFromWhere (MessageQueue mq) method of the RebalanceImpl instance is invoked to obtain the next progress consumption value offset of the MessageQueue object, which is then populated into the attribute of pullRequest object to be created next time.), and create pull request object—pullRequest to add to pull list—pullRequestList, and finally execute dispatchPullRequest () method. PullRequest object of Pull message is put into the blocking queue pullRequestQueue of PullMessageService service thread in turn, and the request of Pull message is sent to Broker end after the service thread takes out. Among them, we can focus on the contrast, RebalancePushImpl and RebalancePullImpl two implementation classes dispatchPullRequest () method is different, the method in RebalancePullImpl class is empty, thus answering the last question in the previous article.

The core design idea of message consumption queue is that a message consumption queue can only be consumed by one consumer in the same consumer group at the same time, and a message consumer can consume multiple message queues at the same time.
