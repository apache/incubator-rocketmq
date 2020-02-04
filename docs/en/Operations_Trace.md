# Message Trace

## 1 Key Attributes of Message Trace Data

| Producer        | Consumer        | Broker     |
| ---------------- | ----------------- | ------------ |
| production instance information     | consumption instance information      | message Topic  |
| send message time | post time, post round | message storage location |
| whether the message was sent successfully | Whether the message was successfully consumed  | The Key of the message  |
| Time spent sending         | Time spent consuming         | Tag of the message  |

## 2 Support for Message Trace Cluster Deployment

### 2.1 Broker Configuration Fille

The properties profile content of the Broker side enabled message trace feature is pasted here:

```
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
storePathRootDir=/data/rocketmq/rootdir-a-m
storePathCommitLog=/data/rocketmq/commitlog-a-m
autoCreateSubscriptionGroup=true
## if msg tracing is open,the flag will be true
traceTopicEnable=true
listenPort=10911
brokerIP1=XX.XX.XX.XX1
namesrvAddr=XX.XX.XX.XX:9876
```

### 2.2 Normal Mode
Each Broker node in the RocketMQ cluster is used to store message trace data collected and sent from the Client.Therefore, there are no requirements or restrictions on the number of Broker nodes in the RocketMQ cluster.

### 2.3 Physical IO Isolation Mode
For scenarios with large amount of trace message data , one of the Broker nodes in the RocketMQ cluster can be selected to store the trace message , so that the common message data of the user and the physical IO of the trace message data are completely isolated from each other.In this mode, there are at least two Broker nodes in the RockeMQ cluster, one of which is defined as the server on which message trace data is stored.

### 2.4 Start the Broker that Starts the MessageTrace
`nohup sh mqbroker -c ../conf/2m-noslave/broker-a.properties &`

## 3 Save the Topic Definition of Message Trace 
RocketMQ's message trace feature supports two ways to store trace data:

### 3.1 System-level TraceTopic
By default, message track data is stored in the system-level TraceTopic(names：**RMQ_SYS_TRACE_TOPIC**)。This Topic is automatically created when the Broker node is started(As described above, the switch variable **traceTopicEnable** needs to be set to **true** in the Broker  configuration file）。

### 3.2 Custom TraceTopic 
If the user is not prepared to store the message track data in the system-level default TraceTopic, you can also define and create a user-level Topic to save the track (that is, to create a regular Topic to save the message track data)。The following section introduces how the Client interface supports the user-defined TraceTopic.

## 4 Client Practices that Support Message Trace
In order to reduce as much as possible the transformation work of RocketMQ message trace feature used in the user service system, the author added a switch parameter (**enableMsgTrace**) to the original interface in the design to realize whether the message trace is opened or not.

### 4.1 Opening  the Message Trace when Sending  the Message
```
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName",true);
        producer.setNamesrvAddr("XX.XX.XX.XX1");
        producer.start();
            try {
                {
                    Message msg = new Message("TopicTest",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg);
                    System.out.printf("%s%n", sendResult);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
```

### 4.2 Opening Message Trace whenSubscribing to a Message
```
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_JODIE_1",true);
        consumer.subscribe("TopicTest", "*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setConsumeTimestamp("20181109221800");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.printf("Consumer Started.%n");
```

### 4.3 Support for Custom Storage Message Trace Topic
The initialization of `DefaultMQProducer` and `DefaultMQPushConsumer` instances can be changed to support the custom storage message trace Topic as follows when sending and subscriving messages above.

```
        ##Where Topic_test11111 needs to be pre-created by the user to save the message trace；
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName",true,"Topic_test11111");
        ......

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_JODIE_1",true,"Topic_test11111");
        ......
```