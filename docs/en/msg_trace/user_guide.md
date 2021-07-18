# message trace
----

## 1. Message trace data's key properties
| Producer End| Consumer End| Broker End|
| --- | --- | --- |
| produce message | consume message | message's topic |
| send message time | delivery time, delivery rounds  | message store location |
| whether the message was sent successfully | whether message was consumed successfully | message's key |
| send cost-time | consume cost-time | message's tag value |

## 2. Enable message trace in cluster deployment

### 2.1 Broker's configuration file
following by Broker's properties file configuration that enable message trace:
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

### 2.2 Common mode
Each Broker node in RocketMQ cluster used for storing message trace data that client collected and sent. So, there is no requirements and limitations to the size of Broker node in RocketMQ cluster.

### 2.3 IO physical isolation mode
For huge amounts of message trace data scenario, we can select any one Broker node in RocketMQ cluster used for storing message trace data special, thus, common message data's IO are isolated from message trace data's IO in physical, not impact each other. In this mode, RocketMQ cluster must have at least two Broker nodes, the one that defined as storing message trace data.

### 2.4 Start Broker that enable message trace
`nohup sh mqbroker -c ../conf/2m-noslave/broker-a.properties &`
  
## 3. Save the definition of topic that with support message trace
RocketMQ's message trace feature supports two types of storage.

### 3.1 System level TraceTopic
Be default, message trace data is stored in system level TraceTopic(topic name: **RMQ_SYS_TRACE_TOPIC**). That topic will be created at startup of broker(As mentioned above, set **traceTopicEnable** to **true** in Broker's configuration).

### 3.2 User defined TraceTopic
If user don't want to store message trace data in system level TraceTopic, he can create user defined TraceTopic used for storing message trace data(that is, create common topic for storing message trace data). The following part will introduce how client SDK support user defined TraceTopic.

## 4. Client SDK demo with message trace feature
For business system adapting to use RocketMQ's message trace feature easily, in design phase, the author add a switch parameter(**enableMsgTrace**) for enable message trace; add a custom parameter(**customizedTraceTopic**) for user defined TraceTopic.

### 4.1 Enable message trace when sending messages
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

### 4.2 Enable message trace when subscribe messages
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

### 4.3 Self-defined topic support message trace
Adjusting instantiation of DefaultMQProducer and DefaultMQPushConsumer as following code to support user defined TraceTopic.
```
        ##Topic_test11111 should be created by user, used for storing message trace data.
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName",true,"Topic_test11111");
        ......

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_JODIE_1",true,"Topic_test11111");
        ......
```


### 4.4 Send and query message trace by mqadmin command
- send message
```shell
./mqadmin sendMessage -m true --topic some-topic-name -n 127.0.0.1:9876 -p "your meesgae content"
```
- query trace
```shell
./mqadmin QueryMsgTraceById -n 127.0.0.1:9876 -i "some-message-id"
```
- query trace result
```
RocketMQLog:WARN No appenders could be found for logger (io.netty.util.internal.PlatformDependent0).
RocketMQLog:WARN Please initialize the logger system properly.
#Type      #ProducerGroup       #ClientHost          #SendTime            #CostTimes #Status
Pub        1623305799667        xxx.xxx.xxx.xxx       2021-06-10 14:16:40  131ms      success
```


