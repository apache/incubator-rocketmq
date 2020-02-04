# 消息轨迹
----

## 1. 消息轨迹数据关键属性
| Producer端| Consumer端 | Broker端 |
| --- | --- | --- |
| 生产实例信息 | 消费实例信息 | 消息的Topic |
| 发送消息时间 | 投递时间,投递轮次  | 消息存储位置 |
| 消息是否发送成功 | 消息是否消费成功 | 消息的Key值 |
| 发送耗时 | 消费耗时 | 消息的Tag值 |

## 2. 支持消息轨迹集群部署

### 2.1 Broker端配置文件
这里贴出Broker端开启消息轨迹特性的properties配置文件内容：
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

### 2.2 普通模式
RocketMQ集群中每一个Broker节点均用于存储Client端收集并发送过来的消息轨迹数据。因此，对于RocketMQ集群中的Broker节点数量并无要求和限制。

### 2.3 物理IO隔离模式
对于消息轨迹数据量较大的场景，可以在RocketMQ集群中选择其中一个Broker节点专用于存储消息轨迹，使得用户普通的消息数据与消息轨迹数据的物理IO完全隔离，互不影响。在该模式下，RockeMQ集群中至少有两个Broker节点，其中一个Broker节点定义为存储消息轨迹数据的服务端。

### 2.4 启动开启消息轨迹的Broker
`nohup sh mqbroker -c ../conf/2m-noslave/broker-a.properties &`
  
## 3. 保存消息轨迹的Topic定义
RocketMQ的消息轨迹特性支持两种存储轨迹数据的方式：

### 3.1 系统级的TraceTopic
在默认情况下，消息轨迹数据是存储于系统级的TraceTopic中(其名称为：**RMQ_SYS_TRACE_TOPIC**)。该Topic在Broker节点启动时，会自动创建出来（如上所叙，需要在Broker端的配置文件中将**traceTopicEnable**的开关变量设置为**true**）。

### 3.2 用户自定义的TraceTopic 
如果用户不准备将消息轨迹的数据存储于系统级的默认TraceTopic，也可以自己定义并创建用户级的Topic来保存轨迹（即为创建普通的Topic用于保存消息轨迹数据）。下面一节会介绍Client客户端的接口如何支持用户自定义的TraceTopic。

## 4. 支持消息轨迹的Client客户端实践
为了尽可能地减少用户业务系统使用RocketMQ消息轨迹特性的改造工作量，作者在设计时候采用对原来接口增加一个开关参数(**enableMsgTrace**)来实现消息轨迹是否开启；并新增一个自定义参(**customizedTraceTopic**)数来实现用户存储消息轨迹数据至自己创建的用户级Topic。

### 4.1 发送消息时开启消息轨迹
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

### 4.2 订阅消息时开启消息轨迹
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

### 4.3 支持自定义存储消息轨迹Topic
在上面的发送和订阅消息时候分别将DefaultMQProducer和DefaultMQPushConsumer实例的初始化修改为如下即可支持自定义存储消息轨迹Topic。
```
        ##其中Topic_test11111需要用户自己预先创建，来保存消息轨迹；
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName",true,"Topic_test11111");
        ......

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_JODIE_1",true,"Topic_test11111");
        ......
```




