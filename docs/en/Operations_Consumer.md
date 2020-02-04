## Consumer

----

### 1 Consumption process idempotent

RocketMQ cannot avoid Exactly-Once, so if the business is very sensitive to consumption repetition, it is important to perform deduplication at the business level. Deduplication can be done with a relational database. First, you need to determine the unique key of the message, which can be either msgId or a unique identifier field in the message content, such as the order Id. Determine if a unique key exists in the relational database before consumption. If it does not exist, insert it and consume it, otherwise skip it. (The actual process should consider the atomic problem, determine whether there is an attempt to insert, if the primary key conflicts, the insertion fails, skip directly)

### 2  Slow message processing

#### 2.1 Increase consumption parallelism

Most messages consumption behaviors are IO-intensive, That is, it may be to operate the database, or call RPC. The consumption speed of such consumption behavior lies in the throughput of the back-end database or the external system. By increasing the consumption parallelism, the total consumption throughput can be increased, but the degree of parallelism is increased to a certain extent. Instead it will fall.Therefore, the application must set a reasonable degree of parallelism. There are several ways to modify the degree of parallelism of consumption as follows:

* Under the same ConsumerGroup, increase the degree of parallelism by increasing the number of Consumer instances (note that the Consumer instance that exceeds the number of subscription queues is invalid). Can be done by adding machines, or by starting multiple processes on an existing machine.
* Improve the consumption parallel thread of a single Consumer by modifying the parameters consumeThreadMin and consumeThreadMax.

#### 2.2 Batch mode consumption

Some business processes can increase consumption throughput to a large extent if they support batch mode consumption. For example, order deduction application, it takes 1s to process one order at a time, and it takes only 2s to process 10 orders at a time. In this way, the throughput of consumption can be greatly improved. By setting the consumer's consumeMessageBatchMaxSize to return a parameter, the default is 1, that is, only one message is consumed at a time, for example, set to N, then the number of messages consumed each time is less than or equal to N.

#### 2.3 Skip non-critical messages

When a message is accumulated, if the consumption speed cannot keep up with the transmission speed, if the service does not require high data, you can choose to discard the unimportant message. For example, when the number of messages in a queue is more than 100,000 , try to discard some or all of the messages, so that you can quickly catch up with the speed of sending messages. The sample code is as follows:

```java
public ConsumeConcurrentlyStatus consumeMessage(
        List<MessageExt> msgs,
        ConsumeConcurrentlyContext context){
   long offest = msgs.get(0).getQueueOffset();
   String maxOffset =    
               msgs.get(0).getProperty(Message.PROPERTY_MAX_OFFSET);
   long diff = Long.parseLong(maxOffset) - offset;
   if(diff > 100000){
        //TODO Special handling of message accumulation
       return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
    //TODO Normal consumption process
    return ConcumeConcurrentlyStatus.CONSUME_SUCCESS;
}
```

#### 2.4 Optimize each message consumption process

For example, the consumption process of a message is as follows:

* Query from DB according to the message [data 1]
* Query from DB according to the message [data 2]
* Complex business calculations
* Insert [Data 3] into the DB
* Insert [Data 4] into the DB

There are 4 interactions with the DB in the consumption process of this message. If it is calculated by 5ms each time, it takes a total of 20ms. If the business calculation takes 5ms, then the total time is 25ms, So if you can optimize 4 DB interactions to 2 times, the total time can be optimized to 15ms, which means the overall performance is increased by 40%. Therefore, if the application is sensitive to delay, the DB can be deployed on the SSD hard disk. Compared with the SCSI disk, the former RT will be much smaller.

### 3 Print Log

If the amount of messages is small, it is recommended to print the message in the consumption entry method, consume time, etc., to facilitate subsequent troubleshooting.

```java
public ConsumeConcurrentlyStatus consumeMessage(
          List<MessageExt> msgs,
    ConsumeConcurrentlyContext context){
    log.info("RECEIVE_MSG_BEGIN: " + msgs.toString());
    //TODO Normal consumption process
    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
}
```

If you can print the time spent on each message, it will be more convenient when troubleshooting online problems such as slow consumption.

### 4 Other consumption suggestions

#### 4.1、Consumer Group and Subscriptions

The first thing you should be aware of is that different Consumer Group can consume the same topic independently, and each of them will have their own consuming offsets. Please make sure each Consumer within the same Group to subscribe the same topics.

#### 4.2、Orderly

The Consumer will lock each MessageQueue to make sure it is consumed one by one in order. This will cause a performance loss, but it is useful when you care about the order of the messages. It is not recommended to throw exceptions, you can return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT instead.

#### 4.3、Concurrently

As the name tells, the Consumer will consume the messages concurrently. It is recommended to use this for good performance. It is not recommended to throw exceptions, you can return ConsumeConcurrentlyStatus.RECONSUME_LATER instead.

#### 4.4、Consume Status

For MessageListenerConcurrently, you can return RECONSUME_LATER to tell the consumer that you can not consume it right now and want to reconsume it later. Then you can continue to consume other messages. For MessageListenerOrderly, because you care about the order, you can not jump over the message, but you can return SUSPEND_CURRENT_QUEUE_A_MOMENT to tell the consumer to wait for a moment.

#### 4.5、Blocking

It is not recommend to block the Listener, because it will block the thread pool, and eventually may stop the consuming process.

#### 4.6、Thread Number

The consumer use a ThreadPoolExecutor to process consuming internally, so you can change it by setting setConsumeThreadMin or setConsumeThreadMax.

#### 4.7、ConsumeFromWhere

When a new Consumer Group is established, it will need to decide whether it needs to consume the historical messages which had already existed in the Broker. CONSUME_FROM_LAST_OFFSET will ignore the historical messages, and consume anything produced after that. CONSUME_FROM_FIRST_OFFSET will consume every message existed in the Broker. You can also use CONSUME_FROM_TIMESTAMP to consume messages produced after the specified timestamp.







