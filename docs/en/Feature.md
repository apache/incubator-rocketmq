# Features
## 1 Subscribe and Publish
Message publication refers to that a producer sends messages to a topic; Message subscription means a consumer follows a topic with certain tags and then consumes data from that topic.

## 2 Message Ordering
Message ordering refers to that a group of messages can be consumed orderly as they are published. For example, an order generates three messages: order creation, order payment, and order completion. It only makes sense to consume them in their generated order, but orders can be consumed in parallel at the same time. RocketMQ can strictly guarantee these messages are in order.

Orderly message is divided into global orderly message and partitioned orderly message. Global order means that all messages under a certain topic must be in order, partitioned order only requires each group of messages are consumed orderly.
- Global message ordering:
For a given Topic, all messages are published and consumed in strict first-in-first-out (FIFO) order.
Applicable scenario: the performance requirement is not high, and all messages are published and consumed according to FIFO principle strictly.
- Partitioned message ordering:
For a given Topic, all messages are partitioned according to sharding key. Messages within the same partition are published and consumed in strict FIFO order. Sharding key is the key field to distinguish message's partition, which is a completely different concept from the key of ordinary messages.
Applicable scenario: high performance requirement, with sharding key as the partition field, messages within the same partition are published and consumed according to FIFO principle strictly.

## 3 Message Filter
Consumers of RocketMQ can filter messages based on tags as well as supporting for user-defined attribute filtering. Message filter is currently implemented on the Broker side, with the advantage of reducing the network transmission of useless messages for Consumer and the disadvantage of increasing the burden on the Broker and relatively complex implementation.

## 4 Message Reliability
RocketMQ supports high reliability of messages in several situations:
1 Broker shutdown normally
2 Broker abnormal crash
3 OS Crash
4 The machine is out of power, but it can be recovered immediately
5 The machine cannot be started up (the CPU, motherboard, memory and other key equipment may be damaged)
6 Disk equipment damaged

In the four cases of 1), 2), 3), and 4) where the hardware resource can be recovered immediately, RocketMQ guarantees that the message will not be lost or a small amount of data will be lost (depending on whether the flush disk type is synchronous or asynchronous).

5 ) and 6) are single point of failure and cannot be recovered. Once it happens, all messages on the single point will be lost. In both cases, RocketMQ ensures that 99% of the messages are not lost through asynchronous replication, but a very few number of messages may still be lost. Synchronous double write mode can completely avoid single point of failure, which will surely affect the performance and suitable for the occasion of high demand for message reliability, such as money related applications. Note: RocketMQ supports synchronous double writes since version 3.0.

## 5 At Least Once
At least Once refers to that every message will be delivered at least once. RocketMQ supports this feature because the Consumer pulls the message locally and does not send an ack back to the server until it has consumed it.

## 6 Backtracking Consumption
Backtracking consumption refers to that the Consumer has consumed the message successfully, but the business needs to consume again. To support this function, the message still needs to be retained after the Broker sends the message to the Consumer successfully. The re-consumption is normally based on time dimension. For example, after the recovery of the Consumer system failured, the data one hour ago needs to be re-consumed, then the Broker needs to provide a mechanism to reverse the consumption progress according to the time dimension. RocketMQ supports backtracking consumption by time trace, with the time dimension down to milliseconds.

## 7 Transactional Message
RocketMQ transactional message refers to the fact that the application of a local transaction and the sending of a Message operation can be defined in a global transaction which means both succeed or failed simultaneously. RocketMQ transactional message provides distributed transaction functionality similar to X/Open XA, enabling the ultimate consistency of distributed transactions through transactional message.

## 8 Scheduled Message
Scheduled message(delay queue) refers to that messages are not consumed immediately after they are sent to the broker, but waiting to be delivered to the real topic after a specific time. 
The broker has a configuration item, `messageDelayLevel`, with default values “1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h”, 18 levels. Users can configure a custom `messageDelayLevel`. Note that `messageDelayLevel` is a broker's property rather than a topic's. When sending a message, just set the delayLevel level: msg.setDelayLevel(level). There are three types of levels:

- level == 0, The message is not a delayed message
- 1<=level<=maxLevel, Message delay specific time, such as level==1, delay for 1s
- level > maxLevel, than level== maxLevel, such as level==20, delay for 2h

Scheduled messages are temporarily saved in a topic named SCHEDULE_TOPIC_XXXX, and saved in a specific queue according to delayTimeLevel, queueId = delayTimeLevel - 1, that is, only messages with the same delay are saved in a queue, ensuring that messages with the same sending delay can be consumed orderly. The broker consumes SCHEDULE_TOPIC_XXXX on schedule and writes messages to the real topic.

Note that Scheduled messages are counted both the first time they are written and the time they are scheduled to be written to the real topic, so both the send number and the TPS are increased.

## 9 Message Retry
When the Consumer fails to consume the message, a retry mechanism is needed to make the message to be consumed again. Consumer's consume failure can usually be classified as follows:
- Due to the reasons of the message itself, such as deserialization failure, the message data itself cannot be processed (for example, the phone number of the current message is cancelled and cannot be charged), etc. This kind of error usually requires skipping this message and consuming others since immediately retry would be failed 99%, so it is better to provide a timed retry mechanism that retries after 10 seconds.
- Due to the reasons of dependent downstream application services are not available, such as db connection is not usable, perimeter network is not unreachable, etc. When this kind of error is encountered, consuming other messages will also result in an error even if the current failed message is skipped. In this case, it is recommended to sleep for 30s before consuming the next message, which will reduce the pressure on the broker to retry the message.

RocketMQ will set up a retry queue named “%RETRY%+consumerGroup” for each consumer group(Note that the retry queue for this topic is for consumer groups, not for each topic) to temporarily save messages cannot be consumed by customer due to all kinds of reasons. Considering that it takes some time for the exception to recover, multiple retry levels are set for the retry queue, and each retry level has a corresponding re-deliver delay. The more retries, the greater the deliver delay. RocketMQ first save retry messages to the delay queue which topic is named “SCHEDULE_TOPIC_XXXX”, then background schedule task will save the messages to “%RETRY%+consumerGroup” retry queue according to their corresponding delay.

## 10 Message Resend
When a producer sends a message, the synchronous message will be resent if fails, the asynchronous message will retry and oneway message is without any guarantee. Message resend ensures that messages are sent successfully and without lost as much as possible, but it can lead to message duplication, which is an unavoidable problem in RocketMQ. Under normal circumstances, message duplication will not occur, but when there is a large number of messages and network jitter, message duplication will be a high-probability event. In addition, producer initiative messages resend and the consumer load changes will also result in duplicate messages. The message retry policy can be set as follows:

- `retryTimesWhenSendFailed`: Synchronous message retry times when send failed, default value is 2, so the producer will try to send `retryTimesWhenSendFailed` + 1 times at most. To ensure that the message is not lost, producer will try sending the message to another broker instead of selecting the broker that failed last time. An exception will be thrown if it reaches the retry limit, and the client should guarantee that the message will not be lost. Messages will resend when RemotingException, MQClientException, and partial MQBrokerException occur.
- `retryTimesWhenSendAsyncFailed`: Asynchronous message retry times when send failed, asynchronous retry sends message to the same broker instead of selecting another one and does not guarantee that the message wont lost.
- `retryAnotherBrokerWhenNotStoreOK`: Message flush disk (master or slave) timeout or slave not available (return status is not SEND_OK), whether to try to send to another broker, default value is false. Very important messages can set to true.

## 11 Flow Control
Producer flow control, because broker processing capacity reaches a bottleneck; Consumer flow control, because the consumption capacity reaches a bottleneck.

Producer flow control:
- When commitLog file locked time exceeds osPageCacheBusyTimeOutMills, default value of `osPageCacheBusyTimeOutMills` is 1000 ms, then return flow control.
- If `transientStorePoolEnable` == true, and the broker is asynchronous flush disk type, and resources are insufficient in the transientStorePool, reject the current send request and return flow control.
- The broker checks the head request wait time of the send request queue every 10ms. If the wait time exceeds waitTimeMillsInSendQueue, which default value is 200ms, the current send request is rejected and the flow control is returned.
- The broker implements flow control by rejecting send requests.

Consumer flow control:
- When consumer local cache messages number exceeds pullThresholdForQueue, default value is 1000. 
- When consumer local cache messages size exceeds pullThresholdSizeForQueue, default value is 100MB. 
- When consumer local cache messages span exceeds consumeConcurrentlyMaxSpan, default value is 2000.

The result of consumer flow control is to reduce the pull frequency.

## 12 Dead Letter Queue
Dead letter queue is used to deal messages that cannot be consumed normally. When a message is consumed failed at first time, the message queue will automatically resend the message. If the consumption still fails after the maximum number retry, it indicates that the consumer cannot properly consume the message under normal circumstances. At this time, the message queue will not immediately abandon the message, but send it to the special queue corresponding to the consumer.

RocketMQ defines the messages that could not be consumed under normal circumstances as Dead-Letter Messages, and the special queue in which the Dead-Letter Messages are saved as Dead-Letter Queues. In RocketMQ, the consumer instance can consume again by resending messages in the Dead-Letter Queue using console.