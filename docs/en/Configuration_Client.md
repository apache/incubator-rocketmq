## Client Configuration

  Relative to RocketMQ's Broker cluster, producers and consumers are client. In this section, it mainly describes the common behavior configuration of producers and consumers.
​ 
### 1 Client Addressing mode

```RocketMQ``` can let client find the ```Name Server```, and then find the ```Broker```by the ```Name Server```. Followings show a variety of configurations, and priority level from highly to lower, the highly priority configurations can override the lower priority configurations.

-  Specified ```Name Server``` address in the code, and multiple ```Name Server``` addresses are separated by semicolons

```java
producer.setNamesrvAddr("192.168.0.1:9876;192.168.0.2:9876");  

consumer.setNamesrvAddr("192.168.0.1:9876;192.168.0.2:9876");
```
- Specified ```Name Server``` address in the Java setup parameters

```text
-Drocketmq.namesrv.addr=192.168.0.1:9876;192.168.0.2:9876  
```
- Specified ```Name Server``` address in the envionment variables

```text
export   NAMESRV_ADDR=192.168.0.1:9876;192.168.0.2:9876   
```
- HTTP static server addressing(default)

After client started, it will access the http static server address, as: <http://jmenv.tbsite.net:8080/rocketmq/nsaddr>, this URL return the following contents:

```text
192.168.0.1:9876;192.168.0.2:9876   
```
By default, the client accesses the HTTP server every 2 minutes, and update the local Name Server address.The URL is hardcoded in the code, you can change the target server by updating ```/etc/hosts``` file, such as add following configuration at the ```/etc/hosts```:
```text
10.232.22.67    jmenv.taobao.net   
```
HTTP static server addressing is recommended, because it is simple client deployment, and the Name Server cluster can be upgraded hot.

### 2 Client Configuration

```DefaultMQProducer```,```TransactionMQProducer```,```DefaultMQPushConsumer```,```DefaultMQPullConsumer``` all extends the ```ClientConfig``` Class, ```ClientConfig``` as the client common configuration class. Client configuration style like getXXX,setXXX, each of the parameters can config by spring and also config their in the code. Such as the ```namesrvAddr``` parameter: ```producer.setNamesrvAddr("192.168.0.1:9876")```, same with the other parameters.

#### 2.1 Client Common Configuration

| Pamater Name                        | Default Value  | Description                                                         |
| ----------------------------- | ------- | ------------------------------------------------------------ |
| namesrvAddr                   |         | Name Server address list, multiple NameServer addresses are separated by semicolons           |
| clientIP                      | local IP  | Client local ip address, some machines will fail to recognize the client IP address, which needs to be enforced in the code |
| instanceName                  | DEFAULT | Name of the client instance, Multiple producers and consumers created by the client actually share one internal instance (this instance contains network connection, thread resources, etc.). |
| clientCallbackExecutorThreads | 4       | Number of communication layer asynchronous callback threads                                        |
| pollNameServerInteval         | 30000   | Polling the Name Server interval in milliseconds                          |
| heartbeatBrokerInterval       | 30000   | The heartbeat interval, in milliseconds, is sent to the Broker                         |
| persistConsumerOffsetInterval | 5000    | The persistent Consumer consumes the progress interval in milliseconds         |

#### 2.2 Producer Configuration

| Pamater Name                       | Default Value          | Description                                                        |
| -------------------------------- | ---------------- | ------------------------------------------------------------ |
| producerGroup                    | DEFAULT_PRODUCER | The name of the Producer group. If multiple producers belong to one application and send the same message, they should be grouped into the same group |
| createTopicKey                   | TBW102           | When a message is sent, topics that do not exist on the server are automatically created and a Key is specified that can be used to configure the default route to the topic where the message is sent.|
| defaultTopicQueueNums            | 4                | The number of default queue when sending messages and auto created topic which not exists the server|
| sendMsgTimeout                   | 10000            | Timeout time of sending message in milliseconds                           |
| compressMsgBodyOverHowmuch       | 4096             | The message Body begins to compress beyond the size(the Consumer gets the message automatically unzipped.), unit of byte|
| retryAnotherBrokerWhenNotStoreOK | FALSE            | If send message and return sendResult but sendStatus!=SEND_OK, Whether to resend |
| retryTimesWhenSendFailed         | 2                | If send message failed, maximum number of retries, this parameter only works for synchronous send mode|
| maxMessageSize                   | 4MB              | Client limit message size, over it may error. Server also limit so need to work with server |
| transactionCheckListener         |                  | The transaction message looks back to the listener, if you want send transaction message, you must setup this
| checkThreadPoolMinSize           | 1                | Minimum of thread in thread pool when Broker look back Producer transaction status                     |
| checkThreadPoolMaxSize           | 1                | Maximum of thread in thread pool when Broker look back Producer transaction status                     |
| checkRequestHoldMax              | 2000             | Producer local buffer request queue size when Broker look back Producer transaction status                     |
| RPCHook                          | null             | This parameter is passed in when the Producer is creating, including the pre-processing before the message sending and the processing after the message response. The user can do some security control or other operations in the first interface.|

#### 2.3 PushConsumer Configuration

| Pamater Name                         | Default Value                      | Description                                                         |
| ---------------------------- | ----------------------------- | ------------------------------------------------------------ |
| consumerGroup                | DEFAULT_CONSUMER              | Consumer group name. If multi Consumer belong to an application, subscribe the same message and consume logic as the same, they should be gathered together |
| messageModel                 | CLUSTERING                    | Message support two mode: cluster consumption and broadcast consumption                          |
| consumeFromWhere             | CONSUME_FROM_LAST_OFFSET      | After Consumer started, default consumption from last location, it include two situation: One is last consumption location is not expired, and consumption start at last location; The other is last location expired, start consumption at current queue's first message |
| consumeTimestamp             | Half an hour ago              | Only consumeFromWhere=CONSUME_FROM_TIMESTAMP, this can work |
| allocateMessageQueueStrategy | AllocateMessageQueueAveragely | Implements strategy of Rebalance algorithms                                        |
| subscription                 |                               | subscription relation                                                    |
| messageListener              |                               | message listener                                                  |
| offsetStore                  |                               | Consumption progress store                                                 |
| consumeThreadMin             | 10                            | Minimum of thread in consumption thread pool                                               |
| consumeThreadMax             | 20                            | Maximum of thread in consumption thread pool                                               |
|                              |                               |                                                              |
| consumeConcurrentlyMaxSpan   | 2000                          | Maximum span allowed for single queue parallel consumption                                 |
| pullThresholdForQueue        | 1000                          | Pull message local queue cache maximum number of messages                               |
| pullInterval                 | 0                             | Pull message interval, because long polling it is 0, but for flow control, you can set value which greater than 0 in milliseconds |
| consumeMessageBatchMaxSize   | 1                             | Batch consume message                                 |
| pullBatchSize                | 32                            | Batch pull message                                 |

#### 2.4 PullConsumer Configuration

| Pamater Name                     | Default Value                 | Description                                                         |
| -------------------------------- | ----------------------------- | ------------------------------------------------------------ |
| consumerGroup                    | DEFAULT_CONSUMER              | Consumer group name. If multi Consumer belong to an application, subscribe the same message and consume logic as the same, they should be gathered together |
| brokerSuspendMaxTimeMillis       | 20000                         | Long polling, Consumer pull message request suspended for the longest time in the Broker in milliseconds     |
| consumerTimeoutMillisWhenSuspend | 30000                         | Long polling, Consumer pull message request suspend in the Broker over this time value, client think timeout. Unit is milliseconds |
| consumerPullTimeoutMillis        | 10000                         | Not long polling, timeout time of pull message in milliseconds                            |
| messageModel                     | BROADCASTING                  | Message support two mode: cluster consumption and broadcast consumption           |
| messageQueueListener             |                               | Listening changing of queue                                                 |
| offsetStore                      |                               | Consumption schedule store                                              |
| registerTopics                   |                               | Collection of registered topics                                              |
| allocateMessageQueueStrategy     | AllocateMessageQueueAveragely | Implements strategy about Rebalance algorithm                                     |

#### 2.5 Message Data Structure

| Field Name         | Default Value  | Description                                                         |
| -------------- | ------ | ------------------------------------------------------------ |
| Topic          | null   | Required, the name of the topic to which the message belongs                                       |
| Body           | null   | Required, message body                                                |
| Tags           | null   | Optional, message tag, convenient for server filtering. Currently only one tag per message is supported |
| Keys           | null   | Optional, represent this message's business keys, server create hash indexes based keys. After setting, you can find message by ```Topics```,```Keys``` in Console system. Because of hash indexes, please make key as unique as possible, such as order number, goods Id and so on.|
| Flag           | 0      | Optional, it is entirely up to the application, and RocketMQ does not intervene                     |
| DelayTimeLevel | 0      | Optional, message delay level, 0 represent no delay, greater tan 0 can consume |
| WaitStoreMsgOK | TRUE   | Optional, indicates whether the message is not answered until the server is down.                |

