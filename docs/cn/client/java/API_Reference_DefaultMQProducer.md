## DefaultMQProducer
---
### 类简介

`public class DefaultMQProducer 
extends ClientConfig 
implements MQProducer`

>`DefaultMQProducer`类是应用用来投递消息的入口，开箱即用，可通过无参构造方法快速创建一个生产者。主要负责消息的发送，支持同步/异步/oneway的发送方式，这些发送方式均支持批量发送。可以通过该类提供的getter/setter方法，调整发送者的参数。`DefaultMQProducer`提供了多个send方法，每个send方法略有不同，在使用前务必详细了解其意图。下面给出一个生产者示例代码，[点击查看更多示例代码](https://github.com/apache/rocketmq/blob/master/example/src/main/java/org/apache/rocketmq/example/)。

``` java 
public class Producer {
    public static void main(String[] args) throws MQClientException {
        // 创建指定分组名的生产者
        DefaultMQProducer producer = new DefaultMQProducer("ProducerGroupName");

        // 启动生产者
        producer.start();

        for (int i = 0; i < 128; i++)
            try {
            	// 构建消息
                Message msg = new Message("TopicTest",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

                // 同步发送
                SendResult sendResult = producer.send(msg);

                // 打印发送结果
                System.out.printf("%s%n", sendResult);
            } catch (Exception e) {
                e.printStackTrace();
            }

        producer.shutdown();
    }
}
```

**注意**：该类是线程安全的。在配置并启动完成后可在多个线程间安全共享。

### 字段摘要
|类型|字段名称|描述|
|------|-------|-------|
|DefaultMQProducerImpl|defaultMQProducerImpl|生产者的内部默认实现|
|String|producerGroup|生产者分组|
|String|createTopicKey|在发送消息时，自动创建服务器不存在的topic|
|int|defaultTopicQueueNums|创建topic时默认的队列数量|
|int|sendMsgTimeout|发送消息的超时时间|
|int|compressMsgBodyOverHowmuch|压缩消息体的阈值|
|int|retryTimesWhenSendFailed|同步模式下内部尝试发送消息的最大次数|
|int|retryTimesWhenSendAsyncFailed|异步模式下内部尝试发送消息的最大次数|
|boolean|retryAnotherBrokerWhenNotStoreOK|是否在内部发送失败时重试另一个broker|
|int|maxMessageSize|消息的最大长度|
|TraceDispatcher|traceDispatcher|消息追踪器。使用rcpHook来追踪消息|

### 构造方法摘要

|方法名称|方法描述|
|-------|------------|
|DefaultMQProducer()|由默认参数值创建一个生产者 |
|DefaultMQProducer(final String producerGroup)|使用指定的分组名创建一个生产者|
|DefaultMQProducer(final String producerGroup, boolean enableMsgTrace)|使用指定的分组名创建一个生产者，并设置是否开启消息追踪|
|DefaultMQProducer(final String producerGroup, boolean enableMsgTrace, final String customizedTraceTopic)|使用指定的分组名创建一个生产者，并设置是否开启消息追踪及追踪topic的名称|
|DefaultMQProducer(RPCHook rpcHook)|使用指定的hook创建一个生产者|
|DefaultMQProducer(final String producerGroup, RPCHook rpcHook)|使用指定的分组名及自定义hook创建一个生产者|
|DefaultMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace,final String customizedTraceTopic)|使用指定的分组名及自定义hook创建一个生产者，并设置是否开启消息追踪及追踪topic的名称|

### 使用方法摘要

|返回值|方法名称|方法描述|
|-------|-------|------------|
|void|createTopic(String key, String newTopic, int queueNum)|在broker上创建指定的topic|
|void|createTopic(String key, String newTopic, int queueNum, int topicSysFlag)|在broker上创建指定的topic|
|long|earliestMsgStoreTime(MessageQueue mq)|查询最早的消息存储时间|
|List<MessageQueue>|fetchPublishMessageQueues(String topic)|获取topic的消息队列|
|long|maxOffset(MessageQueue mq)|查询给定消息队列的最大offset|
|long|minOffset(MessageQueue mq)|查询给定消息队列的最小offset|
|QueryResult|queryMessage(String topic, String key, int maxNum, long begin, long end)|按关键字查询消息|
|long|searchOffset(MessageQueue mq, long timestamp)|查找指定时间的消息队列的物理offset|
|SendResult|send(Collection<Message> msgs)|同步批量发送消息|
|SendResult|send(Collection<Message> msgs, long timeout)|同步批量发送消息|
|SendResult|send(Collection<Message> msgs, MessageQueue messageQueue)|向指定的消息队列同步批量发送消息|
|SendResult|send(Collection<Message> msgs, MessageQueue messageQueue, long timeout)|向指定的消息队列同步批量发送消息，并指定超时时间|
|SendResult|send(Message msg)|同步单条发送消息|
|SendResult|send(Message msg, long timeout)|同步发送单条消息，并指定超时时间|
|SendResult|send(Message msg, MessageQueue mq)|向指定的消息队列同步发送单条消息|
|SendResult|send(Message msg, MessageQueue mq, long timeout)|向指定的消息队列同步单条发送消息，并指定超时时间|
|void|send(Message msg, MessageQueue mq, SendCallback sendCallback)|向指定的消息队列异步单条发送消息，并指定回调方法|
|void|send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)|向指定的消息队列异步单条发送消息，并指定回调方法和超时时间|
|SendResult|send(Message msg, MessageQueueSelector selector, Object arg)|向消息队列同步单条发送消息，并指定发送队列选择器|
|SendResult|send(Message msg, MessageQueueSelector selector, Object arg, long timeout)|向消息队列同步单条发送消息，并指定发送队列选择器与超时时间|
|void|send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)|向指定的消息队列异步单条发送消息|
|void|send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)|向指定的消息队列异步单条发送消息，并指定超时时间|
|void|send(Message msg, SendCallback sendCallback)|异步发送消息|
|void|send(Message msg, SendCallback sendCallback, long timeout)|异步发送消息，并指定回调方法和超时时间|
|TransactionSendResult|sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, final Object arg)|发送事务消息，并指定本地执行事务实例|
|TransactionSendResult|sendMessageInTransaction(Message msg, Object arg)|发送事务消息|
|void|sendOneway(Message msg)|单向发送消息，不等待broker响应|
|void|sendOneway(Message msg, MessageQueue mq) |单向发送消息到指定队列，不等待broker响应|
|void|sendOneway(Message msg, MessageQueueSelector selector, Object arg)|单向发送消息到队列选择器的选中的队列，不等待broker响应|
|void|shutdown()|关闭当前生产者实例并释放相关资源|
|void|start()|启动生产者|
|MessageExt|viewMessage(String offsetMsgId)|根据给定的msgId查询消息|
|MessageExt|public MessageExt viewMessage(String topic, String msgId)|根据给定的msgId查询消息，并指定topic|

### 字段详细信息

- [producerGroup](http://rocketmq.apache.org/docs/core-concept/)

	`private String producerGroup`
	
	生产者的分组名称。相同的分组名称表明生产者实例在概念上归属于同一分组。这对事务消息十分重要，如果原始生产者在事务之后崩溃，那么broker可以联系同一生产者分组的不同生产者实例来提交或回滚事务。

	默认值：DEFAULT_PRODUCER

	注意： 由数字、字母、下划线、横杠（-）、竖线（|）或百分号组成；不能为空；长度不能超过255。

- defaultMQProducerImpl

	`protected final transient DefaultMQProducerImpl defaultMQProducerImpl`

	生产者的内部默认实现，在构造生产者时内部自动初始化，提供了大部分方法的内部实现。

- createTopicKey

	`private String createTopicKey = MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC`

	在发送消息时，自动创建服务器不存在的topic，需要指定Key，该Key可用于配置发送消息所在topic的默认路由。

	默认值：TBW102

	建议：测试或者demo使用，生产环境下不建议打开自动创建配置。

- defaultTopicQueueNums

	`private volatile int defaultTopicQueueNums = 4`

	创建topic时默认的队列数量。

	默认值：4

- sendMsgTimeout

	`private int sendMsgTimeout = 3000`

	发送消息时的超时时间。

	默认值：3000，单位：毫秒 

	建议：不建议修改该值，该值应该与broker配置中的sendTimeout一致，发送超时，可临时修改该值，建议解决超时问题，提高broker集群的Tps。

- compressMsgBodyOverHowmuch

	`private int compressMsgBodyOverHowmuch = 1024 * 4`

	压缩消息体阈值。大于4K的消息体将默认进行压缩。

	默认值：1024 * 4，单位：字节

	建议：可通过DefaultMQProducerImpl.setZipCompressLevel方法设置压缩率（默认为5，可选范围[0,9]）；可通过DefaultMQProducerImpl.tryToCompressMessage方法测试出compressLevel与compressMsgBodyOverHowmuch最佳值。

- retryTimesWhenSendFailed

	`private int retryTimesWhenSendFailed = 2`

	同步模式下，在返回发送失败之前，内部尝试重新发送消息的最大次数。

	默认值：2，即：默认情况下一条消息最多会被投递3次。

	注意：在极端情况下，这可能会导致消息的重复。

- retryTimesWhenSendAsyncFailed

	`private int retryTimesWhenSendAsyncFailed = 2`

	异步模式下，在发送失败之前，内部尝试重新发送消息的最大次数。

	默认值：2，即：默认情况下一条消息最多会被投递3次。

	注意：在极端情况下，这可能会导致消息的重复。

- retryAnotherBrokerWhenNotStoreOK

	`private boolean retryAnotherBrokerWhenNotStoreOK = false`

	同步模式下，消息保存失败时是否重试其他broker。

	默认值：false

	注意：此配置关闭时，非投递时产生异常情况下，会忽略retryTimesWhenSendFailed配置。

- maxMessageSize

	`private int maxMessageSize = 1024 * 1024 * 4`

	消息的最大大小。当消息题的字节数超过maxMessageSize就发送失败。

	默认值：1024 * 1024 * 4，单位：字节

- [traceDispatcher](https://github.com/apache/rocketmq/wiki/RIP-6-Message-Trace)

	`private TraceDispatcher traceDispatcher = null`

	在开启消息追踪后，该类通过hook的方式把消息生产者，消息存储的broker和消费者消费消息的信息像链路一样记录下来。在构造生产者时根据构造入参enableMsgTrace来决定是否创建该对象。

### 构造方法详细信息

1. DefaultMQProducer
	
	`public DefaultMQProducer()`

	创建一个新的生产者。

2. DefaultMQProducer
	
	`DefaultMQProducer(final String producerGroup)`

	使用指定的分组名创建一个生产者。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 缺省值 |描述
		---|---|---|---|---
		producerGroup | String | 是 | DEFAULT_PRODUCER | 生产者的分组名称

3. DefaultMQProducer
	
	`DefaultMQProducer(final String producerGroup, boolean enableMsgTrace)`

	使用指定的分组名创建一个生产者，并设置是否开启消息追踪。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 缺省值 |描述
		---|---|---|---|---
		producerGroup | String | 是 | DEFAULT_PRODUCER | 生产者的分组名称
		enableMsgTrace | boolean | 是 | false |是否开启消息追踪

4. DefaultMQProducer
	
	`DefaultMQProducer(final String producerGroup, boolean enableMsgTrace, final String customizedTraceTopic)`

	使用指定的分组名创建一个生产者，并设置是否开启消息追踪及追踪topic的名称。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 缺省值 |描述
		---|---|---|---|---
		producerGroup | String | 是 | DEFAULT_PRODUCER | 生产者的分组名称
		rpcHook | RPCHook | 否 | null |每个远程命令执行后会回调rpcHook
		enableMsgTrace | boolean | 是 | false |是否开启消息追踪
		customizedTraceTopic | String | 否 | RMQ_SYS_TRACE_TOPIC | 消息跟踪topic的名称

5. DefaultMQProducer
	
	`DefaultMQProducer(RPCHook rpcHook)`

	使用指定的hook创建一个生产者。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 缺省值 |描述
		---|---|---|---|---
		rpcHook | RPCHook | 否 | null |每个远程命令执行后会回调rpcHook

6. DefaultMQProducer

	`DefaultMQProducer(final String producerGroup, RPCHook rpcHook)`

	使用指定的分组名及自定义hook创建一个生产者。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 缺省值 |描述
		---|---|---|---|---
		producerGroup | String | 是 | DEFAULT_PRODUCER | 生产者的分组名称
		rpcHook | RPCHook | 否 | null |每个远程命令执行后会回调rpcHook

7. DefaultMQProducer

	`DefaultMQProducer(final String producerGroup, RPCHook rpcHook, boolean enableMsgTrace,final String customizedTraceTopic)`

	使用指定的分组名及自定义hook创建一个生产者，并设置是否开启消息追踪及追踪topic的名称。

	- 入参描述：

	参数名 | 类型 | 是否必须 | 缺省值 |描述
	---|---|---|---|---
	producerGroup | String | 是 | DEFAULT_PRODUCER | 生产者的分组名称
	rpcHook | RPCHook | 否 | null |每个远程命令执行后会回调rpcHook
	enableMsgTrace | boolean | 是 | false |是否开启消息追踪
	customizedTraceTopic | String | 否 | RMQ_SYS_TRACE_TOPIC | 消息跟踪topic的名称

### 使用方法详细信息

1.  createTopic

	`public void createTopic(String key, String newTopic, int queueNum)`

	在broker上创建一个topic。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 默认值 |值范围 | 说明
		---|---|---|---|---|---
		key | String | 是 | | | 访问密钥。
		newTopic | String | 是 | |  | 新建topic的名称。由数字、字母、下划线（_）、横杠（-）、竖线（&#124;）或百分号（%）组成；<br>长度小于255；不能为TBW102或空。
		queueNum | int | 是 | 0 | (0, maxIntValue] | topic的队列数量。

	- 返回值描述：

		void

	- 异常描述：

		MQClientException - 生产者状态非Running；未找到broker等客户端异常。

2.  createTopic

	`public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)`

	在broker上创建一个topic。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 默认值 |值范围 | 说明
		---|---|---|---|---|---
		key | String | 是 | | | 访问密钥。
		newTopic | String | 是 | |  | 新建topic的名称。
		queueNum | int | 是 | 0 | (0, maxIntValue] | topic的队列数量。
		topicSysFlag | int | 是 | 0 | | 保留字段，暂未使用。

	- 返回值描述：

		void

	- 异常描述：

		MQClientException - 生产者状态非Running；未找到broker等客户端异常。

3. earliestMsgStoreTime

	`public long earliestMsgStoreTime(MessageQueue mq)`

	查询最早的消息存储时间。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		mq | MessageQueue | 是 | | | 要查询的消息队列
		
	- 返回值描述：

		指定队列最早的消息存储时间。单位：毫秒。

	- 异常描述：

		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。

4. fetchPublishMessageQueues

	`public List<MessageQueue> fetchPublishMessageQueues(String topic)`

	获取topic的消息队列。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		topic | String | 是 | | | topic名称

	- 返回值描述：

		传入topic下的消息队列。

	- 异常描述：

		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。

5. maxOffset

	`public long maxOffset(MessageQueue mq)`

	查询消息队列的最大物理偏移量。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		mq | MessageQueue | 是 | | | 要查询的消息队列

	- 返回值描述：

		给定消息队列的最大物理偏移量。

	- 异常描述：

		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。

6. minOffset

	`public long minOffset(MessageQueue mq)`

	查询给定消息队列的最小物理偏移量。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		mq | MessageQueue | 是 | | | 要查询的消息队列

	- 返回值描述：

		给定消息队列的最小物理偏移量。

	- 异常描述：

		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。

7. queryMessage

	`public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)`

	按关键字查询消息。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		topic | String | 是 | | | topic名称
		key | String | 否 | null | | 查找的关键字
		maxNum | int | 是 | | | 返回消息的最大数量
		begin | long | 是 | | | 开始时间戳，单位：毫秒
		end | long | 是 | | | 结束时间戳，单位：毫秒

	- 返回值描述：

		查询到的消息集合。

	- 异常描述：

		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常等客户端异常客户端异常。<br>
		InterruptedException - 线程中断。

8. searchOffset

	`public long searchOffset(MessageQueue mq, long timestamp)`

	查找指定时间的消息队列的物理偏移量。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		mq | MessageQueue | 是 | | | 要查询的消息队列。
		timestamp | long | 是 | | | 指定要查询时间的时间戳。单位：毫秒。

	- 返回值描述：

		指定时间的消息队列的物理偏移量。

	- 异常描述：

		MQClientException - 生产者状态非Running；没有找到broker；broker返回失败；网络异常；线程中断等客户端异常。

9. send

	`public SendResult send(Collection<Message> msgs)`

	同步批量发送消息。在返回发送失败之前，内部尝试重新发送消息的最大次数（参见*retryTimesWhenSendFailed*属性）。未明确指定发送队列，默认采取轮询策略发送。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msgs | Collection<Message> | 是 | | | 待发送的消息集合。集合内的消息必须属同一个topic。

	- 返回值描述：

		批量消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

10. send

	`public SendResult send(Collection<Message> msgs, long timeout)`

	同步批量发送消息，如果在指定的超时时间内未完成消息投递，会抛出*RemotingTooMuchRequestException*。
	在返回发送失败之前，内部尝试重新发送消息的最大次数（参见*retryTimesWhenSendFailed*属性）。未明确指定发送队列，默认采取轮询策略发送。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msgs | Collection<Message> | 是 | | | 待发送的消息集合。集合内的消息必须属同一个topic。
		timeout | long | 是 | 参见*sendMsgTimeout*属性 | | 发送超时时间，单位：毫秒。

	- 返回值描述：

		批量消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

11. send

	`public SendResult send(Collection<Message> msgs, MessageQueue messageQueue)`

	向给定队列同步批量发送消息。
	
	注意：指定队列意味着所有消息均为同一个topic。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msgs | Collection<Message> | 是 | | | 待发送的消息集合。集合内的消息必须属同一个topic。
		messageQueue | MessageQueue | 是 | | | 待投递的消息队列。指定队列意味着待投递消息均为同一个topic。

	- 返回值描述：

		批量消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

12. send

	`public SendResult send(Collection<Message> msgs, MessageQueue messageQueue, long timeout)`

	向给定队列同步批量发送消息，如果在指定的超时时间内未完成消息投递，会抛出*RemotingTooMuchRequestException*。
	
	注意：指定队列意味着所有消息均为同一个topic。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msgs | Collection<Message> | 是 | | | 待发送的消息集合。集合内的消息必须属同一个topic。
		timeout | long | 是 | 参见*sendMsgTimeout*属性 | | 发送超时时间，单位：毫秒。
		messageQueue | MessageQueue | 是 | | | 待投递的消息队列。指定队列意味着待投递消息均为同一个topic。

	- 返回值描述：

		批量消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

13. send

	`public SendResult send(Message msg)`

	以同步模式发送消息，仅当发送过程完全完成时，此方法才会返回。
	在返回发送失败之前，内部尝试重新发送消息的最大次数（参见*retryTimesWhenSendFailed*属性）。未明确指定发送队列，默认采取轮询策略发送。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。

	- 返回值描述：

		消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

14. send

	`public SendResult send(Message msg, long timeout)`

	以同步模式发送消息，如果在指定的超时时间内未完成消息投递，会抛出*RemotingTooMuchRequestException*。仅当发送过程完全完成时，此方法才会返回。
	在返回发送失败之前，内部尝试重新发送消息的最大次数（参见*retryTimesWhenSendFailed*属性）。未明确指定发送队列，默认采取轮询策略发送。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		timeout | long | 是 | 参见*sendMsgTimeout*属性 | | 发送超时时间，单位：毫秒。

	- 返回值描述：

		消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

15. send

	`public SendResult send(Message msg, MessageQueue mq)`

	向指定的消息队列同步发送单条消息。仅当发送过程完全完成时，此方法才会返回。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		mq | MessageQueue | 是 | | | 待投递的消息队列。

	- 返回值描述：

		消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

16. send

	`public SendResult send(Message msg, MessageQueue mq, long timeout)`

	向指定的消息队列同步发送单条消息，如果在指定的超时时间内未完成消息投递，会抛出*RemotingTooMuchRequestException*。仅当发送过程完全完成时，此方法才会返回。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		timeout | long | 是 | 参见*sendMsgTimeout*属性 | | 发送超时时间，单位：毫秒。
		mq | MessageQueue | 是 | | | 待投递的消息队列。指定队列意味着待投递消息均为同一个topic。

	- 返回值描述：

		消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

17. send

	`public void send(Message msg, MessageQueue mq, SendCallback sendCallback)`

	向指定的消息队列异步发送单条消息，异步发送调用后直接返回，并在在发送成功或者异常时回调`sendCallback`，所以异步发送时`sendCallback`参数不能为null，否则在回调时会抛出`NullPointerException`。
	异步发送时，在成功发送前，其内部会尝试重新发送消息的最大次数（参见*retryTimesWhenSendAsyncFailed*属性）。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		mq | MessageQueue | 是 | | | 待投递的消息队列。指定队列意味着待投递消息均为同一个topic。
		sendCallback | SendCallback | 是 | | | 回调接口的实现。

	- 返回值描述：

		void

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		InterruptedException - 发送线程中断。

18. send

	`public void send(Message msg, MessageQueue mq, SendCallback sendCallback, long timeout)`

	向指定的消息队列异步发送单条消息，异步发送调用后直接返回，并在在发送成功或者异常时回调`sendCallback`，所以异步发送时`sendCallback`参数不能为null，否则在回调时会抛出`NullPointerException`。
	若在指定时间内消息未发送成功，回调方法会收到*RemotingTooMuchRequestException*异常。
	异步发送时，在成功发送前，其内部会尝试重新发送消息的最大次数（参见*retryTimesWhenSendAsyncFailed*属性）。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		mq | MessageQueue | 是 | | | 待投递的消息队列。
		sendCallback | SendCallback | 是 | | | 回调接口的实现。
		timeout | long | 是 | 参见*sendMsgTimeout*属性 | | 发送超时时间，单位：毫秒。

	- 返回值描述：
		void

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		InterruptedException - 发送线程中断。

19. send

	`public SendResult send(Message msg, MessageQueueSelector selector, Object arg)`

	向通过`MessageQueueSelector`计算出的队列同步发送消息。

	可以通过自实现`MessageQueueSelector`接口，将某一类消息发送至固定的队列。比如：将同一个订单的状态变更消息投递至固定的队列。

    注意：此消息发送失败内部不会重试。
    
	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		selector | MessageQueueSelector | 是 | | | 队列选择器。
		arg | Object | 否 | | | 供队列选择器使用的参数对象。

	- 返回值描述：

		消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

20. send

	`public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)`

	向通过`MessageQueueSelector`计算出的队列同步发送消息，并指定发送超时时间。

	可以通过自实现`MessageQueueSelector`接口，将某一类消息发送至固定的队列。比如：将同一个订单的状态变更消息投递至固定的队列。
	
	注意：此消息发送失败内部不会重试。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		selector | MessageQueueSelector | 是 | | | 队列选择器。
		arg | Object | 否 | | | 供队列选择器使用的参数对象。
		timeout | long | 是 | 参见*sendMsgTimeout*属性 | | 发送超时时间，单位：毫秒。

	- 返回值描述：

		消息的发送结果，包含msgId，发送状态等信息。

	- 异常描述：
		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 发送线程中断。<br>
		RemotingTooMuchRequestException - 发送超时。

21. send

	`public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)`

	向通过`MessageQueueSelector`计算出的队列异步发送单条消息，异步发送调用后直接返回，并在在发送成功或者异常时回调`sendCallback`，所以异步发送时`sendCallback`参数不能为null，否则在回调时会抛出`NullPointerException`。
	异步发送时，在成功发送前，其内部会尝试重新发送消息的最大次数（参见*retryTimesWhenSendAsyncFailed*属性）。

	可以通过自实现`MessageQueueSelector`接口，将某一类消息发送至固定的队列。比如：将同一个订单的状态变更消息投递至固定的队列。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		selector | MessageQueueSelector | 是 | | | 队列选择器。
		arg | Object | 否 | | | 供队列选择器使用的参数对象。
		sendCallback | SendCallback | 是 | | | 回调接口的实现。

	- 返回值描述：

		void

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		InterruptedException - 发送线程中断。

22. send

	`public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback, long timeout)`

	向通过`MessageQueueSelector`计算出的队列异步发送单条消息，异步发送调用后直接返回，并在在发送成功或者异常时回调`sendCallback`，所以异步发送时`sendCallback`参数不能为null，否则在回调时会抛出`NullPointerException`。
	异步发送时，在成功发送前，其内部会尝试重新发送消息的最大次数（参见*retryTimesWhenSendAsyncFailed*属性）。

	可以通过自实现`MessageQueueSelector`接口，将某一类消息发送至固定的队列。比如：将同一个订单的状态变更消息投递至固定的队列。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		selector | MessageQueueSelector | 是 | | | 队列选择器。
		arg | Object | 否 | | | 供队列选择器使用的参数对象。
		sendCallback | SendCallback | 是 | | | 回调接口的实现。
		timeout | long | 是 | 参见*sendMsgTimeout*属性 | | 发送超时时间，单位：毫秒。

	- 返回值描述：

		void

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		InterruptedException - 发送线程中断。

23. send

	`public void send(Message msg, SendCallback sendCallback)`

	异步发送单条消息，异步发送调用后直接返回，并在在发送成功或者异常时回调`sendCallback`，所以异步发送时`sendCallback`参数不能为null，否则在回调时会抛出`NullPointerException`。
	异步发送时，在成功发送前，其内部会尝试重新发送消息的最大次数（参见*retryTimesWhenSendAsyncFailed*属性）。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		sendCallback | SendCallback | 是 | | | 回调接口的实现。

	- 返回值描述：

		void

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		InterruptedException - 发送线程中断。

24. send

	`public void send(Message msg, SendCallback sendCallback, long timeout)`

	异步发送单条消息，异步发送调用后直接返回，并在在发送成功或者异常时回调`sendCallback`，所以异步发送时`sendCallback`参数不能为null，否则在回调时会抛出`NullPointerException`。
	异步发送时，在成功发送前，其内部会尝试重新发送消息的最大次数（参见*retryTimesWhenSendAsyncFailed*属性）。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		sendCallback | SendCallback | 是 | | | 回调接口的实现。
		timeout | long | 是 | 参见*sendMsgTimeout*属性 | | 发送超时时间，单位：毫秒。

	- 返回值描述：

		void

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		InterruptedException - 发送线程中断。

25. sendMessageInTransaction

	`public TransactionSendResult sendMessageInTransaction(Message msg, LocalTransactionExecuter tranExecuter, final Object arg)`

	发送事务消息。该类不做默认实现，抛出`RuntimeException`异常。参见：`TransactionMQProducer`类。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待投递的事务消息
		tranExecuter | `LocalTransactionExecuter` | 是 | | | 本地事务执行器。该类*已过期*，将在5.0.0版本中移除。请勿使用该方法。
		arg | Object | 是 | | | 供本地事务执行程序使用的参数对象

	- 返回值描述：

		事务结果，参见：`LocalTransactionState`类。

	- 异常描述：

		RuntimeException - 永远抛出该异常。

26. sendMessageInTransaction

	`public TransactionSendResult sendMessageInTransaction(Message msg, final Object arg)`

	发送事务消息。该类不做默认实现，抛出`RuntimeException`异常。参见：`TransactionMQProducer`类。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待投递的事务消息
		arg | Object | 是 | | | 供本地事务执行程序使用的参数对象

	- 返回值描述：
		
		事务结果，参见：`LocalTransactionState`类。

	- 异常描述：

		RuntimeException - 永远抛出该异常。

27. sendOneway

	`public void sendOneway(Message msg)`

	  以oneway形式发送消息，broker不会响应任何执行结果，和[UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol)类似。它具有最大的吞吐量但消息可能会丢失。

	  可在消息量大，追求高吞吐量并允许消息丢失的情况下使用该方式。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待投递的消息

	- 返回值描述：
		
		void

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		InterruptedException - 发送线程中断。

28. sendOneway

	`public void sendOneway(Message msg, MessageQueue mq)`

	  向指定队列以oneway形式发送消息，broker不会响应任何执行结果，和[UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol)类似。它具有最大的吞吐量但消息可能会丢失。

	  可在消息量大，追求高吞吐量并允许消息丢失的情况下使用该方式。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待投递的消息
		mq | MessageQueue | 是 | | | 待投递的消息队列

	- 返回值描述：
		void
	- 异常描述：
		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		InterruptedException - 发送线程中断。

29. sendOneway

	`public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)`

	  向通过`MessageQueueSelector`计算出的队列以oneway形式发送消息，broker不会响应任何执行结果，和[UDP](https://en.wikipedia.org/wiki/User_Datagram_Protocol)类似。它具有最大的吞吐量但消息可能会丢失。

	  可在消息量大，追求高吞吐量并允许消息丢失的情况下使用该方式。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msg | Message | 是 | | | 待发送的消息。
		selector | MessageQueueSelector | 是 | | | 队列选择器。
		arg | Object | 否 | | | 供队列选择器使用的参数对象。

	- 返回值描述：
		
		void

	- 异常描述：

		MQClientException - broker不存在或未找到；namesrv地址为空；未找到topic的路由信息等客户端异常。<br>
		RemotingException - 网络异常。<br>
		InterruptedException - 发送线程中断。

30. shutdown

	`public void shutdown()`

	关闭当前生产者实例并释放相关资源。

	- 入参描述：

		无。

	- 返回值描述：

		void

	- 异常描述：

31. start

	`public void start()`

	启动生产者实例。在发送或查询消息之前必须调用此方法。它执行了许多内部初始化，比如：检查配置、与namesrv建立连接、启动一系列心跳等定时任务等。

	- 入参描述：

		无。

	- 返回值描述：

		void

	- 异常描述：

		MQClientException - 初始化过程中出现失败。

32. viewMessage

	`public MessageExt viewMessage(String offsetMsgId)`

	根据给定的msgId查询消息。

	- 入参描述：
		
		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		offsetMsgId | String | 是 | | | offsetMsgId

	- 返回值描述：

		返回`MessageExt`，包含：topic名称，消息题，消息ID，消费次数，生产者host等信息。

	- 异常描述：

		RemotingException - 网络层发生错误。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 线程被中断。<br>
		MQClientException - 生产者状态非Running；msgId非法等。

33. viewMessage

	`public MessageExt viewMessage(String topic, String msgId)`

	根据给定的msgId查询消息，并指定topic。

	- 入参描述：

		参数名 | 类型 | 是否必须 | 默认值 | 值范围 | 说明
		---|---|---|---|---|---
		msgId | String | 是 | | | msgId
		topic | String | 是 | | | topic名称

	- 返回值描述：

		返回`MessageExt`，包含：topic名称，消息题，消息ID，消费次数，生产者host等信息。

	- 异常描述：

		RemotingException - 网络层发生错误。<br>
		MQBrokerException - broker发生错误。<br>
		InterruptedException - 线程被中断。<br>
		MQClientException - 生产者状态非Running；msgId非法等。