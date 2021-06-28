## DefaultPullConsumer
---
### 类简介

1. `DefaultMQPullConsumer extends ClientConfig implements MQPullConsumer`

2. `DefaultMQPullConsumer`主动的从Broker拉取消息，主动权由应用控制，可以实现批量的消费消息。Pull方式取消息的过程需要用户自己写,首先通过打算消费的Topic拿到MessageQueue的集合,遍历MessageQueue集合,然后针对每个MessageQueue批量取消息,也可以自定义与控制offset位置。
                        
3. 优势:consumer可以按需消费,不用担心自己处理能力,而broker堆积消息也会相对简单,无需记录每一个要发送消息的状态,只需要维护所有消息的队列和偏移量就可以了。所以对于慢消费,消息量有限且到来的速度不均匀的情况,pull模式比较合适消息延迟与忙等。
                        
4. 缺点:由于主动权在消费方,消费方无法及时获取最新的消息。比较适合不及时批处理场景。
                        
``` java 

 
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
 
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
 
public class MQPullConsumer {
 
	private static final Map<MessageQueue,Long> OFFSE_TABLE = new HashMap<MessageQueue,Long>();
	
	public static void main(String[] args) throws MQClientException {
		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("groupName");
		consumer.setNamesrvAddr("127.0.0.1:9876");
		consumer.start();
		// 从指定topic中拉取所有消息队列
		Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues("order-topic");
		for(MessageQueue mq:mqs){
			try {
				// 获取消息的offset，指定从store中获取
				long offset = consumer.fetchConsumeOffset(mq,true);
				System.out.println("consumer from the queue:"+mq+":"+offset);
				while(true){
					PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, 
							getMessageQueueOffset(mq), 32);
					putMessageQueueOffset(mq,pullResult.getNextBeginOffset());
					switch(pullResult.getPullStatus()){
					case FOUND:
						List<MessageExt> messageExtList = pullResult.getMsgFoundList();
                        for (MessageExt m : messageExtList) {
                            System.out.println(new String(m.getBody()));
                        }
						break;
					case NO_MATCHED_MSG:
						break;
					case NO_NEW_MSG:
						break;
					case OFFSET_ILLEGAL:
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		consumer.shutdown();
	}
 
	// 保存上次消费的消息下标
	private static void putMessageQueueOffset(MessageQueue mq,
			long nextBeginOffset) {
		OFFSE_TABLE.put(mq, nextBeginOffset);
	}
	
	// 获取上次消费的消息的下标
	private static Long getMessageQueueOffset(MessageQueue mq) {
		Long offset = OFFSE_TABLE.get(mq);
		if(offset != null){
			return offset;
		}
		return 0l;
	}
	
 
}
```



### 字段摘要
|类型|字段名称|描述|
|------|-------|-------|
|DefaultMQPullConsumerImpl|defaultMQPullConsumerImpl|DefaultMQPullConsumer的内部核心处理默认实现|
|String|consumerGroup|消费的唯一分组|
|long|brokerSuspendMaxTimeMillis|consumer取连接broker的最大延迟时间，不建议修改|
|long|consumerTimeoutMillisWhenSuspend|pull取连接的最大超时时间，必须大于brokerSuspendMaxTimeMillis，不建议修改|
|long|consumerPullTimeoutMillis|socket连接的最大超时时间，不建议修改|
|String|messageModel|默认cluster模式|
|int|messageQueueListener|消息queue监听器，用来获取topic的queue变化|
|int|offsetStore|RemoteBrokerOffsetStore 远程与本地offset存储器|
|int|registerTopics|注册到该consumer的topic集合|
|int|allocateMessageQueueStrategy|consumer的默认获取queue的负载分配策略算法|

### 构造方法摘要

|方法名称|方法描述|
|-------|------------|
|DefaultMQPullConsumer()|由默认参数值创建一个Pull消费者 |
|DefaultMQPullConsumer(final String consumerGroup, RPCHook rpcHook)|使用指定的分组名，hook创建一个消费者|
|DefaultMQPullConsumer(final String consumerGroup)|使用指定的分组名消费者|
|DefaultMQPullConsumer(RPCHook rpcHook)|使用指定的hook创建一个生产者|


### 使用方法摘要

|返回值|方法名称|方法描述|
|-------|-------|------------|
|MQAdmin接口method|-------|------------|
|void|createTopic(String key, String newTopic, int queueNum)|在broker上创建指定的topic|
|void|createTopic(String key, String newTopic, int queueNum, int topicSysFlag)|在broker上创建指定的topic|
|long|earliestMsgStoreTime(MessageQueue mq)|查询最早的消息存储时间|
|long|maxOffset(MessageQueue mq)|查询给定消息队列的最大offset|
|long|minOffset(MessageQueue mq)|查询给定消息队列的最小offset|
|QueryResult|queryMessage(String topic, String key, int maxNum, long begin, long end)|按关键字查询消息|
|long|searchOffset(MessageQueue mq, long timestamp)|查找指定时间的消息队列的物理offset|
|MessageExt|viewMessage(String offsetMsgId)|根据给定的msgId查询消息|
|MessageExt|public MessageExt viewMessage(String topic, String msgId)|根据给定的msgId查询消息，并指定topic|
|MQConsumer接口method|-------|------------|
|Set<MessageQueue>|fetchSubscribeMessageQueues(String topic)|根据topic获取订阅的Queue|
|void|sendMessageBack(final MessageExt msg, final int delayLevel)|如果消息出来失败，可以发送回去延迟消费，delayLevel=DelayConf.DELAY_LEVEL|
|void|sendMessageBack(final MessageExt msg, final int delayLevel, final String brokerName)|如果消息出来失败，可以发送回去延迟消费，delayLevel=DelayConf.DELAY_LEVEL|
|MQPullConsumer接口method|-------|------------|
|long|fetchConsumeOffset(MessageQueue mq, boolean fromStore)|查询给定消息队列的最大offset|
|PullResult |pull(final MessageQueue mq, final String subExpression, final long offset,final int maxNums)|异步拉取制定匹配的消息|
|PullResult| pull(final MessageQueue mq, final String subExpression, final long offset,final int maxNums, final long timeout)|异步拉取制定匹配的消息|
|PullResult|pull(final MessageQueue mq, final MessageSelector selector, final long offset,final int maxNums)|异步拉取制定匹配的消息，通过MessageSelector器来过滤消息，参考org.apache.rocketmq.common.filter.ExpressionType|
|PullResult|pullBlockIfNotFound(final MessageQueue mq, final String subExpression,final long offset, final int maxNums)|异步拉取制定匹配的消息，如果没有消息讲block住，并指定超时时间consumerPullTimeoutMillis|
|void|pullBlockIfNotFound(final MessageQueue mq, final String subExpression, final long offset,final int maxNums, final PullCallback pullCallback)|异步拉取制定匹配的消息，如果没有消息讲block住，并指定超时时间consumerPullTimeoutMillis，通过回调pullCallback来消费|    
|void|updateConsumeOffset(final MessageQueue mq, final long offset)|更新指定mq的offset|
|long|fetchMessageQueuesInBalance(String topic)|根据topic获取订阅的Queue(是balance分配后的)|
|void|void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup)|如果消息出来失败，可以发送回去延迟消费，delayLevel=DelayConf.DELAY_LEVEL,消息可能在同一个consumerGroup消费|
|void|shutdown()|关闭当前消费者实例并释放相关资源|
|void|start()|启动消费者|

