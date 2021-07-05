# Basic Sample 
------
基本示例中提供了以下两个功能
* RocketMQ可用于以三种方式发送消息：可靠的同步、可靠的异步和单向传输。前两种消息类型是可靠的，因为无论它们是否成功发送都有响应。
* RocketMQ可以用来消费消息。
### 1 添加依赖
maven:
``` java
<dependency>
  <groupId>org.apache.rocketmq</groupId>
  <artifactId>rocketmq-client</artifactId>
  <version>4.3.0</version>
</dependency>
```
gradle: 
``` java 
compile 'org.apache.rocketmq:rocketmq-client:4.3.0'
```
### 2 发送消息
##### 2.1 使用Producer发送同步消息
可靠的同步传输被广泛应用于各种场景，如重要的通知消息、短消息通知等。
``` java
public class SyncProducer {
  public static void main(String[] args) throws Exception {
    // Instantiate with a producer group name
    DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
    // Specify name server addresses
    producer.setNamesrvAddr("localhost:9876");
    // Launch the producer instance
    producer.start();
    for (int i = 0; i < 100; i++) {
      // Create a message instance with specifying topic, tag and message body
      Message msg = new Message("TopicTest" /* Topic */,
        "TagA" /* Tag */,
        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
        );
      // Send message to one of brokers
      SendResult sendResult = producer.send(msg);
      // Check whether the message has been delivered by the callback of sendResult
      System.out.printf("%s%n", sendResult);
    }
    // Shut down once the producer instance is not longer in use
    producer.shutdown();
  }
}
```
##### 2.2 发送异步消息
异步传输通常用于响应时间敏感的业务场景。这意味着发送方无法等待代理的响应太长时间。
``` java
public class AsyncProducer {
  public static void main(String[] args) throws Exception {
    // Instantiate with a producer group name
    DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
    // Specify name server addresses
    producer.setNamesrvAddr("localhost:9876");
    // Launch the producer instance
    producer.start();
    producer.setRetryTimesWhenSendAsyncFailed(0);
    for (int i = 0; i < 100; i++) {
      final int index = i;
      // Create a message instance with specifying topic, tag and message body
      Message msg = new Message("TopicTest",
        "TagA",
        "OrderID188",
        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
      // SendCallback: receive the callback of the asynchronous return result.
      producer.send(msg, new SendCallback() {
        @Override
        public void onSuccess(SendResult sendResult) {
          System.out.printf("%-10d OK %s %n", index,
            sendResult.getMsgId());
        }
        @Override
        public void onException(Throwable e) {
          System.out.printf("%-10d Exception %s %n", index, e);
          e.printStackTrace();
        }
      });
    }
    // Shut down once the producer instance is not longer in use
    producer.shutdown();
  }
}
```
##### 2.3 以单向模式发送消息
单向传输用于需要中等可靠性的情况，如日志收集。
``` java
public class OnewayProducer {
  public static void main(String[] args) throws Exception{
    // Instantiate with a producer group name
    DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
    // Specify name server addresses
    producer.setNamesrvAddr("localhost:9876");
    // Launch the producer instance
    producer.start();
    for (int i = 0; i < 100; i++) {
      // Create a message instance with specifying topic, tag and message body
      Message msg = new Message("TopicTest" /* Topic */,
        "TagA" /* Tag */,
        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
      );
      // Send in one-way mode, no return result
      producer.sendOneway(msg);
    }
    // Shut down once the producer instance is not longer in use
     producer.shutdown();
  }
}
```
### 3 消费消息
``` java
public class Consumer {
  public static void main(String[] args) throws InterruptedException, MQClientException {
    // Instantiate with specified consumer group name
    DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");
    
    // Specify name server addresses
    consumer.setNamesrvAddr("localhost:9876");

    // Subscribe one or more topics and tags for finding those messages need to be consumed
    consumer.subscribe("TopicTest", "*");
    // Register callback to execute on arrival of messages fetched from brokers
    consumer.registerMessageListener(new MessageListenerConcurrently() {
      @Override
      public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
        // Mark the message that have been consumed successfully
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
      }
    });
    // Launch the consumer instance
    consumer.start();
    System.out.printf("Consumer Started.%n");
  }
}
```