# Basic Sample 
------
Two functions below are provided in the basic sample:
* The RocketMQ can be utilized to send messages in three ways: reliable synchronous, reliable asynchronous, and one-way transmission.  The first two message types are reliable because there is a response whether they were sent successfully.
* The RocketMQ can be utilized to consume messages.
### 1 Add Dependency
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
### 2 Send Messages
##### 2.1 Use Producer to Send Synchronous Messages
Reliable synchronous transmission is used in extensive scenes, such as important notification messages, SMS notification.
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
##### 2.2 Send Asynchronous Messages
Asynchronous transmission is generally used in response time sensitive business scenarios. It means that it is unable for the sender to wait the response of the Broker too long.
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
##### 2.3 Send Messages in One-way Mode
One-way transmission is used for cases requiring moderate reliability, such as log collection.
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
### 3 Consume Messages
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