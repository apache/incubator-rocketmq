# 样例
-----
## 1 基本样例


在基本样例中我们提供如下的功能场景：

* 使用RocketMQ发送三种类型的消息：同步消息、异步消息和单向消息。其中前两种消息是可靠的，因为会有发送是否成功的应答。
* 使用RocketMQ来消费接收到的消息。

### 1.1 加入依赖：

`maven:`
```
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-client</artifactId>
    <version>4.3.0</version>
</dependency>
```
`gradle`
```
compile 'org.apache.rocketmq:rocketmq-client:4.3.0'
```
### 1.2 消息发送

#### 1、Producer端发送同步消息

这种可靠性同步地发送方式使用的比较广泛，比如：重要的消息通知，短信通知。
```java
public class SyncProducer {
	public static void main(String[] args) throws Exception {
    	// 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
    	// 设置NameServer的地址
    	producer.setNamesrvAddr("localhost:9876");
    	// 启动Producer实例
        producer.start();
    	for (int i = 0; i < 100; i++) {
    	    // 创建消息，并指定Topic，Tag和消息体
    	    Message msg = new Message("TopicTest" /* Topic */,
        	"TagA" /* Tag */,
        	("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
        	);
        	// 发送消息到一个Broker
            SendResult sendResult = producer.send(msg);
            // 通过sendResult返回消息是否成功送达
            System.out.printf("%s%n", sendResult);
    	}
    	// 如果不再发送消息，关闭Producer实例。
    	producer.shutdown();
    }
}
```
#### 2、发送异步消息

异步消息通常用在对响应时间敏感的业务场景，即发送端不能容忍长时间地等待Broker的响应。

```java
public class AsyncProducer {
	public static void main(String[] args) throws Exception {
    	// 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
    	// 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");
    	// 启动Producer实例
        producer.start();
        producer.setRetryTimesWhenSendAsyncFailed(0);
	
	int messageCount = 100;
        // 根据消息数量实例化倒计时计算器
	final CountDownLatch2 countDownLatch = new CountDownLatch2(messageCount);
    	for (int i = 0; i < messageCount; i++) {
                final int index = i;
            	// 创建消息，并指定Topic，Tag和消息体
                Message msg = new Message("TopicTest",
                    "TagA",
                    "OrderID188",
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                // SendCallback接收异步返回结果的回调
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
	// 等待5s
	countDownLatch.await(5, TimeUnit.SECONDS);
    	// 如果不再发送消息，关闭Producer实例。
    	producer.shutdown();
    }
}
```

#### 3、单向发送消息

这种方式主要用在不特别关心发送结果的场景，例如日志发送。

```java
public class OnewayProducer {
	public static void main(String[] args) throws Exception{
    	// 实例化消息生产者Producer
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
    	// 设置NameServer的地址
        producer.setNamesrvAddr("localhost:9876");
    	// 启动Producer实例
        producer.start();
    	for (int i = 0; i < 100; i++) {
        	// 创建消息，并指定Topic，Tag和消息体
        	Message msg = new Message("TopicTest" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
        	);
        	// 发送单向消息，没有任何返回结果
        	producer.sendOneway(msg);

    	}
    	// 如果不再发送消息，关闭Producer实例。
    	producer.shutdown();
    }
}
```

### 1.3 消费消息

```java
public class Consumer {

	public static void main(String[] args) throws InterruptedException, MQClientException {

    	// 实例化消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

    	// 设置NameServer的地址
        consumer.setNamesrvAddr("localhost:9876");

    	// 订阅一个或者多个Topic，以及Tag来过滤需要消费的消息
        consumer.subscribe("TopicTest", "*");
    	// 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                // 标记该消息已经被成功消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
	}
}
```

2 顺序消息样例
----------

消息有序指的是可以按照消息的发送顺序来消费(FIFO)。RocketMQ可以严格的保证消息有序，可以分为分区有序或者全局有序。

顺序消费的原理解析，在默认的情况下消息发送会采取Round Robin轮询方式把消息发送到不同的queue(分区队列)；而消费消息的时候从多个queue上拉取消息，这种情况发送和消费是不能保证顺序。但是如果控制发送的顺序消息只依次发送到同一个queue中，消费的时候只从这个queue上依次拉取，则就保证了顺序。当发送和消费参与的queue只有一个，则是全局有序；如果多个queue参与，则为分区有序，即相对每个queue，消息都是有序的。

下面用订单进行分区有序的示例。一个订单的顺序流程是：创建、付款、推送、完成。订单号相同的消息会被先后发送到同一个队列中，消费时，同一个OrderId获取到的肯定是同一个队列。

### 2.1 顺序消息生产

```java
package org.apache.rocketmq.example.order2;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
* Producer，发送顺序消息
*/
public class Producer {

   public static void main(String[] args) throws Exception {
       DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

       producer.setNamesrvAddr("127.0.0.1:9876");

       producer.start();

       String[] tags = new String[]{"TagA", "TagC", "TagD"};

       // 订单列表
       List<OrderStep> orderList = new Producer().buildOrders();

       Date date = new Date();
       SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
       String dateStr = sdf.format(date);
       for (int i = 0; i < 10; i++) {
           // 加个时间前缀
           String body = dateStr + " Hello RocketMQ " + orderList.get(i);
           Message msg = new Message("TopicTest", tags[i % tags.length], "KEY" + i, body.getBytes());

           SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
               @Override
               public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                   Long id = (Long) arg;  //根据订单id选择发送queue
                   long index = id % mqs.size();
                   return mqs.get((int) index);
               }
           }, orderList.get(i).getOrderId());//订单id

           System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
               sendResult.getSendStatus(),
               sendResult.getMessageQueue().getQueueId(),
               body));
       }

       producer.shutdown();
   }

   /**
    * 订单的步骤
    */
   private static class OrderStep {
       private long orderId;
       private String desc;

       public long getOrderId() {
           return orderId;
       }

       public void setOrderId(long orderId) {
           this.orderId = orderId;
       }

       public String getDesc() {
           return desc;
       }

       public void setDesc(String desc) {
           this.desc = desc;
       }

       @Override
       public String toString() {
           return "OrderStep{" +
               "orderId=" + orderId +
               ", desc='" + desc + '\'' +
               '}';
       }
   }

   /**
    * 生成模拟订单数据
    */
   private List<OrderStep> buildOrders() {
       List<OrderStep> orderList = new ArrayList<OrderStep>();

       OrderStep orderDemo = new OrderStep();
       orderDemo.setOrderId(15103111039L);
       orderDemo.setDesc("创建");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(15103111065L);
       orderDemo.setDesc("创建");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(15103111039L);
       orderDemo.setDesc("付款");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(15103117235L);
       orderDemo.setDesc("创建");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(15103111065L);
       orderDemo.setDesc("付款");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(15103117235L);
       orderDemo.setDesc("付款");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(15103111065L);
       orderDemo.setDesc("完成");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(15103111039L);
       orderDemo.setDesc("推送");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(15103117235L);
       orderDemo.setDesc("完成");
       orderList.add(orderDemo);

       orderDemo = new OrderStep();
       orderDemo.setOrderId(15103111039L);
       orderDemo.setDesc("完成");
       orderList.add(orderDemo);

       return orderList;
   }
}
```

### 2.2 顺序消费消息

```java
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import java.util.List;

package org.apache.rocketmq.example.order2;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
* 顺序消息消费，带事务方式（应用可控制Offset什么时候提交）
*/
public class ConsumerInOrder {

   public static void main(String[] args) throws Exception {
       DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_3");
       consumer.setNamesrvAddr("127.0.0.1:9876");
       /**
        * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
        * 如果非第一次启动，那么按照上次消费的位置继续消费
        */
       consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

       consumer.subscribe("TopicTest", "TagA || TagC || TagD");

       consumer.registerMessageListener(new MessageListenerOrderly() {

           Random random = new Random();

           @Override
           public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
               context.setAutoCommit(true);
               for (MessageExt msg : msgs) {
                   // 可以看到每个queue有唯一的consume线程来消费, 订单对每个queue(分区)有序
                   System.out.println("consumeThread=" + Thread.currentThread().getName() + "queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
               }

               try {
                   //模拟业务逻辑处理中...
                   TimeUnit.SECONDS.sleep(random.nextInt(10));
               } catch (Exception e) {
                   e.printStackTrace();
               }
               return ConsumeOrderlyStatus.SUCCESS;
           }
       });

       consumer.start();

       System.out.println("Consumer Started.");
   }
}
```

3 延时消息样例
----------

### 3.1 启动消费者等待传入订阅消息

```java

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import java.util.List;

public class ScheduledMessageConsumer {
   public static void main(String[] args) throws Exception {
      // 实例化消费者
      DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("ExampleConsumer");
      // 订阅Topics
      consumer.subscribe("TestTopic", "*");
      // 注册消息监听者
      consumer.registerMessageListener(new MessageListenerConcurrently() {
          @Override
          public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
              for (MessageExt message : messages) {
                  // Print approximate delay time period
                  System.out.println("Receive message[msgId=" + message.getMsgId() + "] " + (System.currentTimeMillis() - message.getStoreTimestamp()) + "ms later");
              }
              return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
          }
      });
      // 启动消费者
      consumer.start();
  }
}

```

### 3.2 发送延时消息

```java

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

public class ScheduledMessageProducer {
   public static void main(String[] args) throws Exception {
      // 实例化一个生产者来产生延时消息
      DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
      // 启动生产者
      producer.start();
      int totalMessagesToSend = 100;
      for (int i = 0; i < totalMessagesToSend; i++) {
          Message message = new Message("TestTopic", ("Hello scheduled message " + i).getBytes());
          // 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
          message.setDelayTimeLevel(3);
          // 发送消息
          producer.send(message);
      }
       // 关闭生产者
      producer.shutdown();
  }
}
```

### 3.3 验证

您将会看到消息的消费比存储时间晚10秒。

### 3.4 延时消息的使用场景
比如电商里，提交了一个订单就可以发送一个延时消息，1h后去检查这个订单的状态，如果还是未付款就取消订单释放库存。

### 3.5 延时消息的使用限制

```java
// org/apache/rocketmq/store/config/MessageStoreConfig.java

private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
```
现在RocketMq并不支持任意时间的延时，需要设置几个固定的延时等级，从1s到2h分别对应着等级1到18
消息消费失败会进入延时消息队列，消息发送时间与设置的延时等级和重试次数有关，详见代码`SendMessageProcessor.java`


4 批量消息样例
----------

批量发送消息能显著提高传递小消息的性能。限制是这些批量消息应该有相同的topic，相同的waitStoreMsgOK，而且不能是延时消息。此外，这一批消息的总大小不应超过4MB。

### 4.1 发送批量消息

如果您每次只发送不超过4MB的消息，则很容易使用批处理，样例如下：

```java
String topic = "BatchTest";
List<Message> messages = new ArrayList<>();
messages.add(new Message(topic, "TagA", "OrderID001", "Hello world 0".getBytes()));
messages.add(new Message(topic, "TagA", "OrderID002", "Hello world 1".getBytes()));
messages.add(new Message(topic, "TagA", "OrderID003", "Hello world 2".getBytes()));
try {
   producer.send(messages);
} catch (Exception e) {
   e.printStackTrace();
   //处理error
}

```

### 4.2 消息列表分割

复杂度只有当你发送大批量时才会增长，你可能不确定它是否超过了大小限制（4MB）。这时候你最好把你的消息列表分割一下：

```java

public class ListSplitter implements Iterator<List<Message>> {
   private final int SIZE_LIMIT = 1024 * 1024 * 4;
   private final List<Message> messages;
   private int currIndex;
   public ListSplitter(List<Message> messages) {
           this.messages = messages;
   }
   @Override public boolean hasNext() {
       return currIndex < messages.size();
   }
   @Override public List<Message> next() {
       int nextIndex = currIndex;
       int totalSize = 0;
       for (; nextIndex < messages.size(); nextIndex++) {
           Message message = messages.get(nextIndex);
           int tmpSize = message.getTopic().length() + message.getBody().length;
           Map<String, String> properties = message.getProperties();
           for (Map.Entry<String, String> entry : properties.entrySet()) {
               tmpSize += entry.getKey().length() + entry.getValue().length();
           }
           tmpSize = tmpSize + 20; // 增加日志的开销20字节
           if (tmpSize > SIZE_LIMIT) {
               //单个消息超过了最大的限制
               //忽略,否则会阻塞分裂的进程
               if (nextIndex - currIndex == 0) {
                  //假如下一个子列表没有元素,则添加这个子列表然后退出循环,否则只是退出循环
                  nextIndex++;
               }
               break;
           }
           if (tmpSize + totalSize > SIZE_LIMIT) {
               break;
           } else {
               totalSize += tmpSize;
           }

       }
       List<Message> subList = messages.subList(currIndex, nextIndex);
       currIndex = nextIndex;
       return subList;
   }
}
//把大的消息分裂成若干个小的消息
ListSplitter splitter = new ListSplitter(messages);
while (splitter.hasNext()) {
  try {
      List<Message>  listItem = splitter.next();
      producer.send(listItem);
  } catch (Exception e) {
      e.printStackTrace();
      //处理error
  }
}
```

5 过滤消息样例
----------

在大多数情况下，TAG是一个简单而有用的设计，其可以来选择您想要的消息。例如：

```java
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("CID_EXAMPLE");
consumer.subscribe("TOPIC", "TAGA || TAGB || TAGC");
```

消费者将接收包含TAGA或TAGB或TAGC的消息。但是限制是一个消息只能有一个标签，这对于复杂的场景可能不起作用。在这种情况下，可以使用SQL表达式筛选消息。SQL特性可以通过发送消息时的属性来进行计算。在RocketMQ定义的语法下，可以实现一些简单的逻辑。下面是一个例子：
```
------------
| message  |
|----------|  a > 5 AND b = 'abc'
| a = 10   |  --------------------> Gotten
| b = 'abc'|
| c = true |
------------
------------
| message  |
|----------|   a > 5 AND b = 'abc'
| a = 1    |  --------------------> Missed
| b = 'abc'|
| c = true |
------------
```
### 5.1 基本语法

RocketMQ只定义了一些基本语法来支持这个特性。你也可以很容易地扩展它。

- 数值比较，比如：**>，>=，<，<=，BETWEEN，=；**
- 字符比较，比如：**=，<>，IN；**
- **IS NULL** 或者 **IS NOT NULL；**
- 逻辑符号 **AND，OR，NOT；**

常量支持类型为：

- 数值，比如：**123，3.1415；**
- 字符，比如：**'abc'，必须用单引号包裹起来；**
- **NULL**，特殊的常量
- 布尔值，**TRUE** 或 **FALSE**

只有使用push模式的消费者才能用使用SQL92标准的sql语句，接口如下：
```
public void subscribe(finalString topic, final MessageSelector messageSelector)
```

### 5.2 使用样例

#### 1、生产者样例

发送消息时，你能通过`putUserProperty`来设置消息的属性

```java
DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
producer.start();
Message msg = new Message("TopicTest",
   tag,
   ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET)
);
// 设置一些属性
msg.putUserProperty("a", String.valueOf(i));
SendResult sendResult = producer.send(msg);

producer.shutdown();
```

#### 2、消费者样例

用MessageSelector.bySql来使用sql筛选消息

```java
DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");
// 只有订阅的消息有这个属性a, a >=0 and a <= 3
consumer.subscribe("TopicTest", MessageSelector.bySql("a between 0 and 3");
consumer.registerMessageListener(new MessageListenerConcurrently() {
   @Override
   public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
       return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
   }
});
consumer.start();

```

6 消息事务样例
----------

事务消息共有三种状态，提交状态、回滚状态、中间状态：

- TransactionStatus.CommitTransaction: 提交事务，它允许消费者消费此消息。
- TransactionStatus.RollbackTransaction: 回滚事务，它代表该消息将被删除，不允许被消费。
- TransactionStatus.Unknown: 中间状态，它代表需要检查消息队列来确定状态。

### 6.1 发送事务消息样例

#### 1、创建事务性生产者

使用 `TransactionMQProducer`类创建生产者，并指定唯一的 `ProducerGroup`，就可以设置自定义线程池来处理这些检查请求。执行本地事务后、需要根据执行结果对消息队列进行回复。回传的事务状态在请参考前一节。

```java
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import java.util.List;
public class TransactionProducer {
   public static void main(String[] args) throws MQClientException, InterruptedException {
       TransactionListener transactionListener = new TransactionListenerImpl();
       TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
       ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
           @Override
           public Thread newThread(Runnable r) {
               Thread thread = new Thread(r);
               thread.setName("client-transaction-msg-check-thread");
               return thread;
           }
       });
       producer.setExecutorService(executorService);
       producer.setTransactionListener(transactionListener);
       producer.start();
       String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
       for (int i = 0; i < 10; i++) {
           try {
               Message msg =
                   new Message("TopicTest1234", tags[i % tags.length], "KEY" + i,
                       ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
               SendResult sendResult = producer.sendMessageInTransaction(msg, null);
               System.out.printf("%s%n", sendResult);
               Thread.sleep(10);
           } catch (MQClientException | UnsupportedEncodingException e) {
               e.printStackTrace();
           }
       }
       for (int i = 0; i < 100000; i++) {
           Thread.sleep(1000);
       }
       producer.shutdown();
   }
}

```
#### 2、实现事务的监听接口

当发送半消息成功时，我们使用 `executeLocalTransaction` 方法来执行本地事务。它返回前一节中提到的三个事务状态之一。`checkLocalTranscation` 方法用于检查本地事务状态，并回应消息队列的检查请求。它也是返回前一节中提到的三个事务状态之一。

```java
public class TransactionListenerImpl implements TransactionListener {
  private AtomicInteger transactionIndex = new AtomicInteger(0);
  private ConcurrentHashMap<String, Integer> localTrans = new ConcurrentHashMap<>();
  @Override
  public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
      int value = transactionIndex.getAndIncrement();
      int status = value % 3;
      localTrans.put(msg.getTransactionId(), status);
      return LocalTransactionState.UNKNOW;
  }
  @Override
  public LocalTransactionState checkLocalTransaction(MessageExt msg) {
      Integer status = localTrans.get(msg.getTransactionId());
      if (null != status) {
          switch (status) {
              case 0:
                  return LocalTransactionState.UNKNOW;
              case 1:
                  return LocalTransactionState.COMMIT_MESSAGE;
              case 2:
                  return LocalTransactionState.ROLLBACK_MESSAGE;
          }
      }
      return LocalTransactionState.COMMIT_MESSAGE;
  }
}

```

### 6.2 事务消息使用上的限制

1. 事务消息不支持延时消息和批量消息。
2. 为了避免单个消息被检查太多次而导致半队列消息累积，我们默认将单个消息的检查次数限制为 15 次，但是用户可以通过 Broker 配置文件的 `transactionCheckMax`参数来修改此限制。如果已经检查某条消息超过 N 次的话（ N = `transactionCheckMax` ） 则 Broker 将丢弃此消息，并在默认情况下同时打印错误日志。用户可以通过重写 `AbstractTransactionCheckListener` 类来修改这个行为。
3. 事务消息将在 Broker 配置文件中的参数 transactionMsgTimeout 这样的特定时间长度之后被检查。当发送事务消息时，用户还可以通过设置用户属性 CHECK_IMMUNITY_TIME_IN_SECONDS 来改变这个限制，该参数优先于 `transactionMsgTimeout` 参数。
4. 事务性消息可能不止一次被检查或消费。
5. 提交给用户的目标主题消息可能会失败，目前这依日志的记录而定。它的高可用性通过 RocketMQ 本身的高可用性机制来保证，如果希望确保事务消息不丢失、并且事务完整性得到保证，建议使用同步的双重写入机制。
6. 事务消息的生产者 ID 不能与其他类型消息的生产者 ID 共享。与其他类型的消息不同，事务消息允许反向查询、MQ服务器能通过它们的生产者 ID 查询到消费者。

7 Logappender样例
-----------------

RocketMQ日志提供log4j、log4j2和logback日志框架作为业务应用，下面是配置样例

### 7.1 log4j样例

按下面样例使用log4j属性配置
```
log4j.appender.mq=org.apache.rocketmq.logappender.log4j.RocketmqLog4jAppender
log4j.appender.mq.Tag=yourTag
log4j.appender.mq.Topic=yourLogTopic
log4j.appender.mq.ProducerGroup=yourLogGroup
log4j.appender.mq.NameServerAddress=yourRocketmqNameserverAddress
log4j.appender.mq.layout=org.apache.log4j.PatternLayout
log4j.appender.mq.layout.ConversionPattern=%d{yyyy-MM-dd HH:mm:ss} %-4r [%t] (%F:%L) %-5p - %m%n
```
按下面样例使用log4j xml配置来使用异步添加日志
```
<appender name="mqAppender1"class="org.apache.rocketmq.logappender.log4j.RocketmqLog4jAppender">
  <param name="Tag" value="yourTag" />
  <param name="Topic" value="yourLogTopic" />
  <param name="ProducerGroup" value="yourLogGroup" />
  <param name="NameServerAddress" value="yourRocketmqNameserverAddress"/>
  <layout class="org.apache.log4j.PatternLayout">
      <param name="ConversionPattern" value="%d{yyyy-MM-dd HH:mm:ss}-%p %t %c - %m%n" />
  </layout>
</appender>
<appender name="mqAsyncAppender1"class="org.apache.log4j.AsyncAppender">
  <param name="BufferSize" value="1024" />
  <param name="Blocking" value="false" />
  <appender-ref ref="mqAppender1"/>
</appender>
```
### 7.2 log4j2样例

用log4j2时，配置如下，如果想要非阻塞，只需要使用异步添加引用即可
```
<RocketMQ name="rocketmqAppender" producerGroup="yourLogGroup" nameServerAddress="yourRocketmqNameserverAddress"
   topic="yourLogTopic" tag="yourTag">
  <PatternLayout pattern="%d [%p] hahahah %c %m%n"/>
</RocketMQ>
```
### 7.3 logback样例
```
<appender name="mqAppender1"class="org.apache.rocketmq.logappender.logback.RocketmqLogbackAppender">
  <tag>yourTag</tag>
  <topic>yourLogTopic</topic>
  <producerGroup>yourLogGroup</producerGroup>
  <nameServerAddress>yourRocketmqNameserverAddress</nameServerAddress>
  <layout>
      <pattern>%date %p %t - %m%n</pattern>
  </layout>
</appender>
<appender name="mqAsyncAppender1"class="ch.qos.logback.classic.AsyncAppender">
  <queueSize>1024</queueSize>
  <discardingThreshold>80</discardingThreshold>
  <maxFlushTime>2000</maxFlushTime>
  <neverBlock>true</neverBlock>
  <appender-ref ref="mqAppender1"/>
</appender>
```

8 OpenMessaging样例
---------------

 [OpenMessaging](https://www.google.com/url?q=http://openmessaging.cloud/&sa=D&ust=1546524111089000)旨在建立消息和流处理规范，以为金融、电子商务、物联网和大数据领域提供通用框架及工业级指导方案。在分布式异构环境中，设计原则是面向云、简单、灵活和独立于语言。符合这些规范将帮助企业方便的开发跨平台和操作系统的异构消息传递应用程序。提供了openmessaging-api 0.3.0-alpha的部分实现，下面的示例演示如何基于OpenMessaging访问RocketMQ。

### 8.1 OMSProducer样例

下面的示例演示如何在同步、异步或单向传输中向RocketMQ代理发送消息。

```java
import io.openmessaging.Future;
import io.openmessaging.FutureListener;
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

public class SimpleProducer {
    public static void main(String[] args) {
       final MessagingAccessPoint messagingAccessPoint =
           OMS.getMessagingAccessPoint("oms:rocketmq://localhost:9876/default:default");
       final Producer producer = messagingAccessPoint.createProducer();
       messagingAccessPoint.startup();
       System.out.printf("MessagingAccessPoint startup OK%n");
       producer.startup();
       System.out.printf("Producer startup OK%n");
       {
           Message message = producer.createBytesMessage("OMS_HELLO_TOPIC", "OMS_HELLO_BODY".getBytes(Charset.forName("UTF-8")));
           SendResult sendResult = producer.send(message);
           //final Void aVoid = result.get(3000L);
           System.out.printf("Send async message OK, msgId: %s%n", sendResult.messageId());
       }
       final CountDownLatch countDownLatch = new CountDownLatch(1);
       {
           final Future<SendResult> result = producer.sendAsync(producer.createBytesMessage("OMS_HELLO_TOPIC", "OMS_HELLO_BODY".getBytes(Charset.forName("UTF-8"))));
           result.addListener(new FutureListener<SendResult>() {
               @Override
               public void operationComplete(Future<SendResult> future) {
                   if (future.getThrowable() != null) {
                       System.out.printf("Send async message Failed, error: %s%n", future.getThrowable().getMessage());
                   } else {
                       System.out.printf("Send async message OK, msgId: %s%n", future.get().messageId());
                   }
                   countDownLatch.countDown();
               }
           });
       }
       {
           producer.sendOneway(producer.createBytesMessage("OMS_HELLO_TOPIC", "OMS_HELLO_BODY".getBytes(Charset.forName("UTF-8"))));
           System.out.printf("Send oneway message OK%n");
       }
       try {
           countDownLatch.await();
           Thread.sleep(500); // 等一些时间来发送消息
       } catch (InterruptedException ignore) {
       }
       producer.shutdown();
   }
}
```

### 8.2 OMSPullConsumer

用OMS PullConsumer 来从指定的队列中拉取消息

```java
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.PullConsumer;
import io.openmessaging.producer.Producer;
import io.openmessaging.producer.SendResult;

public class SimplePullConsumer {
    public static void main(String[] args) {
       final MessagingAccessPoint messagingAccessPoint =
           OMS.getMessagingAccessPoint("oms:rocketmq://localhost:9876/default:default");
       messagingAccessPoint.startup();
       final Producer producer = messagingAccessPoint.createProducer();
       final PullConsumer consumer = messagingAccessPoint.createPullConsumer(
           OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, "OMS_CONSUMER"));
       messagingAccessPoint.startup();
       System.out.printf("MessagingAccessPoint startup OK%n");
       final String queueName = "TopicTest";
       producer.startup();
       Message msg = producer.createBytesMessage(queueName, "Hello Open Messaging".getBytes());
       SendResult sendResult = producer.send(msg);
       System.out.printf("Send Message OK. MsgId: %s%n", sendResult.messageId());
       producer.shutdown();
       consumer.attachQueue(queueName);
       consumer.startup();
       System.out.printf("Consumer startup OK%n");
       // 运行直到发现一个消息被发送了
       boolean stop = false;
       while (!stop) {
           Message message = consumer.receive();
           if (message != null) {
               String msgId = message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID);
               System.out.printf("Received one message: %s%n", msgId);
               consumer.ack(msgId);
               if (!stop) {
                   stop = msgId.equalsIgnoreCase(sendResult.messageId());
               }
           } else {
               System.out.printf("Return without any message%n");
           }
       }
       consumer.shutdown();
       messagingAccessPoint.shutdown();
   }
}
```

### 8.3 OMSPushConsumer

以下示范如何将 OMS PushConsumer 添加到指定的队列，并通过 MessageListener 消费这些消息。

```java
import io.openmessaging.Message;
import io.openmessaging.MessagingAccessPoint;
import io.openmessaging.OMS;
import io.openmessaging.OMSBuiltinKeys;
import io.openmessaging.consumer.MessageListener;
import io.openmessaging.consumer.PushConsumer;

public class SimplePushConsumer {
    public static void main(String[] args) {
       final MessagingAccessPoint messagingAccessPoint = OMS
           .getMessagingAccessPoint("oms:rocketmq://localhost:9876/default:default");
       final PushConsumer consumer = messagingAccessPoint.
           createPushConsumer(OMS.newKeyValue().put(OMSBuiltinKeys.CONSUMER_ID, "OMS_CONSUMER"));
       messagingAccessPoint.startup();
       System.out.printf("MessagingAccessPoint startup OK%n");
       Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
           @Override
           public void run() {
               consumer.shutdown();
               messagingAccessPoint.shutdown();
           }
       }));
       consumer.attachQueue("OMS_HELLO_TOPIC", new MessageListener() {
           @Override
           public void onReceived(Message message, Context context) {
               System.out.printf("Received one message: %s%n", message.sysHeaders().getString(Message.BuiltinKeys.MESSAGE_ID));
               context.ack();
           }
       });
       consumer.startup();
       System.out.printf("Consumer startup OK%n");
   }
}
```
