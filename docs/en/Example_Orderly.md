# Example for Ordered Messages

RocketMQ provides ordered messages using FIFO order. All related messages need to be sent into the same message queue in an orderly manner.

The following demonstrates ordered messages by ensuring order of create, pay, send and finish steps of sales order process.

## 1 produce ordered messages

```java
package org.apache.rocketmq.example.order2

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/*
* ordered messages producer
*/
public class Producer {

    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        String[] tags = new String[]{"TagA", "TagC", "TagD"};
        // sales orders list
        List<OrderStep> orderList = new Producer().buildOrders();

        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateStr = sdf.format(date);

        for (int i = 0; i < 10; i++) {
            // generate message timestamp
            String body = dateStr + " Hello RocketMQ " + orderList.get(i);
            Message msg = new Message("TopicTest", tags[i % tags.length], "KEY" + i, body.getBytes());

            SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Long id = (Long) arg;  //message queue is selected by #salesOrderID
                    long index = id % mqs.size();
                    return mqs.get((int) index);
                }
            }, orderList.get(i).getOrderId());

            System.out.println(String.format("SendResult status:%s, queueId:%d, body:%s",
                    sendResult.getSendStatus(),
                    sendResult.getMessageQueue().getQueueId(),
                    body));
        }

        producer.shutdown();
    }

    /**
     * each sales order step
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
     * to generate ten OrderStep objects for three sales orders:
     *  #SalesOrder "15103111039L": create, pay, send, finish;
     *  #SalesOrder "15103111065L": create, pay, finish;
     *  #SalesOrder "15103117235L": create, pay, finish;
     */    
	private List<OrderStep> buildOrders() {
		
        List<OrderStep> orderList = new ArrayList<OrderStep>();

        //create sales order with orderid="15103111039L"
        OrderStep orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("create");
        orderList.add(orderDemo);

        //create sales order with orderid="15103111065L"
        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111065L);
        orderDemo.setDesc("create");
        orderList.add(orderDemo);

        //pay sales order #"15103111039L"
        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("pay");
        orderList.add(orderDemo);

        //create sales order with orderid="15103117235L"
        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103117235L);
        orderDemo.setDesc("create");
        orderList.add(orderDemo);

        //pay sales order #"15103111065L"
        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111065L);
        orderDemo.setDesc("pay");
        orderList.add(orderDemo);

        //pay sales order #"15103117235L"
        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103117235L);
        orderDemo.setDesc("pay");
        orderList.add(orderDemo);

        //mark sales order #"15103111065L" as "finish"
        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111065L);
        orderDemo.setDesc("finish");
        orderList.add(orderDemo);

        //mark mark sales order #"15103111039L" as "send"
        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("send");
        orderList.add(orderDemo);

        ////mark sales order #"15103117235L" as "finish"
        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103117235L);
        orderDemo.setDesc("finish");
        orderList.add(orderDemo);

        //mark sales order #"15103111039L" as "finish"
        orderDemo = new OrderStep();
        orderDemo.setOrderId(15103111039L);
        orderDemo.setDesc("finish");
        orderList.add(orderDemo);

        return orderList;
    }
}

```

## 2 Consume ordered messages

```java

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
 * consume messages in order
 */
public class ConsumerInOrder {

    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_3");
        consumer.setNamesrvAddr("127.0.0.1:9876");
        /**
         * when the consumer is first run, the start point of message queue where it can get messages will be set.
         * or if it is restarted, it will continue from the last place to get messages.
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.subscribe("TopicTest", "TagA || TagC || TagD");

        consumer.registerMessageListener(new MessageListenerOrderly() {

            Random random = new Random();

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
                for (MessageExt msg : msgs) {
                    // one consumer for each message queue, and messages order are kept in a single message queue.
                    System.out.println("consumeThread=" + Thread.currentThread().getName() + "queueId=" + msg.getQueueId() + ", content:" + new String(msg.getBody()));
                }

                try {
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

   
