#  Best practices

## 1 Producer
### 1.1 Attention of send message

#### 1  Uses of tags
An application should use one topic as far as possible, but identify the message's subtype with tags.Tags can be set freely by the application.
Only when producers set tags while sending messages, can consumers to filter messages through broker with tags when subscribing messages: message.setTags("TagA").  
 
#### 2 Uses of keys
The unique identifier for each message at the business level set to the Keys field to help locate message loss problems in the future.
The server creates an index(hash index) for each message, and the application can query the message content via Topic,key,and who consumed the message.
Since it is a hash index, make sure that the key is as unique as possible to avoid potential hash conflicts.

```java
   // order id 
   String orderId = "20034568923546";   
   message.setKeys(orderId);   
```
#### 3 Log print
Print the message log when send success or failed, make sure to print the SendResult and key fields. 
Send messages is successful as long as it does not throw exception. Send successful will have multiple states defined in sendResult.
Each state is describing below:     

- **SEND_OK**

Message send successfully.Note that even though message send successfully, but it doesn't mean than it is reliable.
To make sure nothing lost, you should also enable the SYNC_MASTER or SYNC_FLUSH.

- **FLUSH_DISK_TIMEOUT**

Message send successfully, but the server flush messages to disk timeout.At this point, the message has entered the server's memory, and the message will be lost only when the server is down.
Flush mode and sync flush time interval can be set in the configuration parameters. It will return FLUSH_DISK_TIMEOUT when Broker server doesn't finish flush message to disk in timout(default is 5s
) when sets FlushDiskType=SYNC_FLUSH(default is async flush).

- **FLUSH_SLAVE_TIMEOUT**

Message send successfully, but sync to slave timeout.At this point, the message has entered the server's memory, and the message will be lost only when the server is down.
It will return FLUSH_SLAVE_TIMEOUT when Broker server role is SYNC_MASTER(default is ASYNC_MASTER),and it doesn't sync message to slave successfully in the timeout(default 5s).

- **SLAVE_NOT_AVAILABLE**

Message send successfully, but slave is not available.It will return SLAVE_NOT_AVAILABLE when Broker role is SYNC_MASTER(default is ASYNC_MASTER), and it doesn't have a slave server available. 

### 1.2 Handling of message send failure
Send method of producer itself supports internal retry. The logic of retry is as follows:
- At most twice.
- Try next broker when sync send mode, try current broker when async mode. The total elapsed time of this method does not exceed the value of sendMsgTimeout(default is 10s).
- It will not be retried when the message is sent to the Broker with a timeout exception.

The strategy above ensures the success of message sending to some extent.If the business has a high requirement for message reliability, it is recommended to add the corresponding retry logic:
for example, if the sync send method fails, try to store the message in DB, and then retry periodically by the bg thread to ensure the message must send to broker successfully. 

Why the above DB retry strategy is not integrated into the MQ client, but requires the application to complete it by itself is mainly based on the following considerations:
First, the MQ client is designed to be stateless mode, convenient for arbitrary horizontal expansion, and only consumes CPU, memory and network resources.
Second, if the MQ client internal integration a KV storage module, the data can only be relatively reliable when sync flush to disk, but the sync flush will cause performance lose, so it's usually
 use async flush.Because the application shutdown is not controlled by the MQ operators, A violent shutdown like kill -9 may often occur, resulting in data not flushed to disk and being lost.
Thirdly, the producer is a virtual machine with low reliability, which is not suitable for storing important data.
In conclusion, it is recommended that the retry process must be controlled by the application.

### 1.3 Send message by oneway
Typically, this is the process by which messages are sent：

- Client send request to server
- Server process request
- Server response to client 
So, the time taken to send a message is the sum of the three steps above.Some scenarios require very little time, but not much reliability, such as log collect application.
This type application can use oneway to send messages. Oneway only send request without waiting for a reply, and send a request at the client implementation level is simply the overhead of an
 operating system call that writes data to the client's socket buffer, this process that typically takes microseconds.

## 2 Consumer

## 3 Broker

### 3.1 Broker Role

### 3.2 FlushDiskType

### 3.3 Broker Configuration
| Parameter name                           | Default                        | Description                                                         |
| -------------------------------- | ----------------------------- | ------------------------------------------------------------ |
| listenPort                    | 10911              | listen port for client |
| namesrvAddr       | null                         | name server address     |
| brokerIP1 | InetAddress for network interface                         | Should be configured if having multiple addresses |
| brokerIP2 | InetAddress for network interface                         | If configured for the Master broker in the Master/Slave cluster, slave broker will connect to this port for data synchronization   |
| brokerName        | null                         | broker name                           |
| brokerClusterName                     | DefaultCluster                  | this broker belongs to which cluster           |
| brokerId             | 0                              | broker id, 0 means master, positive integers mean slave                                                 |
| storePathCommitLog                      | $HOME/store/commitlog/                              | file path for commit log                                                 |
| storePathConsumerQueue                   | $HOME/store/consumequeue/                              | file path for consume queue                                              |
| mappedFileSizeCommitLog     | 1024 * 1024 * 1024(1G) | mapped file size for commit log                                        |​ 
| deleteWhen     | 04 | When to delete the commitlog which is out of the reserve time                                        |​ 
| fileReserverdTime     | 72 | The number of hours to keep a commitlog before deleting it                                        |​ 
| brokerRole     | ASYNC_MASTER | SYNC_MASTER/ASYNC_MASTER/SLAVE                                        |​ 
| flushDiskType     | ASYNC_FLUSH | {SYNC_FLUSH/ASYNC_FLUSH}. Broker of SYNC_FLUSH mode flushes each message onto disk before acknowledging producer. Broker of ASYNC_FLUSH mode, on the other hand, takes advantage of group-committing, achieving better performance.                                        |​