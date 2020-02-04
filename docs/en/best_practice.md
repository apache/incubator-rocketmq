#  Best practices

## 1 Producer

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