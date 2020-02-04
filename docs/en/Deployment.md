# Deployment Architectures and Setup Steps

## Cluster Setup

### 1 Single Master mode

This is the simplest, but also the riskiest mode, that makes the entire service unavailable once the broker restarts or goes down. Production environments are not recommended, but can be used for local testing and development. Here are the steps to build.

**1）Start NameServer**

```shell
### Start Name Server first
$ nohup sh mqnamesrv &
 
### Then verify that the Name Server starts successfully
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

We can see 'The Name Server boot success.. ' in namesrv.log that indicates the NameServer has been started successfully.

**2）Start Broker**

```shell
### Also start broker first
$ nohup sh bin/mqbroker -n localhost:9876 &

### Then verify that the broker is started successfully, for example, the IP of broker is 192.168.1.2 and the name is broker-a
$ tail -f ~/logs/rocketmqlogs/Broker.log 
The broker[broker-a,192.169.1.2:10911] boot success...
```

We can see 'The broker[brokerName,ip:port] boot success..' in Broker.log that indicates the broker has been started successfully.

### 2 Multiple Master mode

Multiple master mode means a mode with all master nodes(such as 2 or 3 master nodes) and no slave node. The advantages and disadvantages of this mode are as follows:

- Advantages: 
  1. Simple configuration.
  2. Outage or restart(for maintenance) of one master node has no impact on the application. 
  3. When the disk is configured as RAID10, messages are not lost because the RAID10 disk is very reliable, even if the machine is not recoverable (In the case of asynchronous flush disk mode of the message, a small number of messages are lost; If the brush mode of a message is synchronous, no message will be lost).
  4. In this mode, the performance is the highest.
- Disadvantages:
  1. During a single machine outage, messages that are not consumed on this machine are not subscribed to until the machine recovers, and message real-time is affected.

The starting steps for multiple master mode are as follows:

**1）Start NameServer**

```shell
### Start Name Server first
$ nohup sh mqnamesrv &
 
### Then verify that the Name Server starts successfully
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2）Start the Broker cluster**

```shell
### For example, starting the first Master on machine A, assuming that the configured NameServer IP is: 192.168.1.1.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-noslave/broker-a.properties &
 
### Then starting the second Master on machine B, assuming that the configured NameServer IP is: 192.168.1.1.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-noslave/broker-b.properties &

...
```

The boot command shown above is used in the case of a single NameServer.For clusters of multiple NameServer, the address list after the -n argument in the broker boot command is separated by semicolons, for example, 192.168.1.1: 9876;192.161.2: 9876.

### 3 Multiple Master And Multiple Slave Mode-Asynchronous replication

Each master node configures more thran one slave nodes, with multiple pairs of master-slave.HA uses asynchronous replication, with a short message delay (millisecond) between master node and slave node.The advantages and disadvantages of this mode are as follows:

- Advantages: 
  1. Even if the disk is corrupted, very few messages will be lost and the real-time performance of the message will not be affected.
  2. At the same time, when master node is down, consumers can still consume messages from slave node, and the process is transparent to the application itself and does not require human intervention.
  3. Performance is almost as high as multiple master mode.
- Disadvantages:
  1. A small number of messages will be lost when master node is down and the disk is corrupted.

The starting steps for multiple master and multiple slave mode are as follows:

**1）Start NameServer**

```shell
### Start Name Server first
$ nohup sh mqnamesrv &
 
### Then verify that the Name Server starts successfully
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2）Start the Broker cluster**

```shell
### For example, starting the first Master on machine A, assuming that the configured NameServer IP is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-a.properties &
 
### Then starting the second Master on machine B, assuming that the configured NameServer IP is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-b.properties &
 
### Then starting the first Slave on machine C, assuming that the configured NameServer IP is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-a-s.properties &
 
### Last starting the second Slave on machine D, assuming that the configured NameServer IP is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-async/broker-b-s.properties &
```

The above shows a startup command for 2M-2S-Async mode, similar to other nM-nS-Async modes.

### 4 Multiple Master And Multiple Slave Mode-Synchronous dual write

In this mode, multiple slave node are configured for each master node and there are multiple pairs of Master-Slave.HA uses synchronous double-write, that is, the success response will be returned to the application only when the message is successfully written into the master node and replicated to more than one slave node.

The advantages and disadvantages of this model are as follows:

- Advantages: 
  1. Neither the data nor the service has a single point of failure. 
  2. In the case of master node shutdown, the message is also undelayed. 
  3. Service availability and data availability are very high;
- Disadvantages:
  1. The performance in this mode is slightly lower than in asynchronous replication mode (about 10% lower).
  2. The RT sending a single message is slightly higher, and the current version, the slave node cannot automatically switch to the master after the master node is down.

The starting steps are as follows:

**1）Start NameServer**

```shell
### Start Name Server first
$ nohup sh mqnamesrv &
 
### Then verify that the Name Server starts successfully
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2）Start the Broker cluster**

```shell
### For example, starting the first Master on machine A, assuming that the configured NameServer IP is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-a.properties &
 
### Then starting the second Master on machine B, assuming that the configured NameServer IP is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-b.properties &
 
### Then starting the first Slave on machine C, assuming that the configured NameServer IP is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-a-s.properties &
 
### Last starting the second Slave on machine D, assuming that the configured NameServer IP is: 192.168.1.1 and port is 9876.
$ nohup sh mqbroker -n 192.168.1.1:9876 -c $ROCKETMQ_HOME/conf/2m-2s-sync/broker-b-s.properties &
```

The above Master and Slave are paired by specifying the same config named "brokerName", the "brokerId" of the master node must be 0, and the "brokerId" of the slave node must be greater than 0.