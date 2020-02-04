# Access control list
## Overview
This document focuses on how to quickly deploy and use a RocketMQ cluster that supports the privilege control feature.

## 1. Access control features
Access Control (ACL) mainly provides Topic resource level user access control for RocketMQ.If you want to enable RocketMQ permission control, you can inject the AccessKey and SecretKey signatures through the RPCHook on the Client side.And then, the corresponding permission control attributes (including Topic access rights, IP whitelist and AccessKey and SecretKey signature) are set in the configuration file of distribution/conf/plain_acl.yml.The Broker side will check the permissions owned by the AccessKey, and if the verification fails, an exception is thrown;
The source code about ACL on the Client side can be find in **org.apache.rocketmq.example.simple.AclClient.java**  

## 2. Access control definition and attribute values
### 2.1 Access control definition
The definition of Topic resource access control for RocketMQ is mainly as shown in the following table.

| Permission | explanation |
| --- | --- |
| DENY | permission deny |
| ANY | PUB or SUB permission |
| PUB | Publishing permission |
| SUB | Subscription permission |

### 2.2 Main properties
| key | value | explanation |
| --- | --- | --- |
| globalWhiteRemoteAddresses | string |Global IP whitelist,example:<br>\*; <br>192.168.\*.\*; <br>192.168.0.1 |
| accessKey | string | Access Key |
| secretKey | string | Secret Key |
| whiteRemoteAddress | string | User IP whitelist,example:<br>\*; <br>192.168.\*.\*; <br>192.168.0.1 |
| admin | true;false | Whether an administrator account |
| defaultTopicPerm | DENY;PUB;SUB;PUB\|SUB | Default Topic permission |
| defaultGroupPerm | DENY;PUB;SUB;PUB\|SUB | Default ConsumerGroup permission |
| topicPerms | topic=permission | Topic only permission |
| groupPerms | group=permission | ConsumerGroup only permission |

For details, please refer to the **distribution/conf/plain_acl.yml** configuration file.

## 3. Cluster deployment with permission control
After defining the permission attribute in the **distribution/conf/plain_acl.yml** configuration file as described above, open the **aclEnable** switch variable to enable the ACL feature of the RocketMQ cluster.The configuration file of the ACL feature enabled on the broker is as follows:
```properties
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0
deleteWhen=04
fileReservedTime=48
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
storePathRootDir=/data/rocketmq/rootdir-a-m
storePathCommitLog=/data/rocketmq/commitlog-a-m
autoCreateSubscriptionGroup=true
## if acl is open,the flag will be true
aclEnable=true
listenPort=10911
brokerIP1=XX.XX.XX.XX1
namesrvAddr=XX.XX.XX.XX:9876
```
## 4. Main process of access control
The main ACL process is divided into two parts, including privilege resolution and privilege check.

### 4.1 Privilege resolution
The Broker side parses the client's RequestCommand request and obtains the attribute field that needs to be authenticated.
main attributes:  
 (1) AccessKey:Similar to the user name, on behalf of the user entity, the permission data corresponds to it;  
 (2) Signature:The client obtains the string according to the signature of the SecretKey, and the server uses the SecretKey to perform signature verification.

### 4.2 Privilege check
The check logic of the right side of the broker is mainly divided into the following steps:  
 (1) Check if the global IP whitelist is hit; if yes, the check passes; otherwise, go to step (2);  
 (2) Check if the user IP whitelist is hit; if yes, the check passes; otherwise, go to step (3);  
 (3) Check the signature, if the verification fails, throw an exception; if the verification passes, go to step (4);  
 (4) Check the permissions required by the user request and the permissions owned by the user; if not, throw an exception;  


The verification of the required permissions of the user requires attention to the following points:  
 (1) Special requests such as UPDATE_AND_CREATE_TOPIC can only be operated by the admin account;  
 (2) For a resource, if there is explicit configuration permission, the configured permission is used; if there is no explicit configuration permission, the default permission is adopted;

## 5. Hot loading modified Access control
The default implementation of RocketrMQ's permission control store is based on the yml configuration file. Users can dynamically modify the properties defined by the permission control without restarting the Broker service node.
