# 权限控制
----


## 1.权限控制特性介绍
权限控制（ACL）主要为RocketMQ提供Topic资源级别的用户访问控制。用户在使用RocketMQ权限控制时，可以在Client客户端通过 RPCHook注入AccessKey和SecretKey签名；同时，将对应的权限控制属性（包括Topic访问权限、IP白名单和AccessKey和SecretKey签名等）设置在distribution/conf/plain_acl.yml的配置文件中。Broker端对AccessKey所拥有的权限进行校验，校验不过，抛出异常；
ACL客户端可以参考：**org.apache.rocketmq.example.simple**包下面的**AclClient**代码。

## 2. 权限控制的定义与属性值
### 2.1权限定义
对RocketMQ的Topic资源访问权限控制定义主要如下表所示，分为以下四种


| 权限 | 含义 |
| --- | --- |
| DENY | 拒绝 |
| ANY | PUB 或者 SUB 权限 |
| PUB | 发送权限 |
| SUB | 订阅权限 |

### 2.2 权限定义的关键属性
| 字段 | 取值 | 含义 |
| --- | --- | --- |
| globalWhiteRemoteAddresses | \*;192.168.\*.\*;192.168.0.1 | 全局IP白名单 |
| accessKey | 字符串 | Access Key |
| secretKey | 字符串 | Secret Key |
| whiteRemoteAddress | \*;192.168.\*.\*;192.168.0.1 | 用户IP白名单 |
| admin | true;false | 是否管理员账户 |
| defaultTopicPerm | DENY;PUB;SUB;PUB\|SUB | 默认的Topic权限 |
| defaultGroupPerm | DENY;PUB;SUB;PUB\|SUB | 默认的ConsumerGroup权限 |
| topicPerms | topic=权限 | 各个Topic的权限 |
| groupPerms | group=权限 | 各个ConsumerGroup的权限 |

具体可以参考**distribution/conf/plain_acl.yml**配置文件

## 3. 支持权限控制的集群部署
在**distribution/conf/plain_acl.yml**配置文件中按照上述说明定义好权限属性后，打开**aclEnable**开关变量即可开启RocketMQ集群的ACL特性。这里贴出Broker端开启ACL特性的properties配置文件内容：
```
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

## 4. 权限控制主要流程
ACL主要流程分为两部分，主要包括权限解析和权限校验。

### 4.1 权限解析
Broker端对客户端的RequestCommand请求进行解析，拿到需要鉴权的属性字段。
主要包括：
（1）AccessKey：类似于用户名，代指用户主体，权限数据与之对应；
（2）Signature：客户根据 SecretKey 签名得到的串，服务端再用SecretKey进行签名验证；

### 4.2 权限校验
Broker端对权限的校验逻辑主要分为以下几步：
（1）检查是否命中全局 IP 白名单；如果是，则认为校验通过；否则走 2；
（2）检查是否命中用户 IP 白名单；如果是，则认为校验通过；否则走 3；
（3）校验签名，校验不通过，抛出异常；校验通过，则走 4；
（4）对用户请求所需的权限 和 用户所拥有的权限进行校验；不通过，抛出异常； 
用户所需权限的校验需要注意已下内容：
（1）特殊的请求例如 UPDATE_AND_CREATE_TOPIC 等，只能由 admin 账户进行操作；
（2）对于某个资源，如果有显性配置权限，则采用配置的权限；如果没有显性配置权限，则采用默认的权限；

## 5. 热加载修改后权限控制定义
RocketMQ的权限控制存储的默认实现是基于yml配置文件。用户可以动态修改权限控制定义的属性，而不需重新启动Broker服务节点。

## 6. 权限控制的使用限制
(1)如果ACL与高可用部署(Master/Slave架构)同时启用，那么需要在Broker Master节点的distribution/conf/plain_acl.yml配置文件中
设置全局白名单信息，即为将Slave节点的ip地址设置至Master节点plain_acl.yml配置文件的全局白名单中。

(2)如果ACL与高可用部署(多副本Dledger架构)同时启用，由于出现节点宕机时，Dledger Group组内会自动选主，那么就需要将Dledger Group组
内所有Broker节点的plain_acl.yml配置文件的白名单设置所有Broker节点的ip地址。

## 7. ACL mqadmin配置管理命令

### 7.1 更新ACL配置文件中“account”的属性值

该命令的示例如下：

sh mqadmin updateAclConfig -n 192.168.1.2:9876 -b 192.168.12.134:10911 -a RocketMQ -s 1234567809123 
-t topicA=DENY,topicD=SUB -g groupD=DENY,groupB=SUB

说明：如果不存在则会在ACL Config YAML配置文件中创建；若存在，则会更新对应的“accounts”的属性值;
如果指定的是集群名称，则会在集群中各个broker节点执行该命令；否则会在单个broker节点执行该命令。

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| a | eg:RocketMQ | Access Key值(必填) |
| s | eg:1234567809123 | Secret Key值(可选) |
| m | eg:true | 是否管理员账户(可选) |
| w | eg:192.168.0.* | whiteRemoteAddress,用户IP白名单(可选) |
| i | eg:DENY;PUB;SUB;PUB\|SUB | defaultTopicPerm,默认Topic权限(可选) |
| u | eg:DENY;PUB;SUB;PUB\|SUB | defaultGroupPerm,默认ConsumerGroup权限(可选) |
| t | eg:topicA=DENY,topicD=SUB | topicPerms,各个Topic的权限(可选) |
| g | eg:groupD=DENY,groupB=SUB | groupPerms,各个ConsumerGroup的权限(可选) |

### 7.2 删除ACL配置文件里面的对应“account”
该命令的示例如下：

sh mqadmin deleteAccessConfig -n 192.168.1.2:9876 -c DefaultCluster -a RocketMQ

说明：如果指定的是集群名称，则会在集群中各个broker节点执行该命令；否则会在单个broker节点执行该命令。
其中，参数"a"为Access Key的值，用以标识唯一账户id，因此该命令的参数中指定账户id即可。

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| a | eg:RocketMQ | Access Key的值(必填) |


### 7.3 更新ACL配置文件里面中的全局白名单
该命令的示例如下：

sh mqadmin updateGlobalWhiteAddr -n 192.168.1.2:9876 -b 192.168.12.134:10911 -g 10.10.154.1,10.10.154.2

说明：如果指定的是集群名称，则会在集群中各个broker节点执行该命令；否则会在单个broker节点执行该命令。
其中，参数"g"为全局IP白名的值，用以更新ACL配置文件中的“globalWhiteRemoteAddresses”字段的属性值。

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |
| g | eg:10.10.154.1,10.10.154.2 | 全局IP白名单(必填) |

### 7.4 查询集群/Broker的ACL配置文件版本信息
该命令的示例如下：

sh mqadmin clusterAclConfigVersion -n 192.168.1.2:9876 -c DefaultCluster

说明：如果指定的是集群名称，则会在集群中各个broker节点执行该命令；否则会在单个broker节点执行该命令。

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |

### 7.5 查询集群/Broker的ACL配置文件全部内容
该命令的示例如下：

sh mqadmin getAccessConfigSubCommand -n 192.168.1.2:9876 -c DefaultCluster

说明：如果指定的是集群名称，则会在集群中各个broker节点执行该命令；否则会在单个broker节点执行该命令。

| 参数 | 取值 | 含义 |
| --- | --- | --- |
| n | eg:192.168.1.2:9876 | namesrv地址(必填) |
| c | eg:DefaultCluster | 指定集群名称(与broker地址二选一) |
| b | eg:192.168.12.134:10911 | 指定broker地址(与集群名称二选一) |

**特别注意**开启Acl鉴权认证后导致Master/Slave和Dledger模式下Broker同步数据异常的问题，
在社区[4.5.1]版本中已经修复，具体的PR链接为：https://github.com/apache/rocketmq/pull/1149；