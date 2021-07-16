##NamesrvStarup
---
###类简介
‘public class NamesrvStartup’

>`NamesrvStartup`类是namesrv启动的入口类，在本地配置好环境变量ROCKETMQ_HOME后可通过执行其中的main方法来快速启动一个namesrv。namesrv主要负责维护活跃broker的信息和topic以及topic所在队列的信息。其中启动参数主要由namesrvConfig和nettyServerConfig这两个类来初始化，当然这些配置也可以通过启动指定配置文件进行修改。

``` java
    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {

        try {
            //NamesrvController是namesrv接受请求并响应的核心类,
            NamesrvController controller = createNamesrvController(args);
            //启动并初始化NamesrvController
            start(controller);
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf("%s%n", tip);
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    } 
```

###namesrv启动过程一：创建NamesrvController
createNamesrvController中会创建并初始化namesrvConfig和nettyServerConfig的实例

#### namesrvConfig参数说明
|类型|参数名称|描述|
|------|-------|-------|
|String|rocketmqHome|rocketmq的主目录|
|String|kvConfigPath|kv配置属性的持久化路径|
|String|configStorePath|namesrv的配置文件路径，可以使用-c指定|
|String|productEnvName|环境名|
|boolean|clusterTest|是否开启集群测试，默认false|
|boolean|orderMessageEnable|是否支持顺序消息，默认是false|

#### 参数详细信息
- rocketmqHome<br>
`private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));`

     该属性首先会从java属性rocketmq.home.dir中获取，如果获取不到则从ROCKETMQ_HOME中去获取，为绝对路径，例如：ROCKETMQ_HOME=F:\work\rocketmq

- kvConfigPath<br>
`private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";`

     kv配置文件路径，包含顺序消息topic的配置信息，默认为当前系统用户的主目录路径，建议自己指定

- configStorePath<br>
`private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";`

     NameServer配置文件路径，可以使用-c指定NameServer配置文件路径，文件内容可以为：配置项=值

- productEnvName<br>
`private String productEnvName = "center";`

     环境名

- clusterTest<br>
`private boolean clusterTest = false;`

     是否开启集群测试的配置项，为true时会则会注册 ClusterTestRequestProcessor 处理器。继承于DefaultRequestProcessor主要是重新了getRouteInfoByTopic()方法，当在一个namesrv上获取不到topic的路由信息，则会去其他namesrv上获取

- orderMessageEnable<br>
`private boolean orderMessageEnable = false;`

     是否支持顺序消息的配置项，默认是不开启的，在启用顺序消息时会开启

#### nettyServerConfig参数说明
|类型|参数名称|描述|
|------|-------|-------|
|int|listenPort|nameServer监听的端口，是8888，后面会被初始化为9876|
|int|serverWorkerThreads|netty的worker线程池线程个数|
|int|serverCallbackExecutorThreads|netty的公共任务（发送，消费，心跳检测）的线程池线程个数|
|int|serverSelectorThreads|io任务的线程池线程个数|
|int|serverOnewaySemaphoreValue|发送oneway消息时的请求并发度|
|int|serverAsyncSemaphoreValue|异步消息最大并发度|
|int|serverChannelMaxIdleTimeSeconds|网络最大空闲时间|
|int|serverSocketSndBufSize|socket发送端缓冲区大小|
|int|serverSocketRcvBufSize|socket接收端缓存区大小|
|boolean|serverPooledByteBufAllocatorEnable|是否启用bytebuffer缓存（提高读写速度用的）,默认true|
|boolean|useEpollNativeSelector|是否启用Epoll，默认false|

- listenPort<br>
`private int listenPort = 8888;`

    nameServer监听的端口，是8888，后面会被初始化为9876
- serverWorkerThreads<br>
`private int serverWorkerThreads = 8;`
    
    netty的worker线程池个数，处理namesrv的业务逻辑线程
    
- serverCallbackExecutorThreads<br>
`private int serverCallbackExecutorThreads = 0;`
    
    netty公共任务的线程池个数，netty网络设计会根据业务类型的不同来创建不同线程池来处理发送消息，消息消费和心跳检测等消息
    
- serverSelectorThreads<br>
`private int serverSelectorThreads = 3;`
    
    IO线程池线程个数，具体主要是NameServer.broker端解析请求，返回相应的线程个数，这类线程主要是处理网络请求的，解析请求包。然后转发到各个业务线程池完成具体的业务无操作，然后将结果在返回调用方
    
- serverOnewaySemaphoreValue<br>
`private int serverOnewaySemaphoreValue = 256;`
    
    发送oneway消息请求并发度，oneway消息就是只发送消息，不等待服务器响应，只发送请求不等待应答。此方式发送消息的过程耗时非常短，一般在微秒级别，适用于对可靠性要求并不高的场景
    
- serverAsyncSemaphoreValue<br>
`private int serverAsyncSemaphoreValue = 64;`
    
    异步消息发送最大并发度，异步消息指的是当master收到消息直接返回成功，slave同步的工作异步的进行，适用于对消息可靠性要求不高，对时延有要求的场景
    
- serverChannelMaxIdleTimeSeconds<br>
`private int serverChannelMaxIdleTimeSeconds = 120;`
    
    网络连接最大空闲时间，单位为秒。如果链接空闲时间超过此参数设置的值，连接将被关闭，这里的链接指的应该是producer，broker,consumer到namesrv的长链接
    
- serverSocketSndBufSize<br>
`private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;`
    
    netty网络socket发送缓存区大小131072比特，为16MB
    
- serverSocketRcvBufSize<br>
`private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;`
    
    netty网络socket接收缓存区大小131072比特，为16MB
    
- serverPooledByteBufAllocatorEnable<br>
`private boolean serverPooledByteBufAllocatorEnable = true;`

    ByteBuffer是否开启缓存,作用是提高读写速度
    
- useEpollNativeSelector<br>
`private boolean useEpollNativeSelector = false;`

    是否启用Epoll IO模型。Linux环境建议开启，默认为false

namesrvConfig和nettyServerConfig里的这些配置都可以通过在命令行中使用"-c"来指定属性配置文件的位置来重新修改;也可以通过"-p 属性名=属性值"这样来重新指定

最后调用NamesrvController的构造方法创建NamesrvController实例，NamesrvController创建完成

###namesrv启动过程二：初始化NamesrvController

1.调用start方法启动NamesrvController

2.调用NamesrvController的initialize来初始化

3.初始化过程如下：
- 加载kv配置，存在一个kv配置管理器
`this.kvConfigManager.load();`

    kvConfigManager用到了一个HashMap，map的key是namespace，value是一个HashMap， 通过ReentrantReadWriteLock类对kv的读写各种操作进行并发控制
    
- 创建netty服务的对象，同时在onChannelDestroy方法规定了当broker宕机时要触发的动作，其实就是把registerBroker方法做的事回退掉

    

- 创建netty处理响应的线程池，然后注册
- 创建了一个定时线程池，调用scanNotActiveBroker方法每隔10s来检查哪些broker失去心跳了，如broker每30s上报一次，若连着45s没收到心跳的则剔除该broker
- 创建一个定时线程池，调用printAllPeriodically方法每隔10秒来刷新kv配置
- FileWatchService的作用应该是监听配置文件的改变，有变化有重新加载下
###namesrv启动过程三：释放资源
在执行controller.start()先去注册了一个hook方法，使得在jvm退出（就是namesrv关闭）前先去shutdown这个controller（释放掉之前启的各种线程池）

