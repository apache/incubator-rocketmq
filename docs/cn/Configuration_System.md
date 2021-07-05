# 系统配置

本节重点介绍系统（JVM/OS）的配置 
---

## **1 JVM 选项** ##

建议使用最新发布的 JDK 1.8 版本。设置相同的 Xms 和 Xmx 值以防止 JVM 调整堆大小，并获得更好的性能。一种通用的JVM配置如下： 

    -server -Xms8g -Xmx8g -Xmn4g

设置 Direct ByteBuffer 内存大小。当 Direct ByteBuffer 达到指定大小时，将触发 Full GC：

    -XXMaxDirectMemorySize=15g

如果你不在乎 RocketMQ broker 的启动时间，建议启用预分配 Java 堆以确保在 JVM 初始化期间为每个页面分配内存。你可以通过以下方式启用它： 
    
    -XX+AlwaysPreTouch

禁用偏向锁定可以减少 JVM 停顿： 

    -XX-UseBiasedLocking

关于垃圾收集器，推荐使用 JDK 1.8 的 G1 收集器： 

    -XX+UseG1GC -XXG1HeapRegionSize=16m 
    -XXG1ReservePercent=25
    -XXInitiatingHeapOccupancyPercent=30

这些 GC 选项看起来有点激进，但事实证明它在生产环境中具有良好的性能 

不要把-XXMaxGCPauseMillis 的值设置太小，否则JVM会使用一个小的新生代来实现这个目标，从而导致频繁发生minor GC。因此，建议使用滚动 GC 日志文件：
    
    -XX+UseGCLogFileRotation 
    -XXNumberOfGCLogFiles=5 
    -XXGCLogFileSize=30m
    
写 GC 文件会增加 broker 的延迟，因此可以考虑将 GC 日志文件重定向到内存文件系统：
    
    -Xloggcdevshmmq_gc_%p.log123

## 2 Linux 内核参数 ##

在 bin 文件夹里，有一个 os.sh 脚本，里面列出了许多的内核参数，只需稍作更改即可用于生产用途。需特别关注以下参数，如想了解更多细节，请参考文档/proc/sys/vm/*。 



- **vm.extra_free_kbytes**, 控制VM在后台回收（kswapd）开始的阈值和直接回收（通过分配进程）开始的阈值之间保留额外的空闲内存。通过使用这个参数，RocketMQ 可以避免在内存分配过程中出现高延迟。（与内核版本版本有关）



- **vm.min_free_kbytes**, 该值不应设置低于1024KB，否则系统将遭到破坏，并且在高负载环境下容易出现死锁。 





- **vm.max_map_count**, 规定进程可以拥有的最大内存映射区域数。 RocketMQ 使用 mmap 来加载 CommitLog 和 ConsumeQueue，因此建议将此参数设置为较大的值。 



- **vm.swappiness**, 定义内核交换内存页的频率。该值若较大，则会导致频繁交换，较小则会减少交换量。为了避免交换延迟，建议将此值设为 10。 



- **File descriptor limits**, RocketMQ 需要给文件（CommitLog 和 ConsumeQueue）和网络连接分配文件描述符。因此建议将该值设置为 655350。 



- **Disk scheduler**, 推荐使用deadline IO 调度器，它可以为请求提供有保证的延迟。 