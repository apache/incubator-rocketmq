## Apache RocketMQ 
[![Build Status](https://travis-ci.org/apache/rocketmq.svg?branch=master)](https://travis-ci.org/apache/rocketmq) [![Coverage Status](https://coveralls.io/repos/github/apache/rocketmq/badge.svg?branch=master)](https://coveralls.io/github/apache/rocketmq?branch=master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-all/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Corg.apache.rocketmq)
[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://rocketmq.apache.org/dowloading/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq.svg)](http://isitmaintained.com/project/apache/rocketmq "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq.svg)](http://isitmaintained.com/project/apache/rocketmq "Percentage of issues still open")
![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)

**[Apache RocketMQ](https://rocketmq.apache.org) is a distributed messaging and streaming platform with low latency, high performance and reliability, trillion-level capacity and flexible scalability.**

It offers a variety of features:

* Messageing patterns including publish/subscribe, request/reply and streaming
* Financial grade transactional message
* Built-in fault tolerance and high availability configuration options base on [DLedger](https://github.com/openmessaging/openmessaging-storage-dledger)
* A variety of cross language clients, such as Java, C/C++, Python, Go
* Pluggable transport protocols, such as TCP, SSL, AIO
* Built-in message tracing capability, also support opentracing
* Versatile big-data and streaming ecosytem integration
* Message retroactivity by time or offset
* Reliable FIFO and strict ordered messaging in the same queue
* Efficient pull and push consumption model
* Million-level message accumulation capacity in a single queue
* Multiple messaging protocols like JMS and OpenMessaging
* Flexible distributed scale-out deployment architecture
* Lightning-fast batch message exchange system
* Various message filter mechanics such as SQL and Tag
* Docker images for isolated testing and cloud isolated clusters
* Feature-rich administrative dashboard for configuration, metrics and monitoring
* Authentication and authorization
* Free open source connectors, for both sources and sinks

----------

## Learn it & Contact us
* Mailing Lists: <https://rocketmq.apache.org/about/contact/>
* Home: <https://rocketmq.apache.org>
* Docs: <https://rocketmq.apache.org/docs/quick-start/>
* Issues: <https://github.com/apache/rocketmq/issues>
* Rips: <https://github.com/apache/rocketmq/wiki/RocketMQ-Improvement-Proposal>
* Ask: <https://stackoverflow.com/questions/tagged/rocketmq>
* Slack: <https://rocketmq-invite-automation.herokuapp.com/>
 

----------

## Apache RocketMQ Community
* [RocketMQ Community Projects](https://github.com/apache/rocketmq-externals)
----------

## Contributing
We always welcome new contributions, whether for trivial cleanups, [big new features](https://github.com/apache/rocketmq/wiki/RocketMQ-Improvement-Proposal) or other material rewards, more details see [here](http://rocketmq.apache.org/docs/how-to-contribute/).
 
----------
## License
[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation


----------
## Export Control Notice
This distribution includes cryptographic software. The country in which you currently reside may have
restrictions on the import, possession, use, and/or re-export to another country, of encryption software.
BEFORE using any encryption software, please check your country's laws, regulations and policies concerning
the import, possession, or use, and re-export of encryption software, to see if this is permitted. See
<http://www.wassenaar.org/> for more information.

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this
software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software
using or performing cryptographic functions with asymmetric algorithms. The form and manner of this Apache
Software Foundation distribution makes it eligible for export under the License Exception ENC Technology
Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for
both object code and source code.

The following provides more details on the included cryptographic software:

This software uses Apache Commons Crypto (https://commons.apache.org/proper/commons-crypto/) to
support authentication, and encryption and decryption of data sent across the network between
services.
