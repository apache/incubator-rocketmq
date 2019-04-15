#!/bin/bash
SVM_OPT="--allow-incomplete-classpath --report-unsupported-elements-at-runtime"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.util.BouncyCastleSelfSignedCertGenerator"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.JdkAlpnApplicationProtocolNegotiator"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.JdkNpnApplicationProtocolNegotiator"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.JdkNpnSslEngine"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.JdkAlpnSslEngine"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.util.internal.JavassistTypeParameterMatcherGenerator"
SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=com.alibaba.fastjson.serializer.JodaCodec"
# testing
#SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.util.SelfSignedCertificate"
#SVM_OPT="${SVM_OPT} --delay-class-initialization-to-runtime=io.netty.handler.ssl.util.ThreadLocalInsecureRandom"
SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=io.netty.handler.ssl.util.SelfSignedCertificate"
SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=io.netty.handler.ssl.util.ThreadLocalInsecureRandom"
SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=com.alibaba.fastjson.serializer.SerializeConfig"
SVM_OPT="${SVM_OPT} --rerun-class-initialization-at-runtime=com.alibaba.fastjson.parser.ParserConfig"

SVM_OPT="${SVM_OPT} --enable-url-protocols=http"

WORKDIR=`pwd`
CONFIG_OPT="-H:ConfigurationFileDirectories=${WORKDIR}/config -Dio.netty.noUnsafe=true -H:+ReportExceptionStackTraces --allow-incomplete-classpath"
native_image=/home/cengfeng.lzy/GraalDev/graal/vm/mxbuild/linux-amd64/GRAALVM_LIBGRAAL/graalvm-libgraal-1.0.0-rc16-dev/bin/native-image
#native_image=~/tools/graalvm-ce-1.0.0-rc14/bin/native-image
$native_image  $CONFIG_OPT $SVM_OPT  -jar target/rocketmq-client-4.5.1-SNAPSHOT-jar-with-dependencies.jar
