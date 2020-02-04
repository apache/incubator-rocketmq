#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

## Revise the base dir
CURRENT_DIR="$(cd "$(dirname "$0")"; pwd)"
RMQ_DIR=$CURRENT_DIR/../..
cd $RMQ_DIR

function startNameserver() {
    export JAVA_OPT_EXT=" -Xms512m -Xmx512m  "
    nohup bin/mqnamesrv &
}

function startBroker() {
    export JAVA_OPT_EXT=" -Xms1g -Xmx1g  "
    conf_name=$1
    nohup bin/mqbroker -c $conf_name &
}

function stopNameserver() {
    PIDS=$(ps -ef|grep java|grep NamesrvStartup|grep -v grep|awk '{print $2}')
    if [ ! -z "$PIDS" ]; then
        kill -s TERM $PIDS
    fi
}

function stopBroker() {
    conf_name=$1
    PIDS=$(ps -ef|grep java|grep BrokerStartup|grep $conf_name|grep -v grep|awk '{print $2}')
    i=1
    while [ ! -z "$PIDS" -a $i -lt 5 ]
    do
        echo "Waiting to kill ..."
        kill -s TERM $PIDS
        ((i=$i+1))
        sleep 2
        PIDS=$(ps -ef|grep java|grep BrokerStartup|grep $conf_name|grep -v grep|awk '{print $2}')
    done
    PIDS=$(ps -ef|grep java|grep BrokerStartup|grep $conf_name|grep -v grep|awk '{print $2}')
    if [ ! -z "$PIDS" ]; then
        kill -9 $PIDS
    fi
}

function stopAll() {
    ps -ef|grep java|grep BrokerStartup|grep -v grep|awk '{print $2}'|xargs kill
    stopNameserver
    stopBroker ./conf/dledger/broker-n0.conf
    stopBroker ./conf/dledger/broker-n1.conf
    stopBroker ./conf/dledger/broker-n2.conf
}

function startAll() {
    startNameserver
    startBroker ./conf/dledger/broker-n0.conf
    startBroker ./conf/dledger/broker-n1.conf
    startBroker ./conf/dledger/broker-n2.conf
}

function checkConf() {
    if [ ! -f ./conf/dledger/broker-n0.conf -o ! -f ./conf/dledger/broker-n1.conf -o ! -f ./conf/dledger/broker-n2.conf ]; then
        echo "Make sure the ./conf/dledger/broker-n0.conf, ./conf/dledger/broker-n1.conf, ./conf/dledger/broker-n2.conf exists"
        exit -1
    fi 
}



## Main
if [ $# -lt 1 ]; then
    echo "Usage: sh $0 start|stop"
    exit -1
fi
action=$1
checkConf
case $action in
    "start")
        startAll
        exit
        ;;
    "stop")
        stopAll
        ;;
    *)
        echo "Usage: sh $0 start|stop"
        ;;
esac

