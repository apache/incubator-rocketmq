/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.delaymsg.service;

import org.apache.rocketmq.broker.util.MessageConverter;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.delaymsg.ScheduleIndex;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DelayMsgCheckPoint;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.delaymsg.AbstractReplayDelayMsgService;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LocalReplayDelayMsgService extends AbstractReplayDelayMsgService {
    private static final InternalLogger LOGGER = InternalLoggerFactory.getLogger(LoggerName.DELAY_MESSAGE_LOGGER_NAME);
    private final MessageStore messageStore;
    private ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryImpl("ReplayMessageThread"));

    public LocalReplayDelayMsgService(MessageStore messageStore, DelayMsgCheckPoint delayMsgCheckPoint) {
        super(delayMsgCheckPoint);
        this.messageStore = messageStore;
    }

    @Override
    public void process(ScheduleIndex index) {
        super.process(index);
        index.getPhyOffsets().forEach(n -> executorService.submit(new ReplayTask(n)));
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {
        this.executorService.shutdown();
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    class ReplayTask implements Runnable {

        private long offset;

        public ReplayTask(long offset) {
            this.offset = offset;
        }

        @Override
        public void run() {
            MessageExt messageExt = LocalReplayDelayMsgService.this.messageStore.lookMessageByOffset(offset);
            if (messageExt != null) {
                MessageExtBrokerInner msgInner = MessageConverter.convertCustomDelayMsg2NormalMsg(messageExt);
                PutMessageResult putMessageResult = LocalReplayDelayMsgService.this.messageStore.putMessage(msgInner);
                LOGGER.debug("A delayMsg  time up, topic: {}, msgId: {}, replay Message {}", msgInner.getTopic(), msgInner.getMsgId(), putMessageResult.isOk() ? "success" : "fail");
            }
        }
    }
}
