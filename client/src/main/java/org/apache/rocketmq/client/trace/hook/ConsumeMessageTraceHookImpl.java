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
package org.apache.rocketmq.client.trace.hook;

import org.apache.rocketmq.client.consumer.listener.ConsumeReturnType;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.apache.rocketmq.client.trace.TraceBean;
import org.apache.rocketmq.client.trace.TraceType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

public class ConsumeMessageTraceHookImpl implements ConsumeMessageHook {

    private TraceDispatcher localDispatcher;

    public ConsumeMessageTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "ConsumeMessageTraceHook";
    }

    @Override
    public void consumeMessageBefore(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        TraceContext traceContext = new TraceContext();
        context.setMqTraceContext(traceContext);
        traceContext.setTraceType(TraceType.SubBefore);//
        traceContext.setGroupName(NamespaceUtil.withoutNamespace(context.getConsumerGroup()));//
        List<TraceBean> beans = new ArrayList<TraceBean>();
        for (MessageExt msg : context.getMsgList()) {
            if (msg == null) {
                continue;
            }
            String regionId = msg.getProperty(MessageConst.PROPERTY_MSG_REGION);
            String traceOn = msg.getProperty(MessageConst.PROPERTY_TRACE_SWITCH);

            if (traceOn != null && traceOn.equals("false")) {
                // If trace switch is false ,skip it
                continue;
            }
            TraceBean traceBean = new TraceBean();
            traceBean.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic()));//
            traceBean.setMsgId(msg.getMsgId());//
            traceBean.setTags(msg.getTags());//
            traceBean.setKeys(msg.getKeys());//
            traceBean.setStoreTime(msg.getStoreTimestamp());//
            traceBean.setBodyLength(msg.getStoreSize());//
            traceBean.setRetryTimes(msg.getReconsumeTimes());//
            traceContext.setRegionId(regionId);//
            beans.add(traceBean);
        }
        if (beans.size() > 0) {
            traceContext.setTraceBeans(beans);
            traceContext.setTimeStamp(System.currentTimeMillis());
            localDispatcher.append(traceContext);
        }
    }

    @Override
    public void consumeMessageAfter(ConsumeMessageContext context) {
        if (context == null || context.getMsgList() == null || context.getMsgList().isEmpty()) {
            return;
        }
        TraceContext subBeforeContext = (TraceContext) context.getMqTraceContext();

        if (subBeforeContext.getTraceBeans() == null || subBeforeContext.getTraceBeans().size() < 1) {
            // If subbefore bean is null ,skip it
            return;
        }
        TraceContext subAfterContext = new TraceContext();
        subAfterContext.setTraceType(TraceType.SubAfter);//
        subAfterContext.setRegionId(subBeforeContext.getRegionId());//
        subAfterContext.setGroupName(NamespaceUtil.withoutNamespace(subBeforeContext.getGroupName()));//
        subAfterContext.setRequestId(subBeforeContext.getRequestId());//
        subAfterContext.setSuccess(context.isSuccess());//

        // Caculate the cost time for processing messages
        int costTime = (int) ((System.currentTimeMillis() - subBeforeContext.getTimeStamp()) / context.getMsgList().size());
        subAfterContext.setCostTime(costTime);//
        subAfterContext.setTraceBeans(subBeforeContext.getTraceBeans());
        String contextType = context.getProps().get(MixAll.CONSUME_CONTEXT_TYPE);
        if (contextType != null) {
            subAfterContext.setContextCode(ConsumeReturnType.valueOf(contextType).ordinal());
        }
        localDispatcher.append(subAfterContext);
    }
}
