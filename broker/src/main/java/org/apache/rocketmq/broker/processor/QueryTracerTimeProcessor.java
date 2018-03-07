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

package org.apache.rocketmq.broker.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.ServerTracerTimeUtil;
import org.apache.rocketmq.common.TracerTime;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryTracerTimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryTracerTimeResponseHeader;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public class QueryTracerTimeProcessor implements NettyRequestProcessor {

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception {
        if (RequestCode.QUERY_TRACER_TIME == request.getCode()) {
            final RemotingCommand response = RemotingCommand.createResponseCommand(QueryTracerTimeResponseHeader.class);
            final QueryTracerTimeRequestHeader requestHeader =
                (QueryTracerTimeRequestHeader) request.decodeCommandCustomHeader(QueryTracerTimeRequestHeader.class);

            String messageTracerTimeId = requestHeader.getMessageTracerTimeId();
            TracerTime tracerTime = ServerTracerTimeUtil.tracerTimeCache.getIfPresent(messageTracerTimeId);

            if (tracerTime != null) {
                response.setBody(tracerTime.encode());
            }
            response.setCode(ResponseCode.SUCCESS);
            return response;

        } else {
            throw new RuntimeException(String.format("request code must be %s", RequestCode.QUERY_TRACER_TIME));
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
