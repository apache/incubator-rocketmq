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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.store.CommitLogDispatcher;
import org.apache.rocketmq.store.DispatchRequest;

import java.util.Collection;
import java.util.Iterator;

/**
 * Calculate bit map of filter.
 */
public class CommitLogDispatcherCalcBitMap implements CommitLogDispatcher {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    protected final BrokerConfig brokerConfig;
    protected final ConsumerFilterManager consumerFilterManager;

    public CommitLogDispatcherCalcBitMap(BrokerConfig brokerConfig, ConsumerFilterManager consumerFilterManager) {
        this.brokerConfig = brokerConfig;
        this.consumerFilterManager = consumerFilterManager;
    }

    @Override
    public void dispatch(DispatchRequest request) {
        if (!this.brokerConfig.isEnableCalcFilterBitMap()) {
            return;
        }

        try {

            Collection<ConsumerFilterData> filterDatas = consumerFilterManager.get(request.getTopic());

            if (filterDatas == null || filterDatas.isEmpty()) {
                return;
            }

            Iterator<ConsumerFilterData> iterator = filterDatas.iterator();
            BitsArray filterBitMap = BitsArray.create(
                this.consumerFilterManager.getBloomFilter().getM()
            );

            long startTime = System.currentTimeMillis();
            while (iterator.hasNext()) {
                ConsumerFilterData filterData = iterator.next();

                if (filterData.getCompiledExpression() == null) {
                    log.error("[BUG] Consumer in filter manager has no compiled expression! {}", filterData);
                    continue;
                }

                if (filterData.getBloomFilterData() == null) {
                    log.error("[BUG] Consumer in filter manager has no bloom data! {}", filterData);
                    continue;
                }

                Object ret = null;
                try {
                    MessageEvaluationContext context = new MessageEvaluationContext(request.getPropertiesMap());

                    ret = filterData.getCompiledExpression().evaluate(context);
                } catch (Throwable e) {
                    log.error("Calc filter bit map error!commitLogOffset={}, consumer={}, {}", request.getCommitLogOffset(), filterData, e);
                }

                log.debug("Result of Calc bit map:ret={}, data={}, props={}, offset={}", ret, filterData, request.getPropertiesMap(), request.getCommitLogOffset());

                // eval true
                if (ret != null && ret instanceof Boolean && (Boolean) ret) {
                    consumerFilterManager.getBloomFilter().hashTo(
                        filterData.getBloomFilterData(),
                        filterBitMap
                    );
                }
            }

            request.setBitMap(filterBitMap.bytes());

            long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(startTime);
            // 1ms
            if (elapsedTime >= 1) {
                log.warn("Spend {} ms to calc bit map, consumerNum={}, topic={}", elapsedTime, filterDatas.size(), request.getTopic());
            }
        } catch (Throwable e) {
            log.error("Calc bit map error! topic={}, offset={}, queueId={}, {}", request.getTopic(), request.getCommitLogOffset(), request.getQueueId(), e);
        }
    }
}
