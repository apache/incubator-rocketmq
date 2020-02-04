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
package org.apache.rocketmq.broker.dledger;

import io.openmessaging.storage.dledger.DLedgerLeaderElector;
import io.openmessaging.storage.dledger.DLedgerServer;
import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.utils.DLedgerUtils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;

public class DLedgerRoleChangeHandler implements DLedgerLeaderElector.RoleChangeHandler {

    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryImpl("DLegerRoleChangeHandler_"));
    private BrokerController brokerController;
    private DefaultMessageStore messageStore;
    private DLedgerCommitLog dLedgerCommitLog;
    private DLedgerServer dLegerServer;
    public DLedgerRoleChangeHandler(BrokerController brokerController, DefaultMessageStore messageStore) {
        this.brokerController = brokerController;
        this.messageStore = messageStore;
        this.dLedgerCommitLog = (DLedgerCommitLog) messageStore.getCommitLog();
        this.dLegerServer = dLedgerCommitLog.getdLedgerServer();
    }

    @Override public void handle(long term, MemberState.Role role) {
        Runnable runnable = new Runnable() {
            @Override public void run() {
                long start = System.currentTimeMillis();
                try {
                    boolean succ = true;
                    log.info("Begin handling broker role change term={} role={} currStoreRole={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole());
                    switch (role) {
                        case CANDIDATE:
                            if (messageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                                brokerController.changeToSlave(dLedgerCommitLog.getId());
                            }
                            break;
                        case FOLLOWER:
                            brokerController.changeToSlave(dLedgerCommitLog.getId());
                            break;
                        case LEADER:
                            while (true) {
                                if (!dLegerServer.getMemberState().isLeader()) {
                                    succ = false;
                                    break;
                                }
                                if (dLegerServer.getdLedgerStore().getLedgerEndIndex() == -1) {
                                    break;
                                }
                                if (dLegerServer.getdLedgerStore().getLedgerEndIndex() == dLegerServer.getdLedgerStore().getCommittedIndex()
                                    && messageStore.dispatchBehindBytes() == 0) {
                                    break;
                                }
                                Thread.sleep(100);
                            }
                            if (succ) {
                                messageStore.recoverTopicQueueTable();
                                brokerController.changeToMaster(BrokerRole.SYNC_MASTER);
                            }
                            break;
                        default:
                            break;
                    }
                    log.info("Finish handling broker role change succ={} term={} role={} currStoreRole={} cost={}", succ, term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start));
                } catch (Throwable t) {
                    log.info("[MONITOR]Failed handling broker role change term={} role={} currStoreRole={} cost={}", term, role, messageStore.getMessageStoreConfig().getBrokerRole(), DLedgerUtils.elapsed(start), t);
                }
            }
        };
        executorService.submit(runnable);
    }

    @Override public void startup() {

    }

    @Override public void shutdown() {
        executorService.shutdown();
    }
}
