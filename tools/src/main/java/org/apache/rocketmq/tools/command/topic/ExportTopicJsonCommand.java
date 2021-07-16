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
package org.apache.rocketmq.tools.command.topic;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

import com.alibaba.fastjson.JSON;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ExportTopicJsonCommand implements SubCommand {
    @Override
    public String commandName() {
        return "exportTopicJson";
    }

    @Override
    public String commandDesc() {
        return "export topic.json";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "choose a broker to export topic.json");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("f", "filePath", true, "export topic.json path | default /tmp/rocketmq/config");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook)
        throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String brokerAddr = commandLine.getOptionValue('b').trim();
            String filePath = !commandLine.hasOption('f') ? "/tmp/rocketmq/config" : commandLine.getOptionValue('f')
                .trim();

            TopicConfigSerializeWrapper topicConfigSerializeWrapper = defaultMQAdminExt.getAllTopicConfig(
                brokerAddr, 10000);

            ConcurrentMap<String, TopicConfig> topicConfigMap = topicConfigSerializeWrapper.getTopicConfigTable();
            Iterator<Entry<String, TopicConfig>> iterator = topicConfigMap.entrySet().iterator();
            while (iterator.hasNext()) {
                if (TopicValidator.isSystemTopic(iterator.next().getKey()) || iterator.next().getKey().startsWith(
                    MixAll.RETRY_GROUP_TOPIC_PREFIX) || iterator.next().getKey().startsWith(
                    MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
                    iterator.remove();
                }
            }

            String topicJsonString = JSON.toJSONString(topicConfigSerializeWrapper, true);
            String fileName = filePath + "/topic.json";

            MixAll.string2File(topicJsonString, fileName);

            System.out.printf("export %s success", fileName);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
