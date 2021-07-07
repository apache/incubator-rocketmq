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
package org.apache.rocketmq.tools.command.offset;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.*;

public class ResetOffsetByOffsetCommandTest {
    private static DefaultMQAdminExt defaultMQAdminExt;
    private static ResetOffsetByOffsetCommand cmd = new ResetOffsetByOffsetCommand();

    @BeforeClass
    public static void init() throws NoSuchFieldException, IllegalAccessException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        defaultMQAdminExt = mock(DefaultMQAdminExt.class);
        Field field = ResetOffsetByOffsetCommand.class.getField("defaultMQAdminExt");
        field.setAccessible(true);
        field.set(cmd, defaultMQAdminExt);
    }

    @AfterClass
    public static void terminate() {
        defaultMQAdminExt.shutdown();
    }

    @Test
    public void testExecute() throws Exception {
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String brokerName = "default-broker";
        String group = "default-group";
        String topic = "unit-test";
        int queueId = 0;
        long offset = 100;
        String[] subargs = new String[]{"-b " + brokerName, "-g " + group, "-t " + topic, "-i " + queueId, "-o " + offset};
        final CommandLine commandLine =
                ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
        cmd.execute(commandLine, options, null);
        verify(defaultMQAdminExt, times(1)).resetOffsetByOffset(topic, group, brokerName, queueId, offset, false);
    }
}
