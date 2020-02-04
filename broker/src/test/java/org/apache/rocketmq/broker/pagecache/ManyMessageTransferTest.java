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

package org.apache.rocketmq.broker.pagecache;

import java.nio.ByteBuffer;
import org.apache.rocketmq.store.GetMessageResult;
import org.junit.Assert;
import org.junit.Test;

public class ManyMessageTransferTest {

    @Test
    public void ManyMessageTransferBuilderTest(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putInt(20);
        GetMessageResult getMessageResult = new GetMessageResult();
        ManyMessageTransfer manyMessageTransfer = new ManyMessageTransfer(byteBuffer,getMessageResult);
    }

    @Test
    public void ManyMessageTransferPosTest(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putInt(20);
        GetMessageResult getMessageResult = new GetMessageResult();
        ManyMessageTransfer manyMessageTransfer = new ManyMessageTransfer(byteBuffer,getMessageResult);
        Assert.assertEquals(manyMessageTransfer.position(),4);
    }

    @Test
    public void ManyMessageTransferCountTest(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putInt(20);
        GetMessageResult getMessageResult = new GetMessageResult();
        ManyMessageTransfer manyMessageTransfer = new ManyMessageTransfer(byteBuffer,getMessageResult);

        Assert.assertEquals(manyMessageTransfer.count(),20);

    }

    @Test
    public void ManyMessageTransferCloseTest(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putInt(20);
        GetMessageResult getMessageResult = new GetMessageResult();
        ManyMessageTransfer manyMessageTransfer = new ManyMessageTransfer(byteBuffer,getMessageResult);
        manyMessageTransfer.close();
        manyMessageTransfer.deallocate();
    }
}
