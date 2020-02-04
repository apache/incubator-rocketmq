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
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.junit.Assert;
import org.junit.Test;

public class OneMessageTransferTest {

    @Test
    public void OneMessageTransferTest(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putInt(20);
        SelectMappedBufferResult selectMappedBufferResult = new SelectMappedBufferResult(0,byteBuffer,20,new MappedFile());
        OneMessageTransfer manyMessageTransfer = new OneMessageTransfer(byteBuffer,selectMappedBufferResult);
    }

    @Test
    public void OneMessageTransferCountTest(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putInt(20);
        SelectMappedBufferResult selectMappedBufferResult = new SelectMappedBufferResult(0,byteBuffer,20,new MappedFile());
        OneMessageTransfer manyMessageTransfer = new OneMessageTransfer(byteBuffer,selectMappedBufferResult);
        Assert.assertEquals(manyMessageTransfer.count(),40);
    }

    @Test
    public void OneMessageTransferPosTest(){
        ByteBuffer byteBuffer = ByteBuffer.allocate(20);
        byteBuffer.putInt(20);
        SelectMappedBufferResult selectMappedBufferResult = new SelectMappedBufferResult(0,byteBuffer,20,new MappedFile());
        OneMessageTransfer manyMessageTransfer = new OneMessageTransfer(byteBuffer,selectMappedBufferResult);
        Assert.assertEquals(manyMessageTransfer.position(),8);
    }
}
