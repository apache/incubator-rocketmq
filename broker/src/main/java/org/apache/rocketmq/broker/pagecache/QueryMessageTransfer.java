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

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import org.apache.rocketmq.store.QueryMessageResult;

public class QueryMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final QueryMessageResult queryMessageResult;

    /**
     * Bytes which were transferred already.
     */
    private long transferred;

    public QueryMessageTransfer(ByteBuffer byteBufferHeader, QueryMessageResult queryMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.queryMessageResult = queryMessageResult;
    }

    @Override
    public long position() {
        int pos = byteBufferHeader.position();
        List<ByteBuffer> messageBufferList = this.queryMessageResult.getMessageBufferList();
        for (ByteBuffer bb : messageBufferList) {
            pos += bb.position();
        }
        return pos;
    }

    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long transferred() {
        return transferred;
    }

    @Override
    public long count() {
        return byteBufferHeader.limit() + this.queryMessageResult.getBufferTotalSize();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += target.write(this.byteBufferHeader);
            return transferred;
        } else {
            List<ByteBuffer> messageBufferList = this.queryMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                if (bb.hasRemaining()) {
                    transferred += target.write(bb);
                    return transferred;
                }
            }
        }

        return 0;
    }

    @Override
    public FileRegion retain() {
        super.retain();
        return this;
    }

    @Override
    public FileRegion retain(int increment) {
        super.retain(increment);
        return this;
    }

    @Override
    public FileRegion touch() {
        return this;
    }

    @Override
    public FileRegion touch(Object hint) {
        return this;
    }

    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
        this.queryMessageResult.release();
    }
}
