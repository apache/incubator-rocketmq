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
import org.apache.rocketmq.store.GetMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class ManyMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final GetMessageResult getMessageResult;

    /**
     * Bytes which were transferred already.
     */
    private long transferred;

    public ManyMessageTransfer(ByteBuffer byteBufferHeader, GetMessageResult getMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.getMessageResult = getMessageResult;
        for (SelectMappedBufferResult r: getMessageResult.getMessageMapedList()) {
            r.setStartOffset(r.getStartOffset() - r.getMappedFile().getFileFromOffset());
        }
    }

    @Override
    public long position() {
        int pos = byteBufferHeader.position();
        List<ByteBuffer> messageBufferList = this.getMessageResult.getMessageBufferList();
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
    public long count() {
        return byteBufferHeader.limit() + this.getMessageResult.getBufferTotalSize();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += target.write(this.byteBufferHeader);
            return transferred;
        } else {
            List<SelectMappedBufferResult> messageMapedList = this.getMessageResult.getMessageMapedList();
            for (SelectMappedBufferResult r: messageMapedList) {
                if (r.getSize() <= 0) {
                    continue;
                }
                long written = r.getMappedFile().getFileChannel().transferTo(r.getStartOffset(), r.getSize(), target);
                if (written > 0) {
                    r.setStartOffset(r.getStartOffset() + written);
                    r.setSize(r.getSize() - (int)written);
                    transferred += written;
                }
            }

            return transferred;
        }
    }

    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
        this.getMessageResult.release();
    }
}
