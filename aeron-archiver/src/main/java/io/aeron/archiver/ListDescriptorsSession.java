/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.aeron.archiver;

import io.aeron.Publication;
import io.aeron.archiver.messages.*;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.*;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

class ListDescriptorsSession implements ArchiverConductor.Session
{

    static final long EMPTY_LIST_HEADER;
    static final long DESCRIPTOR_HEADER;

    static
    {
        final MessageHeaderEncoder encoder = new MessageHeaderEncoder();
        encoder.wrap(new UnsafeBuffer(new byte[8]), 0);
        encoder.schemaId(ListStreamInstancesEmptyResponseDecoder.SCHEMA_ID);
        encoder.version(ListStreamInstancesEmptyResponseDecoder.SCHEMA_VERSION);
        encoder.blockLength(ListStreamInstancesEmptyResponseDecoder.BLOCK_LENGTH);
        encoder.templateId(ListStreamInstancesEmptyResponseDecoder.TEMPLATE_ID);
        EMPTY_LIST_HEADER = encoder.buffer().getLong(0);
        encoder.schemaId(ArchiveDescriptorDecoder.SCHEMA_ID);
        encoder.version(ArchiveDescriptorDecoder.SCHEMA_VERSION);
        encoder.blockLength(ArchiveDescriptorDecoder.BLOCK_LENGTH);
        encoder.templateId(ArchiveDescriptorDecoder.TEMPLATE_ID);
        DESCRIPTOR_HEADER = encoder.buffer().getLong(0);
    }

    private BufferClaim bufferClaim = new BufferClaim();

    private enum State
    {
        INIT,
        SENDING,
        CLOSE,
        DONE
    }

    private final ByteBuffer byteBuffer =
        BufferUtil.allocateDirectAligned(ArchiveIndex.INDEX_RECORD_SIZE, CACHE_LINE_LENGTH);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);

    private final Publication reply;
    private final int from;
    private final int to;
    private int cursor;
    private final ArchiveIndex archiveIndex;
    private State state = State.INIT;
    private final Int2ObjectHashMap<ImageArchivingSession> instance2ArchivingSession;

    ListDescriptorsSession(
        final Publication reply,
        final int from,
        final int to,
        final ArchiveIndex archiveIndex, final Int2ObjectHashMap<ImageArchivingSession> instance2ArchivingSession)
    {
        this.reply = reply;
        cursor = from;
        this.from = from;
        this.to = to;
        this.archiveIndex = archiveIndex;
        this.instance2ArchivingSession = instance2ArchivingSession;
    }

    public void abort()
    {
        state = State.CLOSE;
    }

    public boolean isDone()
    {
        return state == State.DONE;
    }

    public int doWork()
    {
        int workDone = 0;
        if (state == State.INIT)
        {
            workDone += init();
        }
        if (state == State.SENDING)
        {
            workDone += sendDescriptors();
        }
        if (state == State.CLOSE)
        {
            workDone += close();
        }
        return workDone;
    }

    private int close()
    {
        if (reply.isConnected() && from == cursor)
        {
            if (reply.tryClaim(8, bufferClaim) > 0L)
            {
                final MutableDirectBuffer buffer = bufferClaim.buffer();
                final int offset = bufferClaim.offset();
                buffer.putLong(offset, EMPTY_LIST_HEADER);
                bufferClaim.commit();
            }
            // FUCK IT let's go bowling?
        }
        CloseHelper.quietClose(reply);
        state = State.DONE;
        return 1;
    }

    private int sendDescriptors()
    {
        final int limit = Math.min(cursor + 4, to);
        for (; cursor < limit; cursor++)
        {
            final ImageArchivingSession session = instance2ArchivingSession.get(cursor);
            if (session == null)
            {
                byteBuffer.clear();
                unsafeBuffer.wrap(byteBuffer);
                try
                {
                    if (!archiveIndex.readArchiveDescriptor(cursor, byteBuffer))
                    {
                        state = State.CLOSE;
                        return 0;
                    }
                }
                catch (IOException e)
                {
                    state = State.CLOSE;
                    LangUtil.rethrowUnchecked(e);
                }

            }
            else
            {
                unsafeBuffer.wrap(session.metaDataBuffer());
            }
            final int length = unsafeBuffer.getInt(0);
            unsafeBuffer.putLong(ArchiveIndex.INDEX_FRAME_LENGTH - 8, DESCRIPTOR_HEADER);
            reply.offer(unsafeBuffer, ArchiveIndex.INDEX_FRAME_LENGTH - 8, length + 8);
        }
        if (cursor == limit)
        {
            state = State.CLOSE;
        }
        return 4;
    }

    private int init()
    {
        if (reply.isConnected())
        {
            state = State.SENDING;
            return 1;
        }
        return 0;
    }
}
