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

import io.aeron.ExclusivePublication;
import io.aeron.archiver.codecs.*;
import io.aeron.logbuffer.ExclusiveBufferClaim;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

class ListDescriptorsSession implements ArchiveConductor.Session
{
    static final long NOT_FOUND_HEADER;
    static final long DESCRIPTOR_HEADER;

    static
    {
        // create constant header values to avoid recalcuation on each meassage sent
        final MessageHeaderEncoder encoder = new MessageHeaderEncoder();
        encoder.wrap(new UnsafeBuffer(new byte[8]), 0);
        encoder.schemaId(ListStreamInstancesNotFoundResponseDecoder.SCHEMA_ID);
        encoder.version(ListStreamInstancesNotFoundResponseDecoder.SCHEMA_VERSION);
        encoder.blockLength(ListStreamInstancesNotFoundResponseDecoder.BLOCK_LENGTH);
        encoder.templateId(ListStreamInstancesNotFoundResponseDecoder.TEMPLATE_ID);
        NOT_FOUND_HEADER = encoder.buffer().getLong(0);
        encoder.schemaId(ArchiveDescriptorDecoder.SCHEMA_ID);
        encoder.version(ArchiveDescriptorDecoder.SCHEMA_VERSION);
        encoder.blockLength(ArchiveDescriptorDecoder.BLOCK_LENGTH);
        encoder.templateId(ArchiveDescriptorDecoder.TEMPLATE_ID);
        DESCRIPTOR_HEADER = encoder.buffer().getLong(0);
    }

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
    private final ExclusiveBufferClaim bufferClaim = new ExclusiveBufferClaim();

    private final ExclusivePublication reply;
    private final int from;
    private final int to;
    private final ArchiveIndex index;
    private final ArchiverProtocolProxy proxy;
    private final int correlationId;

    private int cursor;
    private State state = State.INIT;

    ListDescriptorsSession(
        final int correlationId, final ExclusivePublication reply,
        final int from,
        final int to,
        final ArchiveIndex index,
        final ArchiverProtocolProxy proxy)
    {
        this.reply = reply;
        cursor = from;
        this.from = from;
        this.to = to;
        this.index = index;
        this.proxy = proxy;
        this.correlationId = correlationId;
    }

    public void abort()
    {
        state = State.CLOSE;
    }

    public boolean isDone()
    {
        return state == State.DONE;
    }

    public void remove(final ArchiveConductor conductor)
    {
    }

    public int doWork()
    {
        int workDone = 0;

        switch (state)
        {
            case INIT:
                workDone += init();
                break;

            case SENDING:
                workDone += sendDescriptors();
                break;

            case CLOSE:
                workDone += close();
                break;

        }

        return workDone;
    }

    private int close()
    {
        state = State.DONE;
        return 1;
    }

    private int sendDescriptors()
    {
        final int limit = Math.min(cursor + 4, to);
        for (; cursor <= limit; cursor++)
        {
            final ArchivingSession session = index.getArchivingSession(cursor);
            if (session == null)
            {
                byteBuffer.clear();
                unsafeBuffer.wrap(byteBuffer);
                try
                {
                    if (!index.readArchiveDescriptor(cursor, byteBuffer))
                    {
                        // return relevant error
                        if (reply.tryClaim(
                            8 + ListStreamInstancesNotFoundResponseDecoder.BLOCK_LENGTH, bufferClaim) > 0L)
                        {
                            final MutableDirectBuffer buffer = bufferClaim.buffer();
                            final int offset = bufferClaim.offset();
                            buffer.putLong(offset, NOT_FOUND_HEADER);
                            buffer.putInt(offset + 8, cursor);
                            buffer.putInt(offset + 12, index.maxStreamInstanceId());
                            bufferClaim.commit();
                            state = State.CLOSE;
                        }

                        return 0;
                    }
                }
                catch (final IOException ex)
                {
                    state = State.CLOSE;
                    LangUtil.rethrowUnchecked(ex);
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

        if (cursor > to)
        {
            state = State.CLOSE;
        }

        return 4;
    }

    private int init()
    {
        if (!reply.isConnected())
        {
            // TODO: timeout
            return 0;
        }

        if (from > to)
        {
            proxy.sendResponse(reply, "Requested range is reversed (to < from)", correlationId);
            state = State.CLOSE;
        }
        else if (to > index.maxStreamInstanceId())
        {
            proxy.sendResponse(reply, "Requested range exceeds available range (to > max)", correlationId);
            state = State.CLOSE;
        }
        else
        {
            state = State.SENDING;
        }

        return 1;
    }
}
