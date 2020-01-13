/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.agent;

import io.aeron.archive.codecs.MessageHeaderDecoder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.util.EnumSet;

import static io.aeron.agent.ArchiveEventCode.*;
import static io.aeron.agent.CommonEventEncoder.encode;
import static io.aeron.agent.EventConfiguration.ARCHIVE_EVENT_CODES;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.util.EnumSet.complementOf;
import static java.util.EnumSet.of;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.BufferUtil.allocateDirectAligned;

/**
 * Event logger interface used by interceptors for recording events into a {@link RingBuffer} for an
 * {@link io.aeron.archive.Archive} for via a Java Agent.
 */
public final class ArchiveEventLogger
{
    static final EnumSet<ArchiveEventCode> CONTROL_REQUEST_EVENTS = complementOf(of(CMD_OUT_RESPONSE));

    public static final ArchiveEventLogger LOGGER = new ArchiveEventLogger(EventConfiguration.EVENT_RING_BUFFER);

    private final UnsafeBuffer encodedBuffer =
        new UnsafeBuffer(allocateDirectAligned(MAX_EVENT_LENGTH, CACHE_LINE_LENGTH));
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ManyToOneRingBuffer ringBuffer;

    ArchiveEventLogger(final ManyToOneRingBuffer eventRingBuffer)
    {
        ringBuffer = eventRingBuffer;
    }

    public static int toEventCodeId(final ArchiveEventCode code)
    {
        return EVENT_CODE_TYPE << 16 | (code.id() & 0xFFFF);
    }

    public void logControlRequest(final DirectBuffer buffer, final int offset, final int length)
    {
        headerDecoder.wrap(buffer, offset);

        final int templateId = headerDecoder.templateId();
        final ArchiveEventCode eventCode = getByTemplateId(templateId);
        if (eventCode != null && ARCHIVE_EVENT_CODES.contains(eventCode))
        {
            log(eventCode, buffer, offset, length);
        }
    }

    public void logControlResponse(final DirectBuffer buffer, final int length)
    {
        log(CMD_OUT_RESPONSE, buffer, 0, length);
    }

    private void log(final ArchiveEventCode eventCode, final DirectBuffer buffer, final int offset, final int length)
    {
        final int encodedLength = encode(encodedBuffer, buffer, offset, length);

        ringBuffer.write(toEventCodeId(eventCode), encodedBuffer, 0, encodedLength);
    }
}
