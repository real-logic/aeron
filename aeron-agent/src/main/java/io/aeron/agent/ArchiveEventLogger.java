/*
 * Copyright 2014-2021 Real Logic Limited.
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
import static io.aeron.agent.ArchiveEventEncoder.*;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.ARCHIVE_EVENT_CODES;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static java.util.EnumSet.complementOf;
import static java.util.EnumSet.of;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Event logger interface used by interceptors for recording events into a {@link RingBuffer} for an
 * {@link io.aeron.archive.Archive} for via a Java Agent.
 */
public final class ArchiveEventLogger
{
    public static final ArchiveEventLogger LOGGER = new ArchiveEventLogger(EVENT_RING_BUFFER);

    static final EnumSet<ArchiveEventCode> CONTROL_REQUEST_EVENTS = complementOf(of(
        CMD_OUT_RESPONSE,
        REPLICATION_SESSION_STATE_CHANGE,
        CONTROL_SESSION_STATE_CHANGE,
        REPLAY_SESSION_ERROR,
        CATALOG_RESIZE));

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ManyToOneRingBuffer ringBuffer;

    ArchiveEventLogger(final ManyToOneRingBuffer eventRingBuffer)
    {
        ringBuffer = eventRingBuffer;
    }

    /**
     * Log in incoming control request to the archive.
     *
     * @param buffer containing the encoded request.
     * @param offset in the buffer at which the request begins.
     * @param length of the request in the buffer.
     */
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

    /**
     * Log an outgoing control response from the archive.
     *
     * @param buffer containing the encoded response.
     * @param length of the response in the buffer.
     */
    public void logControlResponse(final DirectBuffer buffer, final int length)
    {
        log(CMD_OUT_RESPONSE, buffer, 0, length);
    }

    /**
     * Log a state change event for a archive control session
     *
     * @param eventCode        for the type of state change.
     * @param oldState         before the change.
     * @param newState         after the change.
     * @param controlSessionId identity for the control session on the Archive.
     * @param <E>              type representing the state change.
     */
    public <E extends Enum<E>> void logSessionStateChange(
        final ArchiveEventCode eventCode, final E oldState, final E newState, final long controlSessionId)
    {
        final int length = sessionStateChangeLength(oldState, newState);
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(eventCode.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeSessionStateChange(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    oldState,
                    newState,
                    controlSessionId);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a control response error.
     *
     * @param sessionId    associated with the response.
     * @param recordingId  to which the error applies.
     * @param errorMessage which resulted.
     */
    public void logReplaySessionError(final long sessionId, final long recordingId, final String errorMessage)
    {
        final int length = SIZE_OF_LONG * 2 + SIZE_OF_INT + errorMessage.length();
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(REPLAY_SESSION_ERROR.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeReplaySessionError(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    sessionId,
                    recordingId,
                    errorMessage);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log a Catalog resize event.
     *
     * @param oldCatalogLength before the resize.
     * @param newCatalogLength after the resize.
     */
    public void logCatalogResize(final long oldCatalogLength, final long newCatalogLength)
    {
        final int length = SIZE_OF_LONG * 2;
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(CATALOG_RESIZE.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encodeCatalogResize(
                    (UnsafeBuffer)ringBuffer.buffer(),
                    index,
                    captureLength,
                    length,
                    oldCatalogLength,
                    newCatalogLength);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }

    /**
     * Log an Archive control event.
     *
     * @param eventCode for the type of control event.
     * @param buffer    containing the encoded event.
     * @param offset    in the buffer at which the event begins.
     * @param length    of the encoded event.
     */
    private void log(final ArchiveEventCode eventCode, final DirectBuffer buffer, final int offset, final int length)
    {
        final int captureLength = captureLength(length);
        final int encodedLength = encodedLength(captureLength);
        final ManyToOneRingBuffer ringBuffer = this.ringBuffer;
        final int index = ringBuffer.tryClaim(eventCode.toEventCodeId(), encodedLength);

        if (index > 0)
        {
            try
            {
                encode((UnsafeBuffer)ringBuffer.buffer(), index, captureLength, length, buffer, offset);
            }
            finally
            {
                ringBuffer.commit(index);
            }
        }
    }
}
