/*
 * Copyright 2014-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archiver;

import io.aeron.*;
import io.aeron.archiver.codecs.*;
import org.agrona.*;
import org.agrona.concurrent.*;

class ControlSessionProxy
{
    private static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private final IdleStrategy idleStrategy;
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ControlResponseEncoder responseEncoder = new ControlResponseEncoder();
    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();
    private final ReplayAbortedEncoder replayAbortedEncoder = new ReplayAbortedEncoder();
    private final RecordingNotFoundResponseEncoder recordingNotFoundResponseEncoder =
        new RecordingNotFoundResponseEncoder();

    ControlSessionProxy(final IdleStrategy idleStrategy)
    {
        this.idleStrategy = idleStrategy;
    }

    void sendOkResponse(final ExclusivePublication reply, final long correlationId)
    {
        sendError(reply, ControlResponseCode.OK, null, correlationId);
    }

    void sendError(
        final ExclusivePublication reply,
        final ControlResponseCode code,
        final String errorMessage,
        final long correlationId)
    {
        responseEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .code(code);

        if (!Strings.isEmpty(errorMessage))
        {
            responseEncoder.errorMessage(errorMessage);
        }
        else
        {
            responseEncoder.putErrorMessage(EMPTY_BYTE_ARRAY, 0, 0);
        }

        send(reply, HEADER_LENGTH + responseEncoder.encodedLength());
    }

    private void send(final ExclusivePublication reply, final int length)
    {
        send(reply, buffer, 0, length);
    }

    private void send(
        final ExclusivePublication reply,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        // TODO: handle dead/slow subscriber, this is not an acceptable place to get stuck
        while (true)
        {
            final long result = reply.offer(buffer, offset, length);
            if (result > 0)
            {
                idleStrategy.reset();
                break;
            }

            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED)
            {
                throw new IllegalStateException("Response channel is down: " + reply);
            }

            idleStrategy.idle();
        }
    }

    void sendDescriptorNotFound(
        final ExclusivePublication reply,
        final long recordingId,
        final long maxRecordingId,
        final long correlationId)
    {
        recordingNotFoundResponseEncoder.wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .maxRecordingId(maxRecordingId);

        send(reply, HEADER_LENGTH + recordingNotFoundResponseEncoder.encodedLength());
    }

    int sendDescriptor(
        final ExclusivePublication reply,
        final UnsafeBuffer descriptorBuffer,
        final long correlationId)
    {
        final int offset = Catalog.CATALOG_FRAME_LENGTH - HEADER_LENGTH;
        final int length = descriptorBuffer.getInt(0) + HEADER_LENGTH;

        recordingDescriptorEncoder
            .wrapAndApplyHeader(descriptorBuffer, offset, messageHeaderEncoder)
            .correlationId(correlationId);

        send(reply, descriptorBuffer, offset, length);
        return length;
    }

    void sendReplayAborted(
        final ExclusivePublication reply,
        final long correlationId,
        final long replaySessionId,
        final long position)
    {
        replayAbortedEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .replayId(replaySessionId)
            .lastPosition(position);

        send(reply, HEADER_LENGTH + replayAbortedEncoder.encodedLength());
    }
}
