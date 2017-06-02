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

import io.aeron.Publication;
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

    void sendOkResponse(final Publication controlPublication, final long correlationId)
    {
        sendError(controlPublication, ControlResponseCode.OK, null, correlationId);
    }

    void sendError(
        final Publication controlPublication,
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

        send(controlPublication, HEADER_LENGTH + responseEncoder.encodedLength());
    }

    private void send(final Publication controlPublication, final int length)
    {
        send(controlPublication, buffer, 0, length);
    }

    private void send(
        final Publication controlPublication,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        // TODO: handle dead/slow subscriber, this is not an acceptable place to get stuck
        while (true)
        {
            final long result = controlPublication.offer(buffer, offset, length);
            if (result > 0)
            {
                idleStrategy.reset();
                break;
            }

            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED)
            {
                throw new IllegalStateException("Response channel is down: " + controlPublication);
            }

            idleStrategy.idle();
        }
    }

    void sendDescriptorNotFound(
        final Publication controlPublication,
        final long recordingId,
        final long maxRecordingId,
        final long correlationId)
    {
        recordingNotFoundResponseEncoder.wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .recordingId(recordingId)
            .maxRecordingId(maxRecordingId);

        send(controlPublication, HEADER_LENGTH + recordingNotFoundResponseEncoder.encodedLength());
    }

    int sendDescriptor(
        final Publication controlPublication,
        final UnsafeBuffer descriptorBuffer,
        final long correlationId)
    {
        final int offset = Catalog.CATALOG_FRAME_LENGTH - HEADER_LENGTH;
        final int length = descriptorBuffer.getInt(0) + HEADER_LENGTH;

        recordingDescriptorEncoder
            .wrapAndApplyHeader(descriptorBuffer, offset, messageHeaderEncoder)
            .correlationId(correlationId);

        send(controlPublication, descriptorBuffer, offset, length);
        return length;
    }

    void sendReplayAborted(
        final Publication controlPublication,
        final long correlationId,
        final long replaySessionId,
        final long position)
    {
        replayAbortedEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .replayId(replaySessionId)
            .lastPosition(position);

        send(controlPublication, HEADER_LENGTH + replayAbortedEncoder.encodedLength());
    }
}
