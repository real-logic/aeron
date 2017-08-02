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
package io.aeron.archive;

import io.aeron.Publication;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.ControlResponseEncoder;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import io.aeron.archive.codecs.RecordingDescriptorEncoder;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.Strings;
import org.agrona.concurrent.UnsafeBuffer;

class ControlSessionProxy
{
    private static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private final ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(2048);

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ControlResponseEncoder responseEncoder = new ControlResponseEncoder();
    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();

    boolean sendOkResponse(final long correlationId, final Publication controlPublication)
    {
        return sendResponse(correlationId, ControlResponseCode.OK, null, controlPublication);
    }

    boolean sendRecordingUnknown(final long correlationId, final long recordingId, final Publication controlPublication)
    {
        return sendResponse(
            correlationId,
            recordingId,
            ControlResponseCode.RECORDING_UNKNOWN,
            null,
            controlPublication);
    }

    boolean sendResponse(
        final long correlationId,
        final ControlResponseCode code,
        final String errorMessage,
        final Publication controlPublication)
    {
        return sendResponse(
            correlationId,
            0,
            code,
            errorMessage,
            controlPublication);
    }

    private boolean sendResponse(
        final long correlationId,
        final long relevantId,
        final ControlResponseCode code,
        final String errorMessage,
        final Publication controlPublication)
    {
        responseEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .correlationId(correlationId)
            .relevantId(relevantId)
            .code(code);

        if (!Strings.isEmpty(errorMessage))
        {
            responseEncoder.errorMessage(errorMessage);
        }
        else
        {
            responseEncoder.putErrorMessage(EMPTY_BYTE_ARRAY, 0, 0);
        }

        return send(controlPublication, HEADER_LENGTH + responseEncoder.encodedLength());
    }

    int sendDescriptor(
        final long correlationId,
        final UnsafeBuffer descriptorBuffer,
        final Publication controlPublication)
    {
        final int offset = Catalog.DESCRIPTOR_HEADER_LENGTH - HEADER_LENGTH;
        final int length = descriptorBuffer.getInt(0) + HEADER_LENGTH;

        recordingDescriptorEncoder
            .wrapAndApplyHeader(descriptorBuffer, offset, messageHeaderEncoder)
            .correlationId(correlationId);

        if (send(controlPublication, descriptorBuffer, offset, length))
        {
            return length;
        }
        else
        {
            return 0;
        }
    }

    private boolean send(final Publication controlPublication, final int length)
    {
        return send(controlPublication, buffer, 0, length);
    }

    private boolean send(
        final Publication controlPublication,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {

        for (int i = 0; i < 3; i++)
        {
            final long result = controlPublication.offer(buffer, offset, length);
            if (result > 0)
            {
                return true;
            }

            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED)
            {
                throw new IllegalStateException("Response channel is down: " + controlPublication);
            }
        }
        return false;
    }
}
