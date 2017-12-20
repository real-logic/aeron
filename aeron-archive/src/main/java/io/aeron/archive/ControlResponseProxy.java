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
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.archive.codecs.RecordingDescriptorEncoder.recordingIdEncodingOffset;

class ControlResponseProxy
{
    private static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int DESCRIPTOR_CONTENT_OFFSET = RecordingDescriptorHeaderDecoder.BLOCK_LENGTH +
        recordingIdEncodingOffset();

    private final ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer(2048);
    private final BufferClaim bufferClaim = new BufferClaim();

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ControlResponseEncoder responseEncoder = new ControlResponseEncoder();
    private final RecordingDescriptorEncoder recordingDescriptorEncoder = new RecordingDescriptorEncoder();

    int sendDescriptor(
        final long controlSessionId,
        final long correlationId,
        final UnsafeBuffer descriptorBuffer,
        final Publication controlPublication)
    {
        final int length = Catalog.descriptorLength(descriptorBuffer);

        for (int i = 0; i < 3; i++)
        {
            final long result = controlPublication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                final MutableDirectBuffer buffer = bufferClaim.buffer();
                final int bufferOffset = bufferClaim.offset();
                final int contentOffset = bufferOffset + HEADER_LENGTH + recordingIdEncodingOffset();
                final int contentLength = length - recordingIdEncodingOffset() - HEADER_LENGTH;

                recordingDescriptorEncoder
                    .wrapAndApplyHeader(buffer, bufferOffset, messageHeaderEncoder)
                    .controlSessionId(controlSessionId)
                    .correlationId(correlationId);

                buffer.putBytes(contentOffset, descriptorBuffer, DESCRIPTOR_CONTENT_OFFSET, contentLength);

                bufferClaim.commit();

                return length;
            }

            checkResult(controlPublication, result);
        }

        return 0;
    }

    boolean sendResponse(
        final long controlSessionId,
        final long correlationId,
        final long relevantId,
        final ControlResponseCode code,
        final String errorMessage,
        final Publication controlPublication)
    {
        responseEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .controlSessionId(controlSessionId)
            .correlationId(correlationId)
            .relevantId(relevantId)
            .code(code)
            .errorMessage(null == errorMessage ? "" : errorMessage);

        return send(controlPublication, buffer, HEADER_LENGTH + responseEncoder.encodedLength());
    }

    private boolean send(final Publication controlPublication, final DirectBuffer buffer, final int length)
    {
        for (int i = 0; i < 3; i++)
        {
            final long result = controlPublication.offer(buffer, 0, length);
            if (result > 0)
            {
                return true;
            }

            checkResult(controlPublication, result);
        }

        return false;
    }

    private static void checkResult(final Publication controlPublication, final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("Response channel is down: " + controlPublication.channel());
        }
    }
}
