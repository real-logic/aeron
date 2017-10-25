/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster.client;

import io.aeron.Publication;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionHeaderEncoder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;
import java.util.Objects;

import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Encapsulate the {@link Publication} to the cluster and when offering apply the cluster session header in a reserved
 * region at the beginning of the buffer. The session payload is expected to be found beyond the reserved region.
 * <p>
 * The session header is applied to the buffer in the reserved region before the message payload. The offered buffer
 * must have a reserved region of {@link #SESSION_HEADER_LENGTH} before the payload.
 * <p>
 * <b>Note:</b> This class is threadsafe if the provided {@link Publication} is threadsafe.
 */
public class OffsetSessionPublication
{
    /**
     * Length of the session header that will be prepended to the message and must be reserved from the offset
     * of offered buffers.
     */
    public static final int SESSION_HEADER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + SessionHeaderEncoder.BLOCK_LENGTH;

    private final long messageHeader;
    private final long clusterSessionId;
    private final Publication publication;

    public OffsetSessionPublication(final Publication publication, final long clusterSessionId)
    {
        Objects.requireNonNull(publication);
        this.publication = publication;
        this.clusterSessionId = clusterSessionId;

        final UnsafeBuffer headerBuffer = new UnsafeBuffer(new byte[MessageHeaderEncoder.ENCODED_LENGTH]);
        new MessageHeaderEncoder()
            .wrap(headerBuffer, 0)
            .blockLength(SessionHeaderEncoder.BLOCK_LENGTH)
            .templateId(SessionHeaderEncoder.TEMPLATE_ID)
            .schemaId(SessionHeaderEncoder.SCHEMA_ID)
            .version(SessionHeaderEncoder.SCHEMA_VERSION);

        messageHeader = headerBuffer.getLong(0);
    }

    /**
     * Get the wrapped session {@link Publication}.
     *
     * @return the wrapped session {@link Publication}.
     */
    public Publication publication()
    {
        return publication;
    }

    /**
     * Non-blocking publish of a partial buffer containing a message to a cluster.
     * <p>
     * This method is threadsafe if the {@link Publication} is threadsafe.
     *
     * @param correlationId to be used to identify the message to the cluster.
     * @param buffer        containing message plus reserved region for the session header.
     * @param offset        offset in the buffer at which the session header begins.
     * @param length        in bytes of the encoded message plus the {@link #SESSION_HEADER_LENGTH}.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)}.
     */
    public long offer(final long correlationId, final MutableDirectBuffer buffer, final int offset, final int length)
    {
        buffer.putLong(offset, messageHeader);
        buffer.putLong(offset + SIZE_OF_LONG, correlationId, ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(offset + SIZE_OF_LONG + SIZE_OF_LONG, clusterSessionId, ByteOrder.LITTLE_ENDIAN);

        return publication.offer(buffer, offset, length);
    }
}
