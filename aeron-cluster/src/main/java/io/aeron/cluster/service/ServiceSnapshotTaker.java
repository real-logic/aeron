/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.cluster.service;

import io.aeron.ExclusivePublication;
import io.aeron.cluster.codecs.ClientSessionEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.IdleStrategy;

final class ServiceSnapshotTaker extends SnapshotTaker
{
    private final ExpandableArrayBuffer offerBuffer = new ExpandableArrayBuffer(1024);
    private final ClientSessionEncoder clientSessionEncoder = new ClientSessionEncoder();

    ServiceSnapshotTaker(
        final ExclusivePublication publication, final IdleStrategy idleStrategy, final AgentInvoker aeronClientInvoker)
    {
        super(publication, idleStrategy, aeronClientInvoker);
    }

    void snapshotSession(final ClientSession session)
    {
        final String responseChannel = session.responseChannel();
        final byte[] encodedPrincipal = session.encodedPrincipal();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClientSessionEncoder.BLOCK_LENGTH +
            ClientSessionEncoder.responseChannelHeaderLength() + responseChannel.length() +
            ClientSessionEncoder.encodedPrincipalHeaderLength() + encodedPrincipal.length;
        if (length <= publication.maxPayloadLength())
        {
            idleStrategy.reset();
            while (true)
            {
                final long result = publication.tryClaim(length, bufferClaim);
                if (result > 0)
                {
                    final MutableDirectBuffer buffer = bufferClaim.buffer();
                    final int offset = bufferClaim.offset();

                    encodeSession(session, responseChannel, encodedPrincipal, buffer, offset);
                    bufferClaim.commit();
                    break;
                }

                checkResultAndIdle(result);
            }
        }
        else
        {
            final int offset = 0;
            encodeSession(session, responseChannel, encodedPrincipal, offerBuffer, offset);
            offer(offerBuffer, offset, length);
        }
    }

    private void encodeSession(
        final ClientSession session,
        final String responseChannel,
        final byte[] encodedPrincipal,
        final MutableDirectBuffer buffer,
        final int offset)
    {
        clientSessionEncoder
            .wrapAndApplyHeader(buffer, offset, messageHeaderEncoder)
            .clusterSessionId(session.id())
            .responseStreamId(session.responseStreamId())
            .responseChannel(responseChannel)
            .putEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.length);
    }
}
