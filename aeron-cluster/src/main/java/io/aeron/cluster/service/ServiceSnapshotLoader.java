/*
 * Copyright 2018 Real Logic Ltd.
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
package io.aeron.cluster.service;

import io.aeron.Image;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import static io.aeron.cluster.service.ClusteredServiceContainer.SNAPSHOT_TYPE_ID;

class ServiceSnapshotLoader implements ControlledFragmentHandler
{
    private static final int FRAGMENT_LIMIT = 10;

    private boolean inSnapshot = false;
    private boolean isDone = false;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SnapshotMarkerDecoder snapshotMarkerDecoder = new SnapshotMarkerDecoder();
    private final ClientSessionDecoder clientSessionDecoder = new ClientSessionDecoder();
    private final Image image;
    private final ClusteredServiceAgent agent;

    ServiceSnapshotLoader(final Image image, final ClusteredServiceAgent agent)
    {
        this.image = image;
        this.agent = agent;
    }

    public boolean isDone()
    {
        return isDone;
    }

    public int poll()
    {
        return image.controlledPoll(this, FRAGMENT_LIMIT);
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SnapshotMarkerDecoder.TEMPLATE_ID:
                snapshotMarkerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long typeId = snapshotMarkerDecoder.typeId();
                if (typeId != SNAPSHOT_TYPE_ID)
                {
                    throw new IllegalStateException("Unexpected snapshot type: " + typeId);
                }

                switch (snapshotMarkerDecoder.mark())
                {
                    case BEGIN:
                        if (inSnapshot)
                        {
                            throw new IllegalStateException("Already in snapshot");
                        }
                        inSnapshot = true;
                        return Action.CONTINUE;

                    case END:
                        if (!inSnapshot)
                        {
                            throw new IllegalStateException("Missing begin snapshot");
                        }
                        isDone = true;
                        return Action.BREAK;
                }
                break;

            case ClientSessionDecoder.TEMPLATE_ID:
                clientSessionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final String responseChannel = clientSessionDecoder.responseChannel();
                final byte[] principalData = new byte[clientSessionDecoder.principalDataLength()];
                clientSessionDecoder.getPrincipalData(principalData, 0, principalData.length);

                agent.addSession(
                    clientSessionDecoder.clusterSessionId(),
                    clientSessionDecoder.responseStreamId(),
                    responseChannel,
                    principalData);
                break;

            default:
                System.out.println("offset = " + offset);
                throw new IllegalStateException("Unknown template id: " + templateId);
        }

        return Action.CONTINUE;
    }
}
