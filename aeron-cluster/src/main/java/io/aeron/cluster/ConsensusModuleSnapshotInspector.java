/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.Image;
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.io.PrintStream;

import static io.aeron.cluster.ConsensusModule.Configuration.SNAPSHOT_TYPE_ID;

class ConsensusModuleSnapshotInspector implements ControlledFragmentHandler
{
    static final int FRAGMENT_LIMIT = 10;

    private boolean inSnapshot = false;
    private boolean isDone = false;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SnapshotMarkerDecoder snapshotMarkerDecoder = new SnapshotMarkerDecoder();
    private final ClusterSessionDecoder clusterSessionDecoder = new ClusterSessionDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final TimerDecoder timerDecoder = new TimerDecoder();
    private final ConsensusModuleDecoder consensusModuleDecoder = new ConsensusModuleDecoder();
    private final ClusterMembersDecoder clusterMembersDecoder = new ClusterMembersDecoder();
    private final PendingMessageTrackerDecoder pendingMessageTrackerDecoder = new PendingMessageTrackerDecoder();
    private final ImageControlledFragmentAssembler fragmentAssembler = new ImageControlledFragmentAssembler(this);
    private final Image image;
    final PrintStream out;

    ConsensusModuleSnapshotInspector(final Image image, final PrintStream out)
    {
        this.image = image;
        this.out = out;
    }

    boolean isDone()
    {
        return isDone;
    }

    int poll()
    {
        return image.controlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    @SuppressWarnings("MethodLength")
    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        switch (messageHeaderDecoder.templateId())
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
                sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                out.println("Pending Message:" +
                    " length=" + length +
                    " clusterSessionId=" + sessionMessageHeaderDecoder.clusterSessionId());
                break;

            case SnapshotMarkerDecoder.TEMPLATE_ID:
                snapshotMarkerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long typeId = snapshotMarkerDecoder.typeId();
                if (typeId != SNAPSHOT_TYPE_ID)
                {
                    throw new ClusterException("unexpected snapshot type: " + typeId);
                }

                switch (snapshotMarkerDecoder.mark())
                {
                    case BEGIN:
                        if (inSnapshot)
                        {
                            throw new ClusterException("already in snapshot");
                        }
                        inSnapshot = true;
                        out.println("Snapshot:" +
                            " appVersion=" + snapshotMarkerDecoder.appVersion() +
                            " timeUnit=" + ClusterClock.map(snapshotMarkerDecoder.timeUnit()));
                        return Action.CONTINUE;

                    case END:
                        if (!inSnapshot)
                        {
                            throw new ClusterException("missing begin snapshot");
                        }
                        isDone = true;
                        return Action.BREAK;
                }
                break;

            case ClusterSessionDecoder.TEMPLATE_ID:
                clusterSessionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                out.println("Cluster Session:" +
                    " clusterSessionId=" + clusterSessionDecoder.clusterSessionId() +
                    " correlationId=" + clusterSessionDecoder.correlationId() +
                    " openedLogPosition=" + clusterSessionDecoder.openedLogPosition() +
                    " timeOfLastActivity=" + clusterSessionDecoder.timeOfLastActivity() +
                    " closeReason=" + clusterSessionDecoder.closeReason() +
                    " responseStreamId=" + clusterSessionDecoder.responseStreamId() +
                    " responseChannel=" + clusterSessionDecoder.responseChannel());
                break;

            case TimerDecoder.TEMPLATE_ID:
                timerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                out.println("Timer:" +
                    " correlationId=" + timerDecoder.correlationId() +
                    " deadline=" + timerDecoder.deadline());
                break;

            case ConsensusModuleDecoder.TEMPLATE_ID:
                consensusModuleDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                out.println("Consensus Module State:" +
                    " nextSessionId=" + consensusModuleDecoder.nextSessionId() +
                    " nextServiceSessionId=" + consensusModuleDecoder.nextServiceSessionId() +
                    " logServiceSessionId=" + consensusModuleDecoder.logServiceSessionId() +
                    " pendingMessageCapacity=" + consensusModuleDecoder.pendingMessageCapacity());
                break;

            case ClusterMembersDecoder.TEMPLATE_ID:
                clusterMembersDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                out.println("Cluster Members:" +
                    " memberId=" + clusterMembersDecoder.memberId() +
                    " highMemberId=" + clusterMembersDecoder.highMemberId() +
                    " clusterMembers=" + clusterMembersDecoder.clusterMembers());
                break;

            case PendingMessageTrackerDecoder.TEMPLATE_ID:
                pendingMessageTrackerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                out.println("Pending Message Tracker:" +
                    " nextServiceSessionId=" + pendingMessageTrackerDecoder.nextServiceSessionId() +
                    " logServiceSessionId=" + pendingMessageTrackerDecoder.logServiceSessionId() +
                    " pendingMessageCapacity=" + pendingMessageTrackerDecoder.pendingMessageCapacity() +
                    " serviceId=" + pendingMessageTrackerDecoder.serviceId());
                break;
        }

        return Action.CONTINUE;
    }
}
