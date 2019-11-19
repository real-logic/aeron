/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

final class ConsensusModuleAdapter implements AutoCloseable
{
    private static final int FRAGMENT_LIMIT = 10;
    private final Subscription subscription;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final ScheduleTimerDecoder scheduleTimerDecoder = new ScheduleTimerDecoder();
    private final CancelTimerDecoder cancelTimerDecoder = new CancelTimerDecoder();
    private final ServiceAckDecoder serviceAckDecoder = new ServiceAckDecoder();
    private final CloseSessionDecoder closeSessionDecoder = new CloseSessionDecoder();
    private final ClusterMembersQueryDecoder clusterMembersQueryDecoder = new ClusterMembersQueryDecoder();
    private final RemoveMemberDecoder removeMemberDecoder = new RemoveMemberDecoder();
    private final AddMemberDecoder addMemberDecoder = new AddMemberDecoder();
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onFragment);

    ConsensusModuleAdapter(final Subscription subscription, final ConsensusModuleAgent consensusModuleAgent)
    {
        this.subscription = subscription;
        this.consensusModuleAgent = consensusModuleAgent;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    int poll()
    {
        return subscription.poll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    @SuppressWarnings({"unused", "MethodLength"})
    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SessionMessageHeaderDecoder.TEMPLATE_ID:
                sessionMessageHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onServiceMessage(
                    sessionMessageHeaderDecoder.leadershipTermId(),
                    buffer,
                    offset + AeronCluster.SESSION_HEADER_LENGTH,
                    length - AeronCluster.SESSION_HEADER_LENGTH);
                break;

            case CloseSessionDecoder.TEMPLATE_ID:
                closeSessionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onServiceCloseSession(closeSessionDecoder.clusterSessionId());
                break;

            case ScheduleTimerDecoder.TEMPLATE_ID:
                scheduleTimerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onScheduleTimer(
                    scheduleTimerDecoder.correlationId(),
                    scheduleTimerDecoder.deadline());
                break;

            case CancelTimerDecoder.TEMPLATE_ID:
                cancelTimerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onCancelTimer(cancelTimerDecoder.correlationId());
                break;

            case ServiceAckDecoder.TEMPLATE_ID:
                serviceAckDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onServiceAck(
                    serviceAckDecoder.logPosition(),
                    serviceAckDecoder.timestamp(),
                    serviceAckDecoder.ackId(),
                    serviceAckDecoder.relevantId(),
                    serviceAckDecoder.serviceId());
                break;

            case ClusterMembersQueryDecoder.TEMPLATE_ID:
                clusterMembersQueryDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onClusterMembersQuery(
                    clusterMembersQueryDecoder.correlationId(),
                    BooleanType.TRUE == clusterMembersQueryDecoder.extended());
                break;

            case RemoveMemberDecoder.TEMPLATE_ID:
                removeMemberDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onRemoveMember(
                    removeMemberDecoder.correlationId(),
                    removeMemberDecoder.memberId(),
                    BooleanType.TRUE == removeMemberDecoder.isPassive());
                break;

            case AddMemberDecoder.TEMPLATE_ID:
                addMemberDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onAddMember(
                    addMemberDecoder.correlationId(),
                    addMemberDecoder.memberId(),
                    addMemberDecoder.memberEndpoints());
                break;
        }
    }
}
