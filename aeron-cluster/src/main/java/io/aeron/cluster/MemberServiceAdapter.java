/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

import java.util.ArrayList;
import java.util.List;

class MemberServiceAdapter implements FragmentHandler, AutoCloseable
{
    interface MemberServiceHandler
    {
        void onClusterMembersResponse(
            long correlationId, int leaderMemberId, String activeMembers, String passiveMembers);

        void onClusterMembersExtendedResponse(
            long correlationId,
            long currentTimeNs,
            int leaderMemberId,
            int memberId,
            List<ClusterMember> activeMembers,
            List<ClusterMember> passiveMembers);
    }

    private final Subscription subscription;
    private final MemberServiceHandler handler;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ClusterMembersResponseDecoder clusterMembersResponseDecoder = new ClusterMembersResponseDecoder();
    private final ClusterMembersExtendedResponseDecoder clusterMembersExtendedResponseDecoder =
        new ClusterMembersExtendedResponseDecoder();

    MemberServiceAdapter(final Subscription subscription, final MemberServiceHandler handler)
    {
        this.subscription = subscription;
        this.handler = handler;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    public int poll()
    {
        return subscription.poll(this, 1);
    }

    @SuppressWarnings("MethodLength")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = messageHeaderDecoder.templateId();
        if (templateId == ClusterMembersResponseDecoder.TEMPLATE_ID)
        {
            clusterMembersResponseDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            handler.onClusterMembersResponse(
                clusterMembersResponseDecoder.correlationId(),
                clusterMembersResponseDecoder.leaderMemberId(),
                clusterMembersResponseDecoder.activeMembers(),
                clusterMembersResponseDecoder.passiveFollowers());
        }
        else if (templateId == ClusterMembersExtendedResponseDecoder.TEMPLATE_ID)
        {
            clusterMembersExtendedResponseDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final long correlationId = clusterMembersExtendedResponseDecoder.correlationId();
            final long currentTimeNs = clusterMembersExtendedResponseDecoder.currentTimeNs();
            final int leaderMemberId = clusterMembersExtendedResponseDecoder.leaderMemberId();
            final int memberId = clusterMembersExtendedResponseDecoder.memberId();

            final ArrayList<ClusterMember> activeMembers = new ArrayList<>();
            for (final ClusterMembersExtendedResponseDecoder.ActiveMembersDecoder activeMembersDecoder :
                clusterMembersExtendedResponseDecoder.activeMembers())
            {
                final int id = activeMembersDecoder.memberId();
                final String clientFacingEndpoint = activeMembersDecoder.clientFacingEndpoint();
                final String memberFacingEndpoint = activeMembersDecoder.memberFacingEndpoint();
                final String logEndpoint = activeMembersDecoder.logEndpoint();
                final String transferEndpoint = activeMembersDecoder.transferEndpoint();
                final String archiveEndpoint = activeMembersDecoder.archiveEndpoint();
                final String endpointsDetail = String.join(
                    ",",
                    clientFacingEndpoint,
                    memberFacingEndpoint,
                    logEndpoint,
                    transferEndpoint,
                    archiveEndpoint);

                final ClusterMember member = new ClusterMember(
                    id,
                    clientFacingEndpoint,
                    memberFacingEndpoint,
                    logEndpoint,
                    transferEndpoint,
                    archiveEndpoint,
                    endpointsDetail)
                    .leadershipTermId(activeMembersDecoder.leadershipTermId())
                    .logPosition(activeMembersDecoder.logPosition())
                    .timeOfLastAppendPositionNs(activeMembersDecoder.timeOfLastAppendNs());

                activeMembers.add(member);
            }

            final ArrayList<ClusterMember> passiveMembers = new ArrayList<>();
            for (final ClusterMembersExtendedResponseDecoder.PassiveMembersDecoder passiveMembersDecoder :
                clusterMembersExtendedResponseDecoder.passiveMembers())
            {
                final int id = passiveMembersDecoder.memberId();
                final String clientFacingEndpoint = passiveMembersDecoder.clientFacingEndpoint();
                final String memberFacingEndpoint = passiveMembersDecoder.memberFacingEndpoint();
                final String logEndpoint = passiveMembersDecoder.logEndpoint();
                final String transferEndpoint = passiveMembersDecoder.transferEndpoint();
                final String archiveEndpoint = passiveMembersDecoder.archiveEndpoint();
                final String endpointsDetail = String.join(
                    ",",
                    clientFacingEndpoint,
                    memberFacingEndpoint,
                    logEndpoint,
                    transferEndpoint,
                    archiveEndpoint);

                final ClusterMember member = new ClusterMember(
                    id,
                    clientFacingEndpoint,
                    memberFacingEndpoint,
                    logEndpoint,
                    transferEndpoint,
                    archiveEndpoint,
                    endpointsDetail)
                    .leadershipTermId(passiveMembersDecoder.leadershipTermId())
                    .logPosition(passiveMembersDecoder.logPosition())
                    .timeOfLastAppendPositionNs(passiveMembersDecoder.timeOfLastAppendNs());

                passiveMembers.add(member);
            }

            handler.onClusterMembersExtendedResponse(
                correlationId,
                currentTimeNs,
                leaderMemberId,
                memberId,
                activeMembers,
                passiveMembers);
        }
    }
}
