/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

class MemberServiceAdapter implements FragmentHandler, AutoCloseable
{
    interface MemberServiceHandler
    {
        void onClusterMembersResponse(
            long correlationId, int leaderMemberId, String activeMembers, String passiveMembers);
    }

    private final Subscription subscription;
    private final MemberServiceHandler handler;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ClusterMembersResponseDecoder clusterMembersResponseDecoder = new ClusterMembersResponseDecoder();

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
    }
}
