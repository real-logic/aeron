/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.Subscription;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

final class ServiceAdapter implements FragmentHandler, AutoCloseable
{
    private final Subscription subscription;
    private final ClusteredServiceAgent clusteredServiceAgent;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final JoinLogDecoder joinLogDecoder = new JoinLogDecoder();

    ServiceAdapter(final Subscription subscription, final ClusteredServiceAgent clusteredServiceAgent)
    {
        this.subscription = subscription;
        this.clusteredServiceAgent = clusteredServiceAgent;
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

        final int templateId = messageHeaderDecoder.templateId();
        if (JoinLogDecoder.TEMPLATE_ID != templateId)
        {
            throw new ClusterException("unknown template id: " + templateId);
        }

        joinLogDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        clusteredServiceAgent.onJoinLog(
            joinLogDecoder.leadershipTermId(),
            joinLogDecoder.commitPositionId(),
            joinLogDecoder.logSessionId(),
            joinLogDecoder.logStreamId(),
            joinLogDecoder.logChannel());
    }
}
