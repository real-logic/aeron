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
package io.aeron.cluster;

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;

class IngressAdapter implements ControlledFragmentHandler, AutoCloseable
{
    private static final int FRAGMENT_POLL_LIMIT = 10;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionConnectRequestDecoder connectRequestDecoder = new SessionConnectRequestDecoder();
    private final SessionCloseRequestDecoder closeRequestDecoder = new SessionCloseRequestDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final SessionKeepAliveRequestDecoder keepAliveRequestDecoder = new SessionKeepAliveRequestDecoder();
    private final ChallengeResponseDecoder challengeResponseDecoder = new ChallengeResponseDecoder();
    private final MembershipQueryDecoder membershipQueryDecoder = new MembershipQueryDecoder();

    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this);
    private final Subscription subscription;
    private final SequencerAgent sequencerAgent;
    private final AtomicCounter invalidRequests;

    IngressAdapter(
        final Subscription subscription, final SequencerAgent sequencerAgent, final AtomicCounter invalidRequests)
    {
        this.subscription = subscription;
        this.sequencerAgent = sequencerAgent;
        this.invalidRequests = invalidRequests;
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    public int poll()
    {
        return subscription.controlledPoll(fragmentAssembler, FRAGMENT_POLL_LIMIT);
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final byte[] credentials;

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SessionConnectRequestDecoder.TEMPLATE_ID:
                connectRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final String responseChannel = connectRequestDecoder.responseChannel();

                credentials = new byte[connectRequestDecoder.encodedCredentialsLength()];
                connectRequestDecoder.getEncodedCredentials(credentials, 0, credentials.length);

                sequencerAgent.onSessionConnect(
                    connectRequestDecoder.correlationId(),
                    connectRequestDecoder.responseStreamId(),
                    responseChannel,
                    credentials);
                break;

            case SessionHeaderDecoder.TEMPLATE_ID:
                sessionHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                return sequencerAgent.onSessionMessage(
                    buffer,
                    offset,
                    length,
                    sessionHeaderDecoder.clusterSessionId(),
                    sessionHeaderDecoder.correlationId());

            case SessionCloseRequestDecoder.TEMPLATE_ID:
                closeRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onSessionClose(closeRequestDecoder.clusterSessionId());
                break;

            case SessionKeepAliveRequestDecoder.TEMPLATE_ID:
                keepAliveRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onSessionKeepAlive(keepAliveRequestDecoder.clusterSessionId());
                break;

            case ChallengeResponseDecoder.TEMPLATE_ID:
                challengeResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                credentials = new byte[challengeResponseDecoder.encodedCredentialsLength()];
                challengeResponseDecoder.getEncodedCredentials(credentials, 0, credentials.length);

                sequencerAgent.onChallengeResponse(
                    challengeResponseDecoder.correlationId(),
                    challengeResponseDecoder.clusterSessionId(),
                    credentials);
                break;

            case MembershipQueryDecoder.TEMPLATE_ID:
                membershipQueryDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                sequencerAgent.onMembershipQuery(
                    membershipQueryDecoder.correlationId(),
                    membershipQueryDecoder.clusterSessionId(),
                    membershipQueryDecoder.queryType());
                break;

            default:
                invalidRequests.incrementOrdered();
        }

        return Action.CONTINUE;
    }
}
