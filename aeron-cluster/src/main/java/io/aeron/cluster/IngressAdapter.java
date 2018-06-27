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

    private Subscription subscription;
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this);
    private final ConsensusModuleAgent consensusModuleAgent;
    private final AtomicCounter invalidRequests;

    IngressAdapter(final ConsensusModuleAgent consensusModuleAgent, final AtomicCounter invalidRequests)
    {
        this.consensusModuleAgent = consensusModuleAgent;
        this.invalidRequests = invalidRequests;
    }

    public void close()
    {
        CloseHelper.close(subscription);
        subscription = null;
        fragmentAssembler.clear();
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);
        final int templateId = messageHeaderDecoder.templateId();

        if (templateId == SessionHeaderDecoder.TEMPLATE_ID)
        {
            sessionHeaderDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            return consensusModuleAgent.onSessionMessage(
                buffer,
                offset,
                length,
                sessionHeaderDecoder.clusterSessionId(),
                sessionHeaderDecoder.correlationId());
        }

        switch (templateId)
        {
            case SessionConnectRequestDecoder.TEMPLATE_ID:
            {
                connectRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final String responseChannel = connectRequestDecoder.responseChannel();

                final byte[] credentials = new byte[connectRequestDecoder.encodedCredentialsLength()];
                connectRequestDecoder.getEncodedCredentials(credentials, 0, credentials.length);

                consensusModuleAgent.onSessionConnect(
                    connectRequestDecoder.correlationId(),
                    connectRequestDecoder.responseStreamId(),
                    responseChannel,
                    credentials);
                break;
            }

            case SessionCloseRequestDecoder.TEMPLATE_ID:
            {
                closeRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onSessionClose(closeRequestDecoder.clusterSessionId());
                break;
            }

            case SessionKeepAliveRequestDecoder.TEMPLATE_ID:
            {
                keepAliveRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onSessionKeepAlive(keepAliveRequestDecoder.clusterSessionId());
                break;
            }

            case ChallengeResponseDecoder.TEMPLATE_ID:
            {
                challengeResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final byte[] credentials = new byte[challengeResponseDecoder.encodedCredentialsLength()];
                challengeResponseDecoder.getEncodedCredentials(credentials, 0, credentials.length);

                consensusModuleAgent.onChallengeResponse(
                    challengeResponseDecoder.correlationId(),
                    challengeResponseDecoder.clusterSessionId(),
                    credentials);
                break;
            }

            default:
                invalidRequests.incrementOrdered();
        }

        return Action.CONTINUE;
    }

    void subscription(final Subscription subscription)
    {
        this.subscription = subscription;
    }

    int poll()
    {
        if (null == subscription)
        {
            return 0;
        }

        return subscription.controlledPoll(fragmentAssembler, FRAGMENT_POLL_LIMIT);
    }

    void freeSessionBuffer(final int imageSessionId)
    {
        fragmentAssembler.freeSessionBuffer(imageSessionId);
    }
}
