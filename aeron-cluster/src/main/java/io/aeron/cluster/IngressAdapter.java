/*
 * Copyright 2014-2021 Real Logic Limited.
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

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;

class IngressAdapter implements ControlledFragmentHandler, AutoCloseable
{
    private final int fragmentPollLimit;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionConnectRequestDecoder connectRequestDecoder = new SessionConnectRequestDecoder();
    private final SessionCloseRequestDecoder closeRequestDecoder = new SessionCloseRequestDecoder();
    private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
    private final SessionKeepAliveDecoder sessionKeepAliveDecoder = new SessionKeepAliveDecoder();
    private final ChallengeResponseDecoder challengeResponseDecoder = new ChallengeResponseDecoder();
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this);
    private final ConsensusModuleAgent consensusModuleAgent;
    private Subscription subscription;

    IngressAdapter(final int fragmentPollLimit, final ConsensusModuleAgent consensusModuleAgent)
    {
        this.fragmentPollLimit = fragmentPollLimit;
        this.consensusModuleAgent = consensusModuleAgent;
    }

    public void close()
    {
        final Subscription subscription = this.subscription;
        this.subscription = null;
        fragmentAssembler.clear();
        if (null != subscription)
        {
            subscription.close();
        }
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

        final int templateId = messageHeaderDecoder.templateId();
        if (templateId == SessionMessageHeaderDecoder.TEMPLATE_ID)
        {
            sessionMessageHeaderDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            return consensusModuleAgent.onIngressMessage(
                sessionMessageHeaderDecoder.leadershipTermId(),
                sessionMessageHeaderDecoder.clusterSessionId(),
                buffer,
                offset + AeronCluster.SESSION_HEADER_LENGTH,
                length - AeronCluster.SESSION_HEADER_LENGTH);
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
                final int credentialsLength = connectRequestDecoder.encodedCredentialsLength();
                final byte[] credentials;
                if (credentialsLength > 0)
                {
                    credentials = new byte[credentialsLength];
                    connectRequestDecoder.getEncodedCredentials(credentials, 0, credentialsLength);
                }
                else
                {
                    credentials = ArrayUtil.EMPTY_BYTE_ARRAY;
                }

                consensusModuleAgent.onSessionConnect(
                    connectRequestDecoder.correlationId(),
                    connectRequestDecoder.responseStreamId(),
                    connectRequestDecoder.version(),
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

                consensusModuleAgent.onSessionClose(
                    closeRequestDecoder.leadershipTermId(),
                    closeRequestDecoder.clusterSessionId());
                break;
            }

            case SessionKeepAliveDecoder.TEMPLATE_ID:
            {
                sessionKeepAliveDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                consensusModuleAgent.onSessionKeepAlive(
                    sessionKeepAliveDecoder.leadershipTermId(),
                    sessionKeepAliveDecoder.clusterSessionId()
                );
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
        }

        return Action.CONTINUE;
    }

    void connect(final Subscription subscription)
    {
        this.subscription = subscription;
    }

    int poll()
    {
        if (null != subscription)
        {
            return subscription.controlledPoll(fragmentAssembler, fragmentPollLimit);
        }

        return 0;
    }

    void freeSessionBuffer(final int imageSessionId)
    {
        fragmentAssembler.freeSessionBuffer(imageSessionId);
    }
}
