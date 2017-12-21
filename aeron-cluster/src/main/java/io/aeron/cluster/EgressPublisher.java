/*
 * Copyright 2017 Real Logic Ltd.
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

import io.aeron.Publication;
import io.aeron.cluster.codecs.ChallengeEncoder;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionEventEncoder;
import io.aeron.logbuffer.BufferClaim;

class EgressPublisher
{
    private static final int SEND_ATTEMPTS = 3;

    private final BufferClaim bufferClaim = new BufferClaim();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final SessionEventEncoder sessionEventEncoder = new SessionEventEncoder();
    private final ChallengeEncoder challengeEncoder = new ChallengeEncoder();

    public boolean sendEvent(final ClusterSession session, final EventCode code, final String detail)
    {
        final Publication publication = session.responsePublication();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH +
            SessionEventEncoder.BLOCK_LENGTH +
            SessionEventEncoder.detailHeaderLength() +
            detail.length();

        int attempts = SEND_ATTEMPTS;
        do
        {
            if (publication.tryClaim(length, bufferClaim) > 0)
            {
                sessionEventEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(session.id())
                    .correlationId(session.lastCorrelationId())
                    .code(code)
                    .detail(detail);

                bufferClaim.commit();

                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }

    public boolean sendChallenge(
        final ClusterSession session, final long correlationId, final long sessionId, final byte[] challengeData)
    {
        final Publication publication = session.responsePublication();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH +
            ChallengeEncoder.BLOCK_LENGTH +
            ChallengeEncoder.challengeDataHeaderLength() +
            challengeData.length;

        int attempts = SEND_ATTEMPTS;
        do
        {
            if (publication.tryClaim(length, bufferClaim) > 0)
            {
                challengeEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(session.id())
                    .correlationId(session.lastCorrelationId())
                    .putChallengeData(challengeData, 0, challengeData.length);

                bufferClaim.commit();

                return true;
            }
        }
        while (--attempts > 0);

        return false;
    }
}
