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

import io.aeron.Publication;
import io.aeron.cluster.codecs.*;
import io.aeron.cluster.service.SnapshotTaker;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.IdleStrategy;

class ConsensusModuleSnapshotTaker extends SnapshotTaker
{
    private static final int ENCODED_TIMER_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + TimerEncoder.BLOCK_LENGTH;

    private final ClusterSessionEncoder clusterSessionEncoder = new ClusterSessionEncoder();
    private final TimerEncoder timerEncoder = new TimerEncoder();
    private final ConsensusModuleEncoder consensusModuleEncoder = new ConsensusModuleEncoder();

    ConsensusModuleSnapshotTaker(
        final Publication publication, final IdleStrategy idleStrategy, final AgentInvoker aeronClientInvoker)
    {
        super(publication, idleStrategy, aeronClientInvoker);
    }

    void snapshotSession(final ClusterSession session)
    {
        final String responseChannel = session.responseChannel();
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ClusterSessionEncoder.BLOCK_LENGTH +
            ClusterSessionEncoder.responseChannelHeaderLength() + responseChannel.length();

        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                clusterSessionEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .clusterSessionId(session.id())
                    .lastCorrelationId(session.lastCorrelationId())
                    .timeOfLastActivity(session.timeOfLastActivityMs())
                    .openedLogPosition(session.openedLogPosition())
                    .closeReason(session.closeReason())
                    .responseStreamId(session.responseStreamId())
                    .responseChannel(responseChannel);

                bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }
    }

    void snapshotTimer(final long correlationId, final long deadline)
    {
        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(ENCODED_TIMER_LENGTH, bufferClaim);
            if (result > 0)
            {
                timerEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .correlationId(correlationId)
                    .deadline(deadline);

                bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }
    }

    void consensusModuleState(final long nextSessionId)
    {
        final int length = MessageHeaderEncoder.ENCODED_LENGTH + ConsensusModuleEncoder.BLOCK_LENGTH;

        idleStrategy.reset();
        while (true)
        {
            final long result = publication.tryClaim(length, bufferClaim);
            if (result > 0)
            {
                consensusModuleEncoder
                    .wrapAndApplyHeader(bufferClaim.buffer(), bufferClaim.offset(), messageHeaderEncoder)
                    .nextSessionId(nextSessionId);

                bufferClaim.commit();
                break;
            }

            checkResultAndIdle(result);
        }
    }
}
