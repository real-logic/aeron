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

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;

class LogReplay
{
    private final long recordingId;
    private final long startPosition;
    private final long stopPosition;
    private final long leadershipTermId;
    private final int logSessionId;
    private final int replayStreamId;
    private final AeronArchive archive;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final ConsensusModule.Context ctx;
    private final LogAdapter logAdapter;
    private final Subscription logSubscription;

    private long replaySessionId = Aeron.NULL_VALUE;
    private boolean isDone = false;

    LogReplay(
        final AeronArchive archive,
        final long recordingId,
        final long startPosition,
        final long stopPosition,
        final long leadershipTermId,
        final int logSessionId,
        final LogAdapter logAdapter,
        final ConsensusModule.Context ctx)
    {
        this.archive = archive;
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.leadershipTermId = leadershipTermId;
        this.logSessionId = logSessionId;
        this.logAdapter = logAdapter;
        this.consensusModuleAgent = logAdapter.consensusModuleAgent();
        this.ctx = ctx;
        this.replayStreamId = ctx.replayStreamId();

        final ChannelUri channelUri = ChannelUri.parse(ctx.replayChannel());
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(logSessionId));
        logSubscription = ctx.aeron().addSubscription(channelUri.toString(), replayStreamId);
    }

    public void close()
    {
        logAdapter.image(null);
        CloseHelper.close(ctx.countedErrorHandler(), logSubscription);
    }

    int doWork(@SuppressWarnings("unused") final long nowMs)
    {
        int workCount = 0;

        if (Aeron.NULL_VALUE == replaySessionId)
        {
            final String channel = logSubscription.channel();
            consensusModuleAgent.awaitServicesReadyForReplay(
                channel, replayStreamId, logSessionId, leadershipTermId, startPosition, stopPosition);

            final long length = stopPosition - startPosition;
            replaySessionId = archive.startReplay(recordingId, startPosition, length, channel, replayStreamId);
            workCount += 1;
        }
        else if (!isDone)
        {
            if (null == logAdapter.image())
            {
                final Image image = logSubscription.imageBySessionId((int)replaySessionId);
                if (null != image)
                {
                    logAdapter.image(image);
                    workCount += 1;
                }
            }
            else
            {
                workCount += consensusModuleAgent.replayLogPoll(logAdapter, stopPosition);
                if (logAdapter.position() >= stopPosition)
                {
                    isDone = true;
                    workCount += 1;
                }
            }
        }

        return workCount;
    }

    boolean isDone()
    {
        return isDone;
    }
}
