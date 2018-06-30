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

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.CommitPos;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;

public class ReplayFromLog implements AutoCloseable
{
    enum State
    {
        INIT,
        REPLAY,
        DONE
    }

    private final long recordingId;
    private final long startPosition;
    private final long stopPosition;
    private final long leadershipTermId;
    private final int logSessionId;
    private final int replayStreamId;
    private final AeronArchive archive;
    private final ConsensusModuleAgent consensusModuleAgent;
    private final String channel;

    private int replaySessionId = Aeron.NULL_VALUE;
    private State state = State.INIT;
    private Counter commitPosition;
    private Subscription logSubscription;
    private LogAdapter logAdapter;

    ReplayFromLog(
        final AeronArchive archive,
        final long recordingId,
        final long startPosition,
        final long stopPosition,
        final long leadershipTermId,
        final int logSessionId,
        final ConsensusModuleAgent consensusModuleAgent,
        final ConsensusModule.Context ctx)
    {
        this.archive = archive;
        this.recordingId = recordingId;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.leadershipTermId = leadershipTermId;
        this.logSessionId = logSessionId;
        this.consensusModuleAgent = consensusModuleAgent;
        this.replayStreamId = ctx.replayStreamId();

        final Aeron aeron = ctx.aeron();
        final MutableDirectBuffer tempBuffer = ctx.tempBuffer();

        final ChannelUri channelUri = ChannelUri.parse(ctx.replayChannel());
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(logSessionId));
        this.channel = channelUri.toString();

        commitPosition = CommitPos.allocate(aeron, tempBuffer, leadershipTermId, startPosition, stopPosition);
        logSubscription = aeron.addSubscription(channel, replayStreamId);
    }

    @SuppressWarnings("unused")
    int doWork(final long nowMs)
    {
        int workCount = 0;

        switch (state)
        {
            case INIT:
                consensusModuleAgent.awaitServicesReadyForReplay(
                    channel, replayStreamId, logSessionId, commitPosition.id(), leadershipTermId, startPosition);

                final long length = stopPosition - startPosition;
                replaySessionId = (int)archive.startReplay(recordingId, startPosition, length, channel, replayStreamId);
                state = State.REPLAY;
                workCount = 1;
                break;

            case REPLAY:
                if (null == logAdapter)
                {
                    final Image image = logSubscription.imageBySessionId(replaySessionId);
                    if (null != image)
                    {
                        logAdapter = new LogAdapter(image, consensusModuleAgent);
                        workCount = 1;
                    }
                }
                else
                {
                    consensusModuleAgent.replayLogPoll(logAdapter, stopPosition, commitPosition);
                    if (logAdapter.position() == stopPosition)
                    {
                        consensusModuleAgent.awaitServicesReplayComplete(stopPosition);

                        commitPosition.close();
                        commitPosition = null;

                        logSubscription.close();
                        logSubscription = null;

                        logAdapter.close();
                        logAdapter = null;

                        state = State.DONE;
                        workCount = 1;
                    }
                }
                break;
        }

        return workCount;
    }

    public void close()
    {
        CloseHelper.close(commitPosition);
        CloseHelper.close(logSubscription);
        CloseHelper.close(logAdapter);
    }

    boolean isDone()
    {
        return State.DONE == state;
    }
}
