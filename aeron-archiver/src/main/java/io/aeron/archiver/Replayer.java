/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.aeron.archiver;

import io.aeron.*;
import io.aeron.archiver.codecs.ControlResponseCode;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.EpochClock;

import java.io.File;

class Replayer extends SessionWorker
{
    private final Aeron aeron;
    private final EpochClock epochClock;
    private final File archiveDir;
    private final ControlSessionProxy controlSessionProxy;
    private final StringBuilder uriBuilder = new StringBuilder(1024);

    private int replaySessionId;

    private final Long2ObjectHashMap<ReplaySession> replaySessionByIdMap = new Long2ObjectHashMap<>();

    Replayer(final Aeron aeron, final Archiver.Context ctx)
    {
        this.aeron = aeron;
        this.epochClock = ctx.epochClock();
        this.archiveDir = ctx.archiveDir();
        controlSessionProxy = new ControlSessionProxy(ctx.idleStrategy());
    }

    public String roleName()
    {
        return "archiver-replayer";
    }

    protected void sessionCleanup(final long sessionId)
    {
        replaySessionByIdMap.remove(sessionId);
    }

    void startReplay(
        final long correlationId,
        final Publication controlPublication,
        final int replayStreamId,
        final String replayChannel,
        final long recordingId,
        final long position,
        final long length)
    {
        final int newId = replaySessionId++;
        final ReplaySession replaySession = new ReplaySession(
            recordingId,
            position,
            length,
            this,
            controlPublication,
            archiveDir,
            controlSessionProxy,
            newId,
            correlationId,
            epochClock,
            replayChannel,
            replayStreamId);

        replaySessionByIdMap.put(newId, replaySession);
        addSession(replaySession);
    }

    void stopReplay(final long correlationId, final Publication controlPublication, final long replayId)
    {
        final ReplaySession session = replaySessionByIdMap.get(replayId);
        if (session != null)
        {
            session.abort();
            controlSessionProxy.sendOkResponse(controlPublication, correlationId);
        }
        else
        {
            controlSessionProxy.sendError(
                controlPublication,
                ControlResponseCode.REPLAY_NOT_FOUND,
                null,
                correlationId);
        }
    }

    ExclusivePublication newReplayPublication(
        final String replayChannel,
        final int replayStreamId,
        final long fromPosition,
        final int mtuLength,
        final int initialTermId,
        final int termBufferLength)
    {
        final int termId = (int)((fromPosition / termBufferLength) + initialTermId);
        final int termOffset = (int)(fromPosition % termBufferLength);
        initUriBuilder(replayChannel);

        uriBuilder
            .append(CommonContext.INITIAL_TERM_ID_PARAM_NAME).append('=').append(initialTermId)
            .append('|')
            .append(CommonContext.MTU_LENGTH_PARAM_NAME).append('=').append(mtuLength)
            .append('|')
            .append(CommonContext.TERM_LENGTH_PARAM_NAME).append('=').append(termBufferLength)
            .append('|')
            .append(CommonContext.TERM_ID_PARAM_NAME).append('=').append(termId)
            .append('|')
            .append(CommonContext.TERM_OFFSET_PARAM_NAME).append('=').append(termOffset);

        return aeron.addExclusivePublication(uriBuilder.toString(), replayStreamId);
    }

    private void initUriBuilder(final String channel)
    {
        uriBuilder.setLength(0);
        uriBuilder.append(channel);

        if (channel.indexOf('?', 0) > -1)
        {
            uriBuilder.append('|');
        }
        else
        {
            uriBuilder.append('?');
        }
    }
}
