/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.ControlResponseCode;
import org.agrona.CloseHelper;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_LENGTH;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

class ReplicationSession implements Session, RecordingDescriptorConsumer
{
    private enum State
    {
        CONNECT, FETCH, REPLAY, EXTEND, CLEANUP, DONE
    }

    private long activeCorrelationId = NULL_VALUE;
    private long srcReplaySessionId = NULL_VALUE;
    private final long correlationId;
    private final long replicationId;
    private final long srcRecordingId;
    private long dstRecordingId;
    private final boolean liveMerge;
    private int replayStreamId;
    private final String replayChannel;
    private final ControlSession controlSession;
    private final ControlResponseProxy controlResponseProxy;
    private final Catalog catalog;
    private final Aeron aeron;
    private AeronArchive.AsyncConnect asyncConnect;
    private AeronArchive srcArchive;
    private State state = State.CONNECT;

    ReplicationSession(
        final long correlationId,
        final long srcRecordingId,
        final long dstRecordingId,
        final String replayChannel,
        final int replayStreamId,
        final boolean liveMerge,
        final long replicationId,
        final AeronArchive.Context context,
        final Catalog catalog,
        final ControlResponseProxy controlResponseProxy,
        final ControlSession controlSession)
    {
        this.correlationId = correlationId;
        this.replicationId = replicationId;
        this.srcRecordingId = srcRecordingId;
        this.dstRecordingId = dstRecordingId;
        this.replayChannel = replayChannel;
        this.replayStreamId = replayStreamId;
        this.liveMerge = liveMerge;
        this.aeron = context.aeron();
        this.asyncConnect = AeronArchive.asyncConnect(context);
        this.catalog = catalog;
        this.controlResponseProxy = controlResponseProxy;
        this.controlSession = controlSession;
    }

    public long sessionId()
    {
        return replicationId;
    }

    public boolean isDone()
    {
        return state == State.DONE;
    }

    public void abort()
    {
        this.state(State.CLEANUP);
    }

    public void close()
    {
        controlSession.archiveConductor().closeReplicationSession(this);
        CloseHelper.close(asyncConnect);
        CloseHelper.close(srcArchive);
    }

    public int doWork()
    {
        int workCount = 0;

        try
        {
            switch (state)
            {
                case CONNECT:
                    workCount += connect();
                    break;

                case FETCH:
                    workCount += fetch();
                    break;

                case REPLAY:
                    workCount += replay();
                    break;

                case EXTEND:
                    workCount += extend();
                    break;

                case CLEANUP:
                    workCount += cleanup();
                    break;
            }
        }
        catch (final Throwable ex)
        {
            state(State.DONE);
            error(ex);
            throw ex;
        }

        return workCount;
    }

    public void onRecordingDescriptor(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long startTimestamp,
        final long stopTimestamp,
        final long startPosition,
        final long stopPosition,
        final int initialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel,
        final String sourceIdentity)
    {
        if (srcRecordingId != recordingId)
        {
            state(State.CLEANUP);
            throw new IllegalStateException("invalid recording id " + recordingId + " expected " + srcRecordingId);
        }

        dstRecordingId = catalog.addNewRecording(
            startPosition,
            startTimestamp,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity);

        replayStreamId = streamId;
        activeCorrelationId = NULL_VALUE;
        state(State.REPLAY);
    }

    private int connect()
    {
        int workCount = 0;
        final int step = asyncConnect.step();
        final AeronArchive archive = asyncConnect.poll();

        if (null == archive)
        {
            if (asyncConnect.step() != step)
            {
                workCount += 1;
            }
        }
        else
        {
            srcArchive = archive;
            asyncConnect = null;
            state(NULL_VALUE == dstRecordingId ? State.FETCH : State.REPLAY);

            workCount += 1;
        }

        return workCount;
    }

    private int fetch()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            if (srcArchive.archiveProxy().listRecording(srcRecordingId, correlationId, srcArchive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                srcArchive.recordingDescriptorPoller().reset(correlationId, 1, this);
                workCount += 1;
            }
        }
        else
        {
            workCount += srcArchive.recordingDescriptorPoller().poll();
        }

        return workCount;
    }

    private int replay()
    {
        int workCount = 0;

        if (NULL_VALUE == activeCorrelationId)
        {
            final long correlationId = aeron.nextCorrelationId();
            if (srcArchive.archiveProxy().replay(
                srcRecordingId,
                NULL_POSITION,
                NULL_LENGTH,
                replayChannel,
                replayStreamId,
                correlationId,
                srcArchive.controlSessionId()))
            {
                activeCorrelationId = correlationId;
                workCount += 1;
            }
        }
        else
        {
            final ControlResponsePoller poller = srcArchive.controlResponsePoller();
            workCount += poller.poll();

            if (poller.isPollComplete() && poller.controlSessionId() == srcArchive.controlSessionId())
            {
                final ControlResponseCode code = poller.code();
                if (code == ControlResponseCode.ERROR)
                {
                    throw new ArchiveException(poller.errorMessage(), code.value());
                }

                if (poller.correlationId() == activeCorrelationId && code == ControlResponseCode.OK)
                {
                    srcReplaySessionId = poller.relevantId();
                    state(State.EXTEND);
                }

                workCount += 1;
            }
        }

        return workCount;
    }

    private int extend()
    {
        state(State.DONE);
        return 1;
    }

    private int cleanup()
    {
        state(State.DONE);
        return 1;
    }

    private void error(final Throwable ex)
    {
        if (!controlSession.controlPublication().isConnected())
        {
            controlSession.sendErrorResponse(correlationId, ex.getMessage(), controlResponseProxy);
        }
    }

    private void state(final State newState)
    {
        //System.out.println(state + " -> " + newState);
        state = newState;
    }
}
