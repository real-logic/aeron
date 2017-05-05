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
import org.agrona.*;

class ArchiveClientSession implements ArchiveConductor.Session, ArchiveRequestListener
{
    enum State
    {
        INIT, WORKING, CLOSE, DONE
    }

    private final Image image;
    private final ClientProxy clientProxy;
    private final ArchiveConductor conductor;
    private final ArchiveRequestAdapter adapter = new ArchiveRequestAdapter(this);
    private ExclusivePublication reply;
    private State state = State.INIT;

    ArchiveClientSession(final Image image, final ClientProxy clientProxy, final ArchiveConductor conductor)
    {
        this.image = image;
        this.clientProxy = clientProxy;
        this.conductor = conductor;
    }

    public void abort()
    {
        state = State.CLOSE;
    }

    public boolean isDone()
    {
        return state == State.DONE;
    }

    public void remove(final ArchiveConductor conductor)
    {
    }

    public int doWork()
    {
        switch (state)
        {
            case INIT:
                return waitForConnection();

            case WORKING:
                if (image.isClosed() || !reply.isConnected())
                {
                    state = State.CLOSE;
                }
                else
                {
                    return image.poll(adapter, 16);
                }
                break;

            case CLOSE:
                CloseHelper.quietClose(reply);
                state = State.DONE;
                break;

            case DONE:
                break;

            default:
                throw new IllegalStateException();
        }

        return 0;
    }

    private int waitForConnection()
    {
        if (reply == null)
        {
            try
            {
                image.poll(adapter, 1);
            }
            catch (final Exception e)
            {
                state = State.CLOSE;
                LangUtil.rethrowUnchecked(e);
            }
        }
        else if (reply.isConnected())
        {
            state = State.WORKING;
        }

        return 0;
    }

    public void onConnect(
        final String channel,
        final int streamId)
    {
        if (state != State.INIT)
        {
            throw new IllegalStateException();
        }

        reply = conductor.clientConnect(channel, streamId);
    }

    public void onStopRecording(
        final int correlationId,
        final String channel,
        final int streamId)
    {
        if (state != State.WORKING)
        {
            throw new IllegalStateException();
        }

        try
        {
            conductor.stopRecording(channel, streamId);
            //clientProxy.sendResponse(reply, null, correlationId);
        }
        catch (final Exception e)
        {
            //clientProxy.sendResponse(reply, e.getMessage(), correlationId);
        }
    }

    public void onStartRecording(
        final int correlationId,
        final String channel,
        final int streamId)
    {
        if (state != State.WORKING)
        {
            throw new IllegalStateException();
        }

        try
        {
            conductor.startRecording(channel, streamId);
            //clientProxy.sendResponse(reply, null, correlationId);
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            //clientProxy.sendResponse(reply, e.getMessage(), correlationId);
        }
    }

    public void onListRecordings(
        final int correlationId,
        final int fromId,
        final int toId)
    {
        if (state != State.WORKING)
        {
            throw new IllegalStateException();
        }

        conductor.listRecordings(correlationId, reply, fromId, toId);
    }

    public void onAbortReplay(final int correlationId)
    {
        if (state != State.WORKING)
        {
            throw new IllegalStateException();
        }

        conductor.stopReplay(0);
    }

    public void onStartReplay(
        final int correlationId,
        final int replayStreamId,
        final String replayChannel,
        final int recordingId,
        final int termId,
        final int termOffset,
        final long length)
    {
        if (state != State.WORKING)
        {
            throw new IllegalStateException();
        }

        conductor.startReplay(
            correlationId,
            reply,
            replayStreamId,
            replayChannel,
            recordingId,
            termId,
            termOffset,
            length);
    }
}
