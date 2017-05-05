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

class ArchiverClientSession implements ArchiveConductor.Session, ArchiverProtocolListener
{
    enum State
    {
        INIT, WORKING, CLOSE, DONE
    }

    private final Image image;
    private final ArchiverProtocolProxy proxy;
    private final ArchiveConductor conductor;
    private final ArchiverProtocolAdapter adapter = new ArchiverProtocolAdapter(this);
    private ExclusivePublication reply;
    private State state = State.INIT;

    ArchiverClientSession(final Image image, final ArchiverProtocolProxy proxy, final ArchiveConductor conductor)
    {
        this.image = image;
        this.proxy = proxy;
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

    @Override
    public void remove(final ArchiveConductor conductor)
    {
    }

    @Override
    public int doWork()
    {
        switch (state)
        {
            case INIT:
                return waitForInitMessage();
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

    private int waitForInitMessage()
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

    public void onClientInit(
        final String channel,
        final int streamId)
    {
        if (state != State.INIT)
        {
            throw new IllegalStateException();
        }
        reply = conductor.clientInit(channel, streamId);
    }

    public void onArchiveStop(
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
            conductor.stopArchive(channel, streamId);
            //proxy.sendResponse(reply, null, correlationId);
        }
        catch (final Exception e)
        {
            //proxy.sendResponse(reply, e.getMessage(), correlationId);
        }
    }

    public void onArchiveStart(
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
            conductor.startArchive(channel, streamId);
            //proxy.sendResponse(reply, null, correlationId);
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            //proxy.sendResponse(reply, e.getMessage(), correlationId);
        }

    }

    public void onListStreamInstances(
        final int correlationId,
        final int from,
        final int to)
    {
        if (state != State.WORKING)
        {
            throw new IllegalStateException();
        }
        conductor.listStreamInstances(correlationId, reply, from, to);
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
        final int streamInstanceId,
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
            streamInstanceId,
            termId,
            termOffset,
            length);
    }

}
