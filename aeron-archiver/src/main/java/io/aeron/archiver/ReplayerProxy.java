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
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;

class ReplayerProxy extends Replayer
{
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue = new OneToOneConcurrentArrayQueue<>(256);

    ReplayerProxy(final Aeron aeron, final Archiver.Context ctx)
    {
        super(aeron, ctx);
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
        final Runnable cmd = () -> super.startReplay(
            correlationId,
            controlPublication,
            replayStreamId,
            replayChannel,
            recordingId,
            position,
            length);

        while (!commandQueue.offer(cmd))
        {
            Thread.yield();
        }
    }

    void stopReplay(final long correlationId, final Publication controlPublication, final long replayId)
    {
        final Runnable cmd = () -> super.stopReplay(correlationId, controlPublication, replayId);

        while (!commandQueue.offer(cmd))
        {
            Thread.yield();
        }
    }

    public int doWork()
    {
        final int work = commandQueue.drain(Runnable::run);
        return work + super.doWork();
    }
}
