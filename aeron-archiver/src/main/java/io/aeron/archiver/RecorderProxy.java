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

class RecorderProxy extends Recorder
{
    private final OneToOneConcurrentArrayQueue<Runnable> commandQueue = new OneToOneConcurrentArrayQueue<>(256);

    RecorderProxy(final Aeron aeron, final Archiver.Context ctx)
    {
        super(aeron, ctx);
    }


    void startRecording(final Image image)
    {
        final Runnable cmd = () -> super.startRecording(image);

        while (!commandQueue.offer(cmd))
        {
            Thread.yield();
        }
    }

    void stopRecording(final long correlationId, final Publication controlPublication, final long recordingId)
    {
        final Runnable cmd = () -> super.stopRecording(correlationId, controlPublication, recordingId);

        while (!commandQueue.offer(cmd))
        {
            Thread.yield();
        }
    }

    public int doWork()
    {
        final int work = commandQueue.drain(r -> r.run());
        return work + super.doWork();
    }
}
