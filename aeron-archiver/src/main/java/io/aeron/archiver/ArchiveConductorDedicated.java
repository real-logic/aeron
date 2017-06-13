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

import io.aeron.Aeron;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import java.util.concurrent.ThreadFactory;

class ArchiveConductorDedicated extends ArchiveConductor
{
    // Needs an unbounded queue to prevent a deadlock, JCTools MpscUnboundedArrayQueue is a better choice
    private final ManyToOneConcurrentLinkedQueue<Runnable> commandQueue = new ManyToOneConcurrentLinkedQueue<>();
    private final AgentRunner replayerAgentRunner;
    private final AgentRunner recorderAgentRunner;
    private final ThreadFactory threadFactory;

    ArchiveConductorDedicated(final Aeron aeron, final Archiver.Context ctx)
    {
        super(aeron, ctx);
        recorderAgentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), recorder);
        replayerAgentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), replayer);
        threadFactory = ctx.threadFactory();
    }

    protected SessionWorker<RecordingSession> constructRecorder(final Archiver.Context ctx)
    {
        return new SessionWorkerDedicated<RecordingSession>("recorder")
        {
            void closeSession(final RecordingSession session)
            {
                if (isClosed())
                {
                    closeRecordingSession(session);
                }
                else
                {
                    commandQueue.offer(() -> closeRecordingSession(session));
                }
            }
        };
    }

    protected SessionWorker<ReplaySession> constructReplayer(final Archiver.Context ctx)
    {
        return new SessionWorkerDedicated<ReplaySession>("replayer")
        {
            void closeSession(final ReplaySession session)
            {
                if (isClosed())
                {
                    closeReplaySession(session);
                }
                else
                {
                    commandQueue.offer(() -> closeReplaySession(session));
                }
            }
        };
    }

    protected int preSessionWork()
    {
        return drainCommandQueue();
    }

    protected void onStart()
    {
        AgentRunner.startOnThread(replayerAgentRunner, threadFactory);
        AgentRunner.startOnThread(recorderAgentRunner, threadFactory);
    }

    protected void closeSessionWorkers()
    {
        CloseHelper.quietClose(recorderAgentRunner);
        CloseHelper.quietClose(replayerAgentRunner);
        drainCommandQueue();
    }

    private int drainCommandQueue()
    {
        int i = 0;
        Runnable r;
        while ((r = commandQueue.poll()) != null)
        {
            r.run();
            i++;
        }
        return i;
    }
}
