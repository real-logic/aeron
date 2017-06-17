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

class DedicatedModeArchiveConductor extends ArchiveConductor
{
    public static final int COMMAND_LIMIT = 10;

    private final ManyToOneConcurrentLinkedQueue<Runnable> commandQueue = new ManyToOneConcurrentLinkedQueue<>();
    private final AgentRunner replayerAgentRunner;
    private final AgentRunner recorderAgentRunner;
    private final ThreadFactory threadFactory;

    DedicatedModeArchiveConductor(final Aeron aeron, final Archiver.Context ctx)
    {
        super(aeron, ctx);

        recorderAgentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), recorder);
        replayerAgentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), replayer);
        threadFactory = ctx.threadFactory();
    }

    protected SessionWorker<RecordingSession> constructRecorder(final Archiver.Context ctx)
    {
        return new DedicatedModeSessionWorker<RecordingSession>("recorder")
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
        return new DedicatedModeSessionWorker<ReplaySession>("replayer")
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
        return processCommandQueue();
    }

    public void onStart()
    {
        AgentRunner.startOnThread(replayerAgentRunner, threadFactory);
        AgentRunner.startOnThread(recorderAgentRunner, threadFactory);
    }

    protected void closeSessionWorkers()
    {
        CloseHelper.quietClose(recorderAgentRunner);
        CloseHelper.quietClose(replayerAgentRunner);

        while (processCommandQueue() > 0)
        {
            // drain the command queue
        }
    }

    private int processCommandQueue()
    {
        int i = 0;
        Runnable r;
        while ((r = commandQueue.poll()) != null)
        {
            r.run();

            if (++i >= COMMAND_LIMIT)
            {
                break;
            }
        }

        return i;
    }
}
