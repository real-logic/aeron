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
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.util.concurrent.ThreadFactory;

class DedicatedModeArchiveConductor extends ArchiveConductor
{
    private static final int COMMAND_LIMIT = 10;

    private final ManyToOneConcurrentArrayQueue<Session> closeQueue;
    private final AgentRunner replayerAgentRunner;
    private final AgentRunner recorderAgentRunner;
    private final ThreadFactory threadFactory;

    DedicatedModeArchiveConductor(final Aeron aeron, final Archiver.Context ctx)
    {
        super(aeron, ctx);

        closeQueue = new ManyToOneConcurrentArrayQueue<>(ctx.maxConcurrentRecordings() + ctx.maxConcurrentReplays());
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
                closeQueue.offer(session);
            }
        };
    }

    protected SessionWorker<ReplaySession> constructReplayer(final Archiver.Context ctx)
    {
        return new DedicatedModeSessionWorker<ReplaySession>("replayer")
        {
            ControlSessionProxy proxy = new ControlSessionProxy(ctx.idleStrategy());

            void postSessionAdd(final ReplaySession session)
            {
                session.setThreadLocalControlSessionProxy(proxy);
            }

            void closeSession(final ReplaySession session)
            {
                closeQueue.offer(session);
            }
        };
    }

    protected int preSessionWork()
    {
        return processCloseQueue();
    }

    public void onStart()
    {
        super.onStart();

        AgentRunner.startOnThread(replayerAgentRunner, threadFactory);
        AgentRunner.startOnThread(recorderAgentRunner, threadFactory);
    }

    protected void closeSessionWorkers()
    {
        CloseHelper.quietClose(recorderAgentRunner);
        CloseHelper.quietClose(replayerAgentRunner);

        while (processCloseQueue() > 0)
        {
            // drain the command queue
        }
    }

    private int processCloseQueue()
    {
        int i;
        Session session;
        for (i = 0; i < COMMAND_LIMIT && (session = closeQueue.poll()) != null; i++)
        {
            if (session instanceof RecordingSession)
            {
                closeRecordingSession((RecordingSession)session);
            }
            else if (session instanceof ReplaySession)
            {
                final ReplaySession replaySession = (ReplaySession) session;
                replaySession.setThreadLocalControlSessionProxy(controlSessionProxy);
                closeReplaySession(replaySession);
            }
            else
            {
                closeSession(session);
            }
        }

        return i;
    }
}
