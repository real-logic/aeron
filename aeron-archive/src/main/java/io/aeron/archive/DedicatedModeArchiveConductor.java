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

package io.aeron.archive;

import io.aeron.Aeron;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

final class DedicatedModeArchiveConductor extends ArchiveConductor
{
    private static final int COMMAND_LIMIT = 10;

    private final ManyToOneConcurrentArrayQueue<Session> closeQueue;
    private AgentRunner replayerAgentRunner;
    private AgentRunner recorderAgentRunner;

    DedicatedModeArchiveConductor(final Aeron aeron, final Archive.Context ctx)
    {
        super(aeron, ctx);

        closeQueue = new ManyToOneConcurrentArrayQueue<>(ctx.maxConcurrentRecordings() + ctx.maxConcurrentReplays());
    }

    public void onStart()
    {
        super.onStart();

        recorderAgentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), recorder);
        replayerAgentRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), replayer);

        AgentRunner.startOnThread(replayerAgentRunner, ctx.threadFactory());
        AgentRunner.startOnThread(recorderAgentRunner, ctx.threadFactory());
    }

    protected SessionWorker<RecordingSession> constructRecorder()
    {
        return new DedicatedModeRecorder(errorHandler, ctx.errorCounter(), closeQueue);
    }

    protected SessionWorker<ReplaySession> constructReplayer()
    {
        return new DedicatedModeReplayer(
            errorHandler,
            ctx.errorCounter(),
            closeQueue,
            new ControlSessionProxy());
    }

    protected int preWork()
    {
        return super.preWork() + processCloseQueue();
    }

    @SuppressWarnings("StatementWithEmptyBody")
    protected void closeSessionWorkers()
    {
        try
        {
            CloseHelper.close(recorderAgentRunner);
        }
        catch (final Exception e)
        {
            errorHandler.onError(e);
        }
        try
        {
            CloseHelper.close(replayerAgentRunner);
        }
        catch (final Exception e)
        {
            errorHandler.onError(e);
        }

        while (processCloseQueue() > 0 || !closeQueue.isEmpty())
        {
            // drain the command queue
        }
    }

    protected void postSessionsClose()
    {
        if (!closeQueue.isEmpty())
        {
            System.err.println("ERR: Close queue not empty");
        }
        super.postSessionsClose();
    }

    private int processCloseQueue()
    {
        int i;
        Session session;
        for (i = 0; i < COMMAND_LIMIT && (session = closeQueue.poll()) != null; i++)
        {
            if (session instanceof RecordingSession)
            {
                closeRecordingSession((RecordingSession) session);
            } else if (session instanceof ReplaySession)
            {
                final ReplaySession replaySession = (ReplaySession) session;
                replaySession.setThreadLocalControlSessionProxy(controlSessionProxy);
                closeReplaySession(replaySession);
            } else
            {
                closeSession(session);
            }
        }

        return i;
    }

    private static class DedicatedModeRecorder extends DedicatedModeSessionWorker<RecordingSession>
    {
        private final ManyToOneConcurrentArrayQueue<Session> closeQueue;

        DedicatedModeRecorder(
            final ErrorHandler errorHandler,
            final AtomicCounter errorCounter,
            final ManyToOneConcurrentArrayQueue<Session> closeQueue)
        {
            super("archive-recorder", errorHandler, errorCounter);
            this.closeQueue = closeQueue;
        }

        protected void closeSession(final RecordingSession session)
        {
            closeQueue.offer(session);
        }
    }

    private static class DedicatedModeReplayer extends DedicatedModeSessionWorker<ReplaySession>
    {
        private final ManyToOneConcurrentArrayQueue<Session> closeQueue;
        private final ControlSessionProxy proxy;

        DedicatedModeReplayer(
            final ErrorHandler errorHandler,
            final AtomicCounter errorCounter,
            final ManyToOneConcurrentArrayQueue<Session> closeQueue,
            final ControlSessionProxy proxy)
        {
            super("archive-replayer", errorHandler, errorCounter);
            this.closeQueue = closeQueue;
            this.proxy = proxy;
        }

        protected void postSessionAdd(final ReplaySession session)
        {
            session.setThreadLocalControlSessionProxy(proxy);
        }

        protected void closeSession(final ReplaySession session)
        {
            closeQueue.offer(session);
        }
    }
}
