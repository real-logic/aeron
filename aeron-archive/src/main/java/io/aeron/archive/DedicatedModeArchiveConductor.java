/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.function.Consumer;

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

        recorderAgentRunner = new AgentRunner(ctx.idleStrategy(), errorHandler, ctx.errorCounter(), recorder);
        replayerAgentRunner = new AgentRunner(ctx.idleStrategy(), errorHandler, ctx.errorCounter(), replayer);

        AgentRunner.startOnThread(replayerAgentRunner, ctx.threadFactory());
        AgentRunner.startOnThread(recorderAgentRunner, ctx.threadFactory());
    }

    protected SessionWorker<RecordingSession> newRecorder()
    {
        return new DedicatedModeRecorder(errorHandler, ctx.errorCounter(), closeQueue, ctx.maxConcurrentRecordings());
    }

    protected SessionWorker<ReplaySession> newReplayer()
    {
        return new DedicatedModeReplayer(errorHandler, ctx.errorCounter(), closeQueue, ctx.maxConcurrentReplays());
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
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }

        try
        {
            CloseHelper.close(replayerAgentRunner);
        }
        catch (final Exception ex)
        {
            errorHandler.onError(ex);
        }

        while (processCloseQueue() > 0 || !closeQueue.isEmpty())
        {
            Thread.yield();
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
                closeReplaySession((ReplaySession)session);
            }
            else
            {
                closeSession(session);
            }
        }

        return i;
    }

    static class DedicatedModeRecorder extends SessionWorker<RecordingSession> implements Consumer<RecordingSession>
    {
        private final OneToOneConcurrentArrayQueue<RecordingSession> sessionsQueue;
        private final ManyToOneConcurrentArrayQueue<Session> closeQueue;
        private final AtomicCounter errorCounter;

        DedicatedModeRecorder(
            final ErrorHandler errorHandler,
            final AtomicCounter errorCounter,
            final ManyToOneConcurrentArrayQueue<Session> closeQueue,
            final int maxConcurrentSessions)
        {
            super("archive-recorder", errorHandler);

            this.closeQueue = closeQueue;
            this.errorCounter = errorCounter;
            this.sessionsQueue = new OneToOneConcurrentArrayQueue<>(maxConcurrentSessions);
        }

        public void accept(final RecordingSession session)
        {
            super.addSession(session);
        }

        protected int preWork()
        {
            return sessionsQueue.drain(this);
        }

        protected void preSessionsClose()
        {
            sessionsQueue.drain(this);
        }

        protected void addSession(final RecordingSession session)
        {
            send(session);
        }

        protected void closeSession(final RecordingSession session)
        {
            while (!closeQueue.offer(session))
            {
                errorCounter.increment();
                Thread.yield();
            }
        }

        private void send(final RecordingSession session)
        {
            while (!sessionsQueue.offer(session))
            {
                errorCounter.increment();
                Thread.yield();
            }
        }
    }

    static class DedicatedModeReplayer extends SessionWorker<ReplaySession> implements Consumer<ReplaySession>
    {
        private final OneToOneConcurrentArrayQueue<ReplaySession> sessionsQueue;
        private final ManyToOneConcurrentArrayQueue<Session> closeQueue;
        private final AtomicCounter errorCounter;

        DedicatedModeReplayer(
            final ErrorHandler errorHandler,
            final AtomicCounter errorCounter,
            final ManyToOneConcurrentArrayQueue<Session> closeQueue,
            final int maxConcurrentSessions)
        {
            super("archive-replayer", errorHandler);

            this.closeQueue = closeQueue;
            this.errorCounter = errorCounter;
            this.sessionsQueue = new OneToOneConcurrentArrayQueue<>(maxConcurrentSessions);
        }

        public void accept(final ReplaySession session)
        {
            super.addSession(session);
        }

        protected void addSession(final ReplaySession session)
        {
            send(session);
        }

        protected int preWork()
        {
            return sessionsQueue.drain(this);
        }

        protected void preSessionsClose()
        {
            sessionsQueue.drain(this);
        }

        protected void closeSession(final ReplaySession session)
        {
            while (!closeQueue.offer(session))
            {
                errorCounter.increment();
                Thread.yield();
            }
        }

        private void send(final ReplaySession session)
        {
            while (!sessionsQueue.offer(session))
            {
                errorCounter.increment();
                Thread.yield();
            }
        }
    }
}
