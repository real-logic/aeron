/*
 * Copyright 2014-2021 Real Logic Limited.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archive;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;

import java.util.concurrent.CountDownLatch;

final class DedicatedModeArchiveConductor extends ArchiveConductor
{
    private static final int COMMAND_LIMIT = 10;

    private final ManyToOneConcurrentLinkedQueue<Session> closeQueue;
    private AgentRunner replayerAgentRunner;
    private AgentRunner recorderAgentRunner;

    DedicatedModeArchiveConductor(final Archive.Context ctx)
    {
        super(ctx);
        closeQueue = new ManyToOneConcurrentLinkedQueue<>();
    }

    public void onStart()
    {
        super.onStart();

        recorderAgentRunner = new AgentRunner(ctx.recorderIdleStrategy(), errorHandler, ctx.errorCounter(), recorder);
        replayerAgentRunner = new AgentRunner(ctx.replayerIdleStrategy(), errorHandler, ctx.errorCounter(), replayer);

        AgentRunner.startOnThread(replayerAgentRunner, ctx.threadFactory());
        AgentRunner.startOnThread(recorderAgentRunner, ctx.threadFactory());
    }

    protected SessionWorker<RecordingSession> newRecorder()
    {
        return new DedicatedModeRecorder(errorHandler, ctx.errorCounter(), closeQueue, ctx.abortLatch());
    }

    protected SessionWorker<ReplaySession> newReplayer()
    {
        return new DedicatedModeReplayer(errorHandler, ctx.errorCounter(), closeQueue, ctx.abortLatch());
    }

    public int doWork()
    {
        return processCloseQueue() + super.doWork();
    }

    protected void closeSessionWorkers()
    {
        CloseHelper.close(errorHandler, recorderAgentRunner);
        CloseHelper.close(errorHandler, replayerAgentRunner);

        while (processCloseQueue() > 0 || !closeQueue.isEmpty())
        {
            Thread.yield();
            if (Thread.currentThread().isInterrupted())
            {
                break;
            }
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

    static class DedicatedModeRecorder extends SessionWorker<RecordingSession>
    {
        private final ManyToOneConcurrentLinkedQueue<RecordingSession> sessionsQueue;
        private final ManyToOneConcurrentLinkedQueue<Session> closeQueue;
        private final AtomicCounter errorCounter;
        private final CountDownLatch abortLatch;
        private volatile boolean isAbort;

        DedicatedModeRecorder(
            final ErrorHandler errorHandler,
            final AtomicCounter errorCounter,
            final ManyToOneConcurrentLinkedQueue<Session> closeQueue,
            final CountDownLatch abortLatch)
        {
            super("archive-recorder", errorHandler);

            this.closeQueue = closeQueue;
            this.errorCounter = errorCounter;
            this.sessionsQueue = new ManyToOneConcurrentLinkedQueue<>();
            this.abortLatch = abortLatch;
        }

        protected void abort()
        {
            isAbort = true;
        }

        public int doWork()
        {
            if (isAbort)
            {
                throw new AgentTerminationException();
            }

            return drainSessionsQueue() + super.doWork();
        }

        protected void preSessionsClose()
        {
            drainSessionsQueue();
        }

        private int drainSessionsQueue()
        {
            int workCount = 0;
            RecordingSession session;

            while (null != (session = sessionsQueue.poll()))
            {
                workCount += 1;
                super.addSession(session);
            }

            return workCount;
        }

        protected void addSession(final RecordingSession session)
        {
            send(session);
        }

        protected void closeSession(final RecordingSession session)
        {
            while (!closeQueue.offer(session))
            {
                if (!errorCounter.isClosed())
                {
                    errorCounter.increment();
                }

                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    break;
                }
            }
        }

        protected void postSessionsClose()
        {
            if (isAbort)
            {
                abortLatch.countDown();
            }
        }

        private void send(final RecordingSession session)
        {
            while (!sessionsQueue.offer(session))
            {
                if (!errorCounter.isClosed())
                {
                    errorCounter.increment();
                }

                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    break;
                }
            }
        }
    }

    static class DedicatedModeReplayer extends SessionWorker<ReplaySession>
    {
        private final ManyToOneConcurrentLinkedQueue<ReplaySession> sessionsQueue;
        private final ManyToOneConcurrentLinkedQueue<Session> closeQueue;
        private final AtomicCounter errorCounter;
        private final CountDownLatch abortLatch;
        private volatile boolean isAbort;

        DedicatedModeReplayer(
            final ErrorHandler errorHandler,
            final AtomicCounter errorCounter,
            final ManyToOneConcurrentLinkedQueue<Session> closeQueue,
            final CountDownLatch abortLatch)
        {
            super("archive-replayer", errorHandler);

            this.closeQueue = closeQueue;
            this.errorCounter = errorCounter;
            this.sessionsQueue = new ManyToOneConcurrentLinkedQueue<>();
            this.abortLatch = abortLatch;
        }

        protected void abort()
        {
            isAbort = true;
        }

        protected void addSession(final ReplaySession session)
        {
            send(session);
        }

        public int doWork()
        {
            if (isAbort)
            {
                throw new AgentTerminationException();
            }

            return drainSessionQueue() + super.doWork();
        }

        protected void preSessionsClose()
        {
            drainSessionQueue();
        }

        private int drainSessionQueue()
        {
            int workCount = 0;
            ReplaySession session;

            while (null != (session = sessionsQueue.poll()))
            {
                workCount += 1;
                super.addSession(session);
            }

            return workCount;
        }

        protected void closeSession(final ReplaySession session)
        {
            while (!closeQueue.offer(session))
            {
                if (!errorCounter.isClosed())
                {
                    errorCounter.increment();
                }

                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    break;
                }
            }
        }

        protected void postSessionsClose()
        {
            if (isAbort)
            {
                abortLatch.countDown();
            }
        }

        private void send(final ReplaySession session)
        {
            while (!sessionsQueue.offer(session))
            {
                if (!errorCounter.isClosed())
                {
                    errorCounter.increment();
                }

                Thread.yield();
                if (Thread.currentThread().isInterrupted())
                {
                    break;
                }
            }
        }
    }
}
