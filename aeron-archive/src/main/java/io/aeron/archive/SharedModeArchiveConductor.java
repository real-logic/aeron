/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.AgentInvoker;

final class SharedModeArchiveConductor extends ArchiveConductor
{
    private AgentInvoker replayerAgentInvoker;
    private AgentInvoker recorderAgentInvoker;

    SharedModeArchiveConductor(final Archive.Context ctx)
    {
        super(ctx);
    }

    public void onStart()
    {
        super.onStart();

        replayerAgentInvoker = new AgentInvoker(errorHandler, ctx.errorCounter(), replayer);
        recorderAgentInvoker = new AgentInvoker(errorHandler, ctx.errorCounter(), recorder);

        replayerAgentInvoker.start();
        recorderAgentInvoker.start();
    }

    protected SessionWorker<RecordingSession> newRecorder()
    {
        return new SharedModeRecorder(errorHandler);
    }

    protected SessionWorker<ReplaySession> newReplayer()
    {
        return new SharedModeReplayer(errorHandler);
    }

    protected int preWork()
    {
        return super.preWork() +
            replayerAgentInvoker.invoke() +
            invokeDriverConductor() +
            recorderAgentInvoker.invoke() +
            invokeDriverConductor();
    }

    protected void closeSessionWorkers()
    {
        CloseHelper.close(recorderAgentInvoker);
        CloseHelper.close(replayerAgentInvoker);
    }

    class SharedModeRecorder extends SessionWorker<RecordingSession>
    {
        SharedModeRecorder(final ErrorHandler errorHandler)
        {
            super("archive-recorder", errorHandler);
        }

        protected void closeSession(final RecordingSession session)
        {
            closeRecordingSession(session);
        }
    }

    class SharedModeReplayer extends SessionWorker<ReplaySession>
    {
        SharedModeReplayer(final ErrorHandler errorHandler)
        {
            super("archive-replayer", errorHandler);
        }

        protected void closeSession(final ReplaySession session)
        {
            closeReplaySession(session);
        }
    }
}
