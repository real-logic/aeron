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
import org.agrona.concurrent.AgentInvoker;

class SharedModeArchiveConductor extends ArchiveConductor
{
    private final AgentInvoker replayerAgentInvoker;
    private final AgentInvoker recorderAgentInvoker;

    SharedModeArchiveConductor(final Aeron aeron, final Archiver.Context ctx)
    {
        super(aeron, ctx);

        replayerAgentInvoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), replayer);
        recorderAgentInvoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), recorder);
    }

    public void onStart()
    {
        super.onStart();

        replayerAgentInvoker.start();
        recorderAgentInvoker.start();
    }

    protected SessionWorker<RecordingSession> constructRecorder(final Archiver.Context ctx)
    {
        return new SessionWorker<RecordingSession>("recorder")
        {
            void closeSession(final RecordingSession session)
            {
                closeRecordingSession(session);
            }
        };
    }

    protected SessionWorker<ReplaySession> constructReplayer(final Archiver.Context ctx)
    {
        return new SessionWorker<ReplaySession>("replayer")
        {
            void closeSession(final ReplaySession session)
            {
                closeReplaySession(session);
            }
        };
    }

    protected int preSessionWork()
    {
        return replayerAgentInvoker.invoke() + recorderAgentInvoker.invoke();
    }

    protected void closeSessionWorkers()
    {
        CloseHelper.quietClose(recorderAgentInvoker);
        CloseHelper.quietClose(replayerAgentInvoker);
    }
}
