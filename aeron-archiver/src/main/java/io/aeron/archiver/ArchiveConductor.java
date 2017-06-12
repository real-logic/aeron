/*
 * Copyright 2014-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archiver;

import io.aeron.*;
import io.aeron.archiver.codecs.ControlResponseCode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.*;

import java.util.Objects;

class ArchiveConductor extends SessionWorker
{
    /**
     * Low term length for control channel reflect expected low bandwidth usage.
     */
    private static final String DEFAULT_CONTROL_CHANNEL_TERM_LENGTH_PARAM =
        CommonContext.TERM_LENGTH_PARAM_NAME + "=" + Integer.toString(64 * 1024);

    private final Aeron aeron;
    private final AgentInvoker aeronClientAgentInvoker;
    private final AgentInvoker driverAgentInvoker;

    private final AgentInvoker replayerAgentInvoker;
    private final AgentInvoker recorderAgentInvoker;

    private final Replayer replayer;
    private final Recorder recorder;

    private final Subscription controlSubscription;

    private final AvailableImageHandler availableImageHandler = this::onAvailableImage;

    private final ControlSessionProxy controlSessionProxy;
    private final StringBuilder uriBuilder = new StringBuilder(1024);
    private final EpochClock epochClock;

    ArchiveConductor(final Aeron aeron, final Archiver.Context ctx)
    {
        this.aeron = aeron;
        aeronClientAgentInvoker = aeron.conductorAgentInvoker();
        Objects.requireNonNull(aeronClientAgentInvoker, "In the archiver context an aeron invoker should be present");

        epochClock = ctx.epochClock();
        this.driverAgentInvoker = ctx.mediaDriverAgentInvoker();

        controlSessionProxy = new ControlSessionProxy(ctx.idleStrategy());

        controlSubscription = aeron.addSubscription(
            ctx.controlChannel(),
            ctx.controlStreamId(),
            availableImageHandler,
            null);

        replayer = ctx.replayer();
        replayerAgentInvoker = ctx.replayerInvoker();

        recorder = ctx.recorder();
        recorderAgentInvoker = ctx.recorderInvoker();
    }

    public String roleName()
    {
        return "archiver-conductor";
    }

    protected void postSessionsClose()
    {
        CloseHelper.close(recorderAgentInvoker);
        CloseHelper.close(replayerAgentInvoker);
        CloseHelper.close(aeronClientAgentInvoker);
        CloseHelper.close(driverAgentInvoker);
    }

    public int doWork()
    {
        int workDone = safeInvoke(driverAgentInvoker);
        workDone += aeronClientAgentInvoker.invoke();

        workDone += super.doWork();
        workDone += safeInvoke(replayerAgentInvoker);
        workDone += safeInvoke(recorderAgentInvoker);

        return workDone;
    }

    protected void sessionCleanup(final long sessionId)
    {
    }

    private static int safeInvoke(final AgentInvoker invoker)
    {
        if (null != invoker)
        {
            return invoker.invoke();
        }

        return 0;
    }

    /**
     * Note: this is only a thread safe interaction because we are running the aeron client as an invoked agent so the
     * available image notifications are run from this agent thread.
     */
    private void onAvailableImage(final Image image)
    {
        if (image.subscription() == controlSubscription)
        {
            addSession(new ControlSession(image, controlSessionProxy, this, epochClock));
        }
        else
        {
            startRecording(image);
        }
    }

    void stopRecording(
        final long correlationId,
        final Publication controlPublication,
        final long recordingId)
    {
        recorder.stopRecording(correlationId, controlPublication, recordingId);
    }

    void setupRecording(
        final long correlationId,
        final Publication controlPublication,
        final String channel,
        final int streamId)
    {
        try
        {
            // Subscription is closed on RecordingSession close(this is consistent with local archiver usage)
            aeron.addSubscription(channel, streamId, availableImageHandler, null);
            controlSessionProxy.sendOkResponse(correlationId, controlPublication);
        }
        catch (final Exception ex)
        {
            controlSessionProxy.sendError(
                correlationId, ControlResponseCode.ERROR, ex.getMessage(), controlPublication);
        }
    }

    private void startRecording(final Image image)
    {
        recorder.startRecording(image);
    }

    void listRecordings(
        final long correlationId,
        final Publication controlPublication,
        final long fromId,
        final int count)
    {
        recorder.listRecordings(correlationId, controlPublication, fromId, count);
    }

    void stopReplay(final long correlationId, final Publication controlPublication, final long replayId)
    {
        replayer.stopReplay(correlationId, controlPublication, replayId);
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
        replayer.startReplay(
            correlationId,
            controlPublication,
            replayStreamId,
            replayChannel,
            recordingId,
            position,
            length);
    }

    Publication newControlPublication(final String channel, final int streamId)
    {
        final String controlChannel;
        if (!channel.contains(CommonContext.TERM_LENGTH_PARAM_NAME))
        {
            initUriBuilder(channel);
            uriBuilder.append(DEFAULT_CONTROL_CHANNEL_TERM_LENGTH_PARAM);
            controlChannel = uriBuilder.toString();
        }
        else
        {
            controlChannel = channel;
        }

        return aeron.addPublication(controlChannel, streamId);
    }

    private void initUriBuilder(final String channel)
    {
        uriBuilder.setLength(0);
        uriBuilder.append(channel);

        if (channel.indexOf('?', 0) > -1)
        {
            uriBuilder.append('|');
        }
        else
        {
            uriBuilder.append('?');
        }
    }
}
