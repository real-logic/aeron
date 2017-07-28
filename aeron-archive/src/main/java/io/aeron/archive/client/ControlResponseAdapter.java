/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.archive.client;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Encapsulate the polling, decoding, and dispatching of archive control protocol response messages.
 */
public class ControlResponseAdapter
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final ReplayStartedDecoder replayStartedDecoder = new ReplayStartedDecoder();
    private final ReplayAbortedDecoder replayAbortedDecoder = new ReplayAbortedDecoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final RecordingUnknownResponseDecoder recordingUnknownResponseDecoder =
        new RecordingUnknownResponseDecoder();

    private final int fragmentLimit;
    private final ControlResponseListener listener;
    private final Subscription subscription;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onFragment);

    /**
     * Create an adapter for a given subscription to an archive for control response messages.
     *
     * @param listener      to which responses are dispatched.
     * @param subscription  to poll for new events.
     * @param fragmentLimit to apply for each polling operation.
     */
    public ControlResponseAdapter(
        final ControlResponseListener listener,
        final Subscription subscription,
        final int fragmentLimit)
    {
        this.fragmentLimit = fragmentLimit;
        this.listener = listener;
        this.subscription = subscription;
    }

    /**
     * Poll for recording events and dispatch them to the {@link RecordingEventsListener} for this instance.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    public int poll()
    {
        return subscription.poll(fragmentAssembler, fragmentLimit);
    }

    private void onFragment(
        final DirectBuffer buffer,
        final int offset,
        @SuppressWarnings("unused") final int length,
        @SuppressWarnings("unused") final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case ControlResponseDecoder.TEMPLATE_ID:
                handleControlResponse(listener, buffer, offset);
                break;

            case RecordingDescriptorDecoder.TEMPLATE_ID:
                handleRecordingDescriptor(listener, buffer, offset);
                break;

            case ReplayStartedDecoder.TEMPLATE_ID:
                handleReplayStarted(listener, buffer, offset);
                break;

            case ReplayAbortedDecoder.TEMPLATE_ID:
                handleReplayAborted(listener, buffer, offset);
                break;

            case RecordingUnknownResponseDecoder.TEMPLATE_ID:
                handleRecordingUnknownResponse(listener, buffer, offset);
                break;

            default:
                throw new IllegalStateException("Unknown templateId: " + templateId);
        }
    }

    private void handleControlResponse(
        final ControlResponseListener listener,
        final DirectBuffer buffer,
        final int offset)
    {
        controlResponseDecoder.wrap(
            buffer,
            offset + MessageHeaderEncoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        listener.onResponse(
            controlResponseDecoder.correlationId(),
            controlResponseDecoder.code(),
            controlResponseDecoder.errorMessage());
    }

    private void handleRecordingDescriptor(
        final ControlResponseListener listener,
        final DirectBuffer buffer,
        final int offset)
    {
        recordingDescriptorDecoder.wrap(
            buffer,
            offset + MessageHeaderEncoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        listener.onRecordingDescriptor(
            recordingDescriptorDecoder.correlationId(),
            recordingDescriptorDecoder.recordingId(),
            recordingDescriptorDecoder.startTimestamp(),
            recordingDescriptorDecoder.stopTimestamp(),
            recordingDescriptorDecoder.startPosition(),
            recordingDescriptorDecoder.stopPosition(),
            recordingDescriptorDecoder.initialTermId(),
            recordingDescriptorDecoder.segmentFileLength(),
            recordingDescriptorDecoder.termBufferLength(),
            recordingDescriptorDecoder.mtuLength(),
            recordingDescriptorDecoder.sessionId(),
            recordingDescriptorDecoder.streamId(),
            recordingDescriptorDecoder.strippedChannel(),
            recordingDescriptorDecoder.originalChannel(),
            recordingDescriptorDecoder.sourceIdentity());
    }

    private void handleReplayStarted(
        final ControlResponseListener listener,
        final DirectBuffer buffer,
        final int offset)
    {
        replayStartedDecoder.wrap(
            buffer,
            offset + MessageHeaderEncoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        listener.onReplayStarted(
            replayStartedDecoder.correlationId(),
            replayStartedDecoder.replayId());
    }

    private void handleReplayAborted(
        final ControlResponseListener listener,
        final DirectBuffer buffer,
        final int offset)
    {
        replayAbortedDecoder.wrap(
            buffer,
            offset + MessageHeaderEncoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        listener.onReplayAborted(
            replayAbortedDecoder.correlationId(),
            replayAbortedDecoder.stopPosition());
    }

    private void handleRecordingUnknownResponse(
        final ControlResponseListener controlResponseListener,
        final DirectBuffer buffer,
        final int offset)
    {
        recordingUnknownResponseDecoder.wrap(
            buffer,
            offset + MessageHeaderEncoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        controlResponseListener.onUnknownRecording(
            recordingUnknownResponseDecoder.correlationId(),
            recordingUnknownResponseDecoder.recordingId(),
            recordingUnknownResponseDecoder.maxRecordingId());
    }
}
