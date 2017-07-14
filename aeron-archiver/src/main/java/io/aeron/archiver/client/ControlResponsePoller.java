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
package io.aeron.archiver.client;

import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archiver.codecs.*;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Encapsulate the polling, decoding, and dispatching of archive control protocol response messages.
 */
public class ControlResponsePoller
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ReplayAbortedDecoder replayAbortedDecoder = new ReplayAbortedDecoder();
    private final ReplayStartedDecoder replayStartedDecoder = new ReplayStartedDecoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final ControlResponseDecoder archiverResponseDecoder = new ControlResponseDecoder();
    private final RecordingNotFoundResponseDecoder recordingNotFoundResponseDecoder =
        new RecordingNotFoundResponseDecoder();

    private final int fragmentLimit;
    private final ControlResponseListener listener;
    private final Subscription subscription;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onFragment);

    /**
     * Create a poller for a given subscription to an archive for control response messages.
     *
     * @param listener      to which responses are dispatched.
     * @param subscription  to poll for new events.
     * @param fragmentLimit to apply for each polling operation.
     */
    public ControlResponsePoller(
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
                handleArchiverResponse(listener, buffer, offset);
                break;

            case ReplayAbortedDecoder.TEMPLATE_ID:
                handleReplayAborted(listener, buffer, offset);
                break;

            case ReplayStartedDecoder.TEMPLATE_ID:
                handleReplayStarted(listener, buffer, offset);
                break;

            case RecordingDescriptorDecoder.TEMPLATE_ID:
                handleRecordingDescriptor(listener, buffer, offset);
                break;

            case RecordingNotFoundResponseDecoder.TEMPLATE_ID:
                handleRecordingNotFoundResponse(listener, buffer, offset);
                break;

            default:
                throw new IllegalStateException("Unknown templateId: " + templateId);
        }
    }

    private void handleRecordingNotFoundResponse(
        final ControlResponseListener controlResponseListener,
        final DirectBuffer buffer,
        final int offset)
    {
        recordingNotFoundResponseDecoder.wrap(
            buffer,
            offset + MessageHeaderEncoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        controlResponseListener.onRecordingNotFound(
            recordingNotFoundResponseDecoder.correlationId(),
            recordingNotFoundResponseDecoder.recordingId(),
            recordingNotFoundResponseDecoder.maxRecordingId());
    }

    private void handleArchiverResponse(
        final ControlResponseListener listener,
        final DirectBuffer buffer,
        final int offset)
    {
        archiverResponseDecoder.wrap(
            buffer,
            offset + MessageHeaderEncoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        listener.onResponse(
            archiverResponseDecoder.correlationId(),
            archiverResponseDecoder.code(),
            archiverResponseDecoder.errorMessage());
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
            replayAbortedDecoder.endPosition());
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
            recordingDescriptorDecoder.joinTimestamp(),
            recordingDescriptorDecoder.endTimestamp(),
            recordingDescriptorDecoder.joinPosition(),
            recordingDescriptorDecoder.endPosition(),
            recordingDescriptorDecoder.initialTermId(),
            recordingDescriptorDecoder.segmentFileLength(),
            recordingDescriptorDecoder.termBufferLength(),
            recordingDescriptorDecoder.mtuLength(),
            recordingDescriptorDecoder.sessionId(),
            recordingDescriptorDecoder.streamId(),
            recordingDescriptorDecoder.channel(),
            recordingDescriptorDecoder.sourceIdentity(),
            recordingDescriptorDecoder.originalChannel());
    }
}
