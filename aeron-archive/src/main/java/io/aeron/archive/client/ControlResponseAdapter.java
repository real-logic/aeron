/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.archive.codecs.ControlResponseDecoder;
import io.aeron.archive.codecs.MessageHeaderDecoder;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Encapsulate the polling, decoding, and dispatching of archive control protocol response messages.
 */
public class ControlResponseAdapter implements FragmentHandler
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();

    private final int fragmentLimit;
    private final ControlResponseListener listener;
    private final Subscription subscription;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);

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

    /**
     * Dispatch a descriptor message to a consumer by reading the fields in the correct order.
     *
     * @param decoder  which wraps the encoded message ready for reading.
     * @param consumer to which the decoded fields should be passed.
     */
    public static void dispatchDescriptor(
        final RecordingDescriptorDecoder decoder, final RecordingDescriptorConsumer consumer)
    {
        consumer.onRecordingDescriptor(
            decoder.controlSessionId(),
            decoder.correlationId(),
            decoder.recordingId(),
            decoder.startTimestamp(),
            decoder.stopTimestamp(),
            decoder.startPosition(),
            decoder.stopPosition(),
            decoder.initialTermId(),
            decoder.segmentFileLength(),
            decoder.termBufferLength(),
            decoder.mtuLength(),
            decoder.sessionId(),
            decoder.streamId(),
            decoder.strippedChannel(),
            decoder.originalChannel(),
            decoder.sourceIdentity());
    }

    public void onFragment(
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

            default:
                throw new ArchiveException("unknown templateId: " + templateId);
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
            controlResponseDecoder.controlSessionId(),
            controlResponseDecoder.correlationId(),
            controlResponseDecoder.relevantId(),
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

        dispatchDescriptor(recordingDescriptorDecoder, listener);
    }
}
