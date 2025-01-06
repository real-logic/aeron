/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
public final class ControlResponseAdapter
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();

    private final int fragmentLimit;
    private final ControlResponseListener controlResponseListener;
    private final RecordingSignalConsumer recordingSignalConsumer;
    private final Subscription subscription;
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this::onFragment);

    /**
     * Create an adapter for a given subscription to an archive for control response messages.
     *
     * @param controlResponseListener for dispatching responses.
     * @param subscription            to poll for responses.
     * @param fragmentLimit           to apply for each polling operation.
     */
    public ControlResponseAdapter(
        final ControlResponseListener controlResponseListener,
        final Subscription subscription,
        final int fragmentLimit)
    {
        this(
            controlResponseListener,
            AeronArchive.Configuration.NO_OP_RECORDING_SIGNAL_CONSUMER,
            subscription,
            fragmentLimit);
    }

    /**
     * Create an adapter for a given subscription to an archive for control response messages.
     *
     * @param controlResponseListener for dispatching responses.
     * @param recordingSignalConsumer for dispatching recording signals.
     * @param subscription            to poll for responses.
     * @param fragmentLimit           to apply for each polling operation.
     */
    public ControlResponseAdapter(
        final ControlResponseListener controlResponseListener,
        final RecordingSignalConsumer recordingSignalConsumer,
        final Subscription subscription,
        final int fragmentLimit)
    {
        this.fragmentLimit = fragmentLimit;
        this.controlResponseListener = controlResponseListener;
        this.recordingSignalConsumer = recordingSignalConsumer;
        this.subscription = subscription;
    }

    /**
     * Poll for recording events and dispatch them to the {@link ControlResponseListener} for this instance.
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

    void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        switch (messageHeaderDecoder.templateId())
        {
            case ControlResponseDecoder.TEMPLATE_ID:
                handleControlResponse(controlResponseListener, buffer, offset);
                break;

            case RecordingDescriptorDecoder.TEMPLATE_ID:
                handleRecordingDescriptor(controlResponseListener, buffer, offset);
                break;

            case RecordingSignalEventDecoder.TEMPLATE_ID:
                handleRecordingSignal(recordingSignalConsumer, buffer, offset);
                break;
        }
    }

    private void handleControlResponse(
        final ControlResponseListener listener, final DirectBuffer buffer, final int offset)
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
        final ControlResponseListener listener, final DirectBuffer buffer, final int offset)
    {
        recordingDescriptorDecoder.wrap(
            buffer,
            offset + MessageHeaderEncoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        dispatchDescriptor(recordingDescriptorDecoder, listener);
    }

    private void handleRecordingSignal(
        final RecordingSignalConsumer recordingSignalConsumer, final DirectBuffer buffer, final int offset)
    {
        recordingSignalEventDecoder.wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        recordingSignalConsumer.onSignal(
            recordingSignalEventDecoder.controlSessionId(),
            recordingSignalEventDecoder.correlationId(),
            recordingSignalEventDecoder.recordingId(),
            recordingSignalEventDecoder.subscriptionId(),
            recordingSignalEventDecoder.position(),
            recordingSignalEventDecoder.signal());
    }
}
