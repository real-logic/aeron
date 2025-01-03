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

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;

/**
 * Encapsulate the polling, decoding, dispatching of recording descriptors from an archive.
 *
 * @see RecordingSubscriptionDescriptorConsumer
 * @see ArchiveProxy#listRecordingSubscriptions(int, int, String, int, boolean, long, long)
 * @see AeronArchive#listRecordingSubscriptions(int, int, String, int, boolean, RecordingSubscriptionDescriptorConsumer)
 */
public final class RecordingSubscriptionDescriptorPoller
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final RecordingSubscriptionDescriptorDecoder recordingSubscriptionDescriptorDecoder =
        new RecordingSubscriptionDescriptorDecoder();
    private final RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();

    private final long controlSessionId;
    private final int fragmentLimit;
    private final Subscription subscription;
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this::onFragment);
    private final ErrorHandler errorHandler;
    private final RecordingSignalConsumer recordingSignalConsumer;

    private long correlationId;
    private int remainingSubscriptionCount;
    private boolean isDispatchComplete = false;
    private RecordingSubscriptionDescriptorConsumer subscriptionDescriptorConsumer;

    /**
     * Create a poller for a given subscription to an archive for control response messages.
     *
     * @param subscription     to poll for new events.
     * @param errorHandler     to call for asynchronous errors.
     * @param controlSessionId to filter the responses.
     * @param fragmentLimit    to apply for each polling operation.
     */
    public RecordingSubscriptionDescriptorPoller(
        final Subscription subscription,
        final ErrorHandler errorHandler,
        final long controlSessionId,
        final int fragmentLimit)
    {
        this(
            subscription,
            errorHandler,
            AeronArchive.Configuration.NO_OP_RECORDING_SIGNAL_CONSUMER,
            controlSessionId,
            fragmentLimit);
    }

    /**
     * Create a poller for a given subscription to an archive for control response messages.
     *
     * @param subscription            to poll for new events.
     * @param errorHandler            to call for asynchronous errors.
     * @param recordingSignalConsumer for consuming interleaved recording signals on the control session.
     * @param controlSessionId        to filter the responses.
     * @param fragmentLimit           to apply for each polling operation.
     */
    public RecordingSubscriptionDescriptorPoller(
        final Subscription subscription,
        final ErrorHandler errorHandler,
        final RecordingSignalConsumer recordingSignalConsumer,
        final long controlSessionId,
        final int fragmentLimit)
    {
        this.subscription = subscription;
        this.errorHandler = errorHandler;
        this.recordingSignalConsumer = recordingSignalConsumer;
        this.fragmentLimit = fragmentLimit;
        this.controlSessionId = controlSessionId;
    }

    /**
     * Get the {@link Subscription} used for polling responses.
     *
     * @return the {@link Subscription} used for polling responses.
     */
    public Subscription subscription()
    {
        return subscription;
    }

    /**
     * Poll for recording subscriptions and delegate to the {@link RecordingSubscriptionDescriptorConsumer}.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    public int poll()
    {
        if (isDispatchComplete)
        {
            isDispatchComplete = false;
        }

        return subscription.controlledPoll(fragmentAssembler, fragmentLimit);
    }

    /**
     * Control session id for filtering responses.
     *
     * @return control session id for filtering responses.
     */
    public long controlSessionId()
    {
        return controlSessionId;
    }

    /**
     * Is the dispatch of descriptors complete?
     *
     * @return true if the dispatch of descriptors complete?
     */
    public boolean isDispatchComplete()
    {
        return isDispatchComplete;
    }

    /**
     * Get the number of remaining subscriptions expected.
     *
     * @return the number of remaining subscriptions expected.
     */
    public int remainingSubscriptionCount()
    {
        return remainingSubscriptionCount;
    }

    /**
     * Reset the poller to dispatch the descriptors returned from a query.
     *
     * @param correlationId     for the response.
     * @param subscriptionCount of descriptors to expect.
     * @param consumer          to which the recording subscription descriptors are to be dispatched.
     */
    public void reset(
        final long correlationId, final int subscriptionCount, final RecordingSubscriptionDescriptorConsumer consumer)
    {
        this.correlationId = correlationId;
        this.subscriptionDescriptorConsumer = consumer;
        this.remainingSubscriptionCount = subscriptionCount;
        isDispatchComplete = false;
    }

    @SuppressWarnings("MethodLength")
    ControlledFragmentAssembler.Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (isDispatchComplete)
        {
            return ControlledFragmentAssembler.Action.ABORT;
        }

        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case ControlResponseDecoder.TEMPLATE_ID:
                controlResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                if (controlResponseDecoder.controlSessionId() == controlSessionId)
                {
                    final ControlResponseCode code = controlResponseDecoder.code();
                    final long responseCorrelationId = controlResponseDecoder.correlationId();

                    if (ControlResponseCode.SUBSCRIPTION_UNKNOWN == code && responseCorrelationId == correlationId)
                    {
                        isDispatchComplete = true;
                        return ControlledFragmentAssembler.Action.BREAK;
                    }

                    if (ControlResponseCode.ERROR == code)
                    {
                        final ArchiveException ex = new ArchiveException(
                            "response for correlationId=" + correlationId +
                            ", error: " + controlResponseDecoder.errorMessage(),
                            (int)controlResponseDecoder.relevantId(),
                            responseCorrelationId);

                        if (responseCorrelationId == correlationId)
                        {
                            throw ex;
                        }
                        else if (null != errorHandler)
                        {
                            errorHandler.onError(ex);
                        }
                    }
                }
                break;

            case RecordingSubscriptionDescriptorDecoder.TEMPLATE_ID:
                recordingSubscriptionDescriptorDecoder.wrap(
                    buffer,
                    offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                if (recordingSubscriptionDescriptorDecoder.controlSessionId() == controlSessionId &&
                    recordingSubscriptionDescriptorDecoder.correlationId() == correlationId)
                {
                    subscriptionDescriptorConsumer.onSubscriptionDescriptor(
                        controlSessionId,
                        correlationId,
                        recordingSubscriptionDescriptorDecoder.subscriptionId(),
                        recordingSubscriptionDescriptorDecoder.streamId(),
                        recordingSubscriptionDescriptorDecoder.strippedChannel());

                    if (0 == --remainingSubscriptionCount)
                    {
                        isDispatchComplete = true;
                        return ControlledFragmentAssembler.Action.BREAK;
                    }
                }
                break;

            case RecordingSignalEventDecoder.TEMPLATE_ID:
                recordingSignalEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                if (controlSessionId == recordingSignalEventDecoder.controlSessionId())
                {
                    recordingSignalConsumer.onSignal(
                        recordingSignalEventDecoder.controlSessionId(),
                        recordingSignalEventDecoder.correlationId(),
                        recordingSignalEventDecoder.recordingId(),
                        recordingSignalEventDecoder.subscriptionId(),
                        recordingSignalEventDecoder.position(),
                        recordingSignalEventDecoder.signal());
                }
                break;
        }

        return ControlledFragmentAssembler.Action.CONTINUE;
    }
}
