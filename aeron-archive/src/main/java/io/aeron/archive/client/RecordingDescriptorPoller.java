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

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Encapsulate the polling, decoding, dispatching of recording descriptors from an archive.
 */
public class RecordingDescriptorPoller implements ControlledFragmentHandler
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();

    private final int fragmentLimit;
    private final Subscription subscription;
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this);
    private final long controlSessionId;

    private long expectedCorrelationId;
    private int remainingRecordCount;
    private RecordingDescriptorConsumer consumer;
    private boolean isDispatchComplete = false;

    /**
     * Create a poller for a given subscription to an archive for control response messages.
     *
     * @param subscription     to poll for new events.
     * @param fragmentLimit    to apply for each polling operation.
     * @param controlSessionId to filter the responses.
     */
    public RecordingDescriptorPoller(
        final Subscription subscription, final int fragmentLimit, final long controlSessionId)
    {
        this.subscription = subscription;
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
     * Poll for recording events.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    public int poll()
    {
        isDispatchComplete = false;

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
     * Get the number of remaining records are expected.
     *
     * @return the number of remaining records are expected.
     */
    public int remainingRecordCount()
    {
        return remainingRecordCount;
    }

    /**
     * Reset the poller to dispatch the descriptors returned from a query.
     *
     * @param expectedCorrelationId for the response.
     * @param recordCount           of descriptors to expect.
     * @param consumer              to which the recording descriptors are to be dispatched.
     */
    public void reset(
        final long expectedCorrelationId,
        final int recordCount,
        final RecordingDescriptorConsumer consumer)
    {
        this.expectedCorrelationId = expectedCorrelationId;
        this.consumer = consumer;
        this.remainingRecordCount = recordCount;
        isDispatchComplete = false;
    }

    public Action
        onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case ControlResponseDecoder.TEMPLATE_ID:
                controlResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                if (controlResponseDecoder.controlSessionId() != controlSessionId)
                {
                    break;
                }

                final ControlResponseCode code = controlResponseDecoder.code();

                if (ControlResponseCode.RECORDING_UNKNOWN == code)
                {
                    isDispatchComplete = true;
                    return Action.BREAK;
                }

                if (ControlResponseCode.ERROR == code)
                {
                    throw new ArchiveException("response for expectedCorrelationId=" + expectedCorrelationId +
                        ", error: " + controlResponseDecoder.errorMessage());
                }
                break;

            case RecordingDescriptorDecoder.TEMPLATE_ID:
                recordingDescriptorDecoder.wrap(
                    buffer,
                    offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long correlationId = recordingDescriptorDecoder.correlationId();
                if (controlSessionId != recordingDescriptorDecoder.controlSessionId() ||
                    correlationId != expectedCorrelationId)
                {
                    break;
                }

                consumer.onRecordingDescriptor(
                    controlSessionId,
                    correlationId,
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

                if (0 == --remainingRecordCount)
                {
                    isDispatchComplete = true;
                    return Action.BREAK;
                }
                break;

            default:
                throw new ArchiveException("unknown templateId: " + templateId);
        }

        return Action.CONTINUE;
    }
}
