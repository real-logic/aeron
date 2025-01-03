/*
 * Copyright 2014-2025 Real Logic Limited.
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

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;

/**
 * Encapsulate the polling, decoding, and dispatching of recording transition events for a session plus the
 * asynchronous events to check for errors.
 * <p>
 * Important: set the underlying {@link RecordingSignalConsumer} instance on the {@link AeronArchive} using the
 * {@link AeronArchive.Context#recordingSignalConsumer(RecordingSignalConsumer)} method to avoid missing signals.
 *
 * @see RecordingSignal
 */
public final class RecordingSignalAdapter
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();
    private final ControlledFragmentAssembler assembler = new ControlledFragmentAssembler(this::onFragment);
    private final ControlEventListener controlEventListener;
    private final RecordingSignalConsumer recordingSignalConsumer;
    private final Subscription subscription;
    private final int fragmentLimit;
    private final long controlSessionId;
    private boolean isDone = false;

    /**
     * Create an adapter for a given subscription to an archive for recording events.
     *
     * @param controlSessionId        to listen for associated asynchronous control events, such as errors.
     * @param controlEventListener    listener for control events which may indicate an error on the session.
     * @param recordingSignalConsumer consumer of recording transition events.
     * @param subscription            to poll for new events.
     * @param fragmentLimit           to apply for each polling operation.
     */
    public RecordingSignalAdapter(
        final long controlSessionId,
        final ControlEventListener controlEventListener,
        final RecordingSignalConsumer recordingSignalConsumer,
        final Subscription subscription,
        final int fragmentLimit)
    {
        this.controlSessionId = controlSessionId;
        this.controlEventListener = controlEventListener;
        this.recordingSignalConsumer = recordingSignalConsumer;
        this.subscription = subscription;
        this.fragmentLimit = fragmentLimit;
    }

    /**
     * Poll for recording transitions and dispatch them to the {@link RecordingSignalConsumer} for this instance,
     * plus check for async responses for this control session which may have an exception and dispatch to the
     * {@link ControlResponseListener}.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    public int poll()
    {
        if (isDone)
        {
            isDone = false;
        }

        return subscription.controlledPoll(assembler, fragmentLimit);
    }

    /**
     * Indicate that poll was successful and a signal or control response was received.
     *
     * @return true if a signal or control response was received.
     */
    public boolean isDone()
    {
        return isDone;
    }

    private ControlledFragmentHandler.Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (isDone)
        {
            return ABORT;
        }

        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        switch (messageHeaderDecoder.templateId())
        {
            case ControlResponseDecoder.TEMPLATE_ID:
                controlResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                if (controlResponseDecoder.controlSessionId() == controlSessionId)
                {
                    controlEventListener.onResponse(
                        controlSessionId,
                        controlResponseDecoder.correlationId(),
                        controlResponseDecoder.relevantId(),
                        controlResponseDecoder.code(),
                        controlResponseDecoder.errorMessage());

                    isDone = true;
                    return BREAK;
                }
                break;

            case RecordingSignalEventDecoder.TEMPLATE_ID:
                recordingSignalEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                if (recordingSignalEventDecoder.controlSessionId() == controlSessionId)
                {
                    recordingSignalConsumer.onSignal(
                        recordingSignalEventDecoder.controlSessionId(),
                        recordingSignalEventDecoder.correlationId(),
                        recordingSignalEventDecoder.recordingId(),
                        recordingSignalEventDecoder.subscriptionId(),
                        recordingSignalEventDecoder.position(),
                        recordingSignalEventDecoder.signal());

                    isDone = true;
                    return BREAK;
                }
                break;
        }

        return CONTINUE;
    }
}
