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

import io.aeron.Subscription;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Encapsulate the polling, decoding, and dispatching of recording transition events for a session plus the
 * asynchronous events to check for errors.
 */
public class RecordingTransitionAdapter implements FragmentHandler
{
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final RecordingTransitionDecoder recordingTransitionDecoder = new RecordingTransitionDecoder();

    private final int fragmentLimit;
    private final long controlSessionId;
    private final ControlEventListener controlEventListener;
    private final RecordingTransitionConsumer recordingTransitionConsumer;
    private final Subscription subscription;

    /**
     * Create an adapter for a given subscription to an archive for recording events.
     *
     * @param controlSessionId            to listen for associated asynchronous control events, such as errors.
     * @param controlEventListener        listener for control events which may indicate an error on the session.
     * @param recordingTransitionConsumer consumer of recording transition events.
     * @param subscription                to poll for new events.
     * @param fragmentLimit               to apply for each polling operation.
     */
    public RecordingTransitionAdapter(
        final long controlSessionId,
        final ControlEventListener controlEventListener,
        final RecordingTransitionConsumer recordingTransitionConsumer,
        final Subscription subscription,
        final int fragmentLimit)
    {
        this.controlSessionId = controlSessionId;
        this.controlEventListener = controlEventListener;
        this.recordingTransitionConsumer = recordingTransitionConsumer;
        this.subscription = subscription;
        this.fragmentLimit = fragmentLimit;
    }

    /**
     * Poll for recording transitions and dispatch them to the {@link RecordingTransitionDecoder} for this instance,
     * plus check for async responses for this control session which may have an exception and dispatch to the
     * {@link ControlResponseListener}.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    public int poll()
    {
        return subscription.poll(this, fragmentLimit);
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
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
                }
                break;

            case RecordingTransitionDecoder.TEMPLATE_ID:
                recordingTransitionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                if (recordingTransitionDecoder.controlSessionId() == controlSessionId)
                {
                    recordingTransitionConsumer.onTransition(
                        recordingTransitionDecoder.controlSessionId(),
                        recordingTransitionDecoder.correlationId(),
                        recordingTransitionDecoder.recordingId(),
                        recordingTransitionDecoder.subscriptionId(),
                        recordingTransitionDecoder.position(),
                        recordingTransitionDecoder.transitionType());
                }
                break;
        }
    }
}
