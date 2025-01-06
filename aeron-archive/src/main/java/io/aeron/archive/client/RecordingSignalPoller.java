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

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.SemanticVersion;

/**
 * Encapsulate the polling and decoding of archive control protocol response and recording signal messages.
 */
public final class RecordingSignalPoller
{
    /**
     * Limit to apply when polling messages.
     */
    public static final int FRAGMENT_LIMIT = 10;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final RecordingSignalEventDecoder recordingSignalEventDecoder = new RecordingSignalEventDecoder();

    private final Subscription subscription;
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this::onFragment);
    private final long controlSessionId;
    private long correlationId = Aeron.NULL_VALUE;
    private long relevantId = Aeron.NULL_VALUE;
    private int templateId = Aeron.NULL_VALUE;
    private int version = 0;
    private long recordingId = Aeron.NULL_VALUE;
    private long recordingSubscriptionId = Aeron.NULL_VALUE;
    private long recordingPosition = Aeron.NULL_VALUE;
    private RecordingSignal recordingSignal = null;
    private ControlResponseCode code;
    private String errorMessage;
    private final int fragmentLimit;
    private boolean isPollComplete = false;

    /**
     * Create a poller for a given subscription to an archive for control messages.
     *
     * @param controlSessionId to listen for associated asynchronous control events, such as errors.
     * @param subscription     to poll for new events.
     * @param fragmentLimit    to apply when polling.
     */
    private RecordingSignalPoller(final long controlSessionId, final Subscription subscription, final int fragmentLimit)
    {
        this.controlSessionId = controlSessionId;
        this.subscription = subscription;
        this.fragmentLimit = fragmentLimit;
    }

    /**
     * Create a poller for a given subscription to an archive for control response messages with a default
     * fragment limit for polling as {@link #FRAGMENT_LIMIT}.
     *
     * @param controlSessionId to listen for associated asynchronous control events, such as errors.
     * @param subscription to poll for new events.
     */
    public RecordingSignalPoller(final long controlSessionId, final Subscription subscription)
    {
        this(controlSessionId, subscription, FRAGMENT_LIMIT);
    }

    /**
     * Get the {@link Subscription} used for polling messages.
     *
     * @return the {@link Subscription} used for polling messages.
     */
    public Subscription subscription()
    {
        return subscription;
    }

    /**
     * Poll for control response events.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    public int poll()
    {
        if (isPollComplete)
        {
            isPollComplete = false;
            templateId = Aeron.NULL_VALUE;
            correlationId = Aeron.NULL_VALUE;
            relevantId = Aeron.NULL_VALUE;
            version = 0;
            errorMessage = null;
            recordingId = Aeron.NULL_VALUE;
            recordingSubscriptionId = Aeron.NULL_VALUE;
            recordingPosition = Aeron.NULL_VALUE;
            recordingSignal = null;
        }

        return subscription.controlledPoll(fragmentAssembler, fragmentLimit);
    }

    /**
     * Control session id of the last polled message or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return control session id of the last polled message or {@link Aeron#NULL_VALUE} if poll returned nothing.
     */
    public long controlSessionId()
    {
        return controlSessionId;
    }

    /**
     * Correlation id of the last polled message or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return correlation id of the last polled message or {@link Aeron#NULL_VALUE} if poll returned nothing.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Get the relevant id returned with the response, e.g. replay session id.
     *
     * @return the relevant id returned with the response.
     */
    public long relevantId()
    {
        return relevantId;
    }

    /**
     * Get the template id of the last received message.
     *
     * @return the template id of the last received message.
     */
    public int templateId()
    {
        return templateId;
    }

    /**
     * Get the recording id of the last received message.
     *
     * @return the recording id of the last received message.
     */
    public long recordingId()
    {
        return recordingId;
    }

    /**
     * Get the recording subscription id of the last received message.
     *
     * @return the recording subscription id of the last received message.
     */
    public long recordingSubscriptionId()
    {
        return recordingSubscriptionId;
    }

    /**
     * Get the recording position of the last received message.
     *
     * @return the recording position of the last received message.
     */
    public long recordingPosition()
    {
        return recordingPosition;
    }

    /**
     * Get the recording signal of the last received message.
     *
     * @return the recording signal of the last received message.
     */
    public RecordingSignal recordingSignal()
    {
        return recordingSignal;
    }

    /**
     * Version response from the server in semantic version form.
     *
     * @return response from the server in semantic version form.
     */
    public int version()
    {
        return version;
    }

    /**
     * Has the last polling action received a complete message?
     *
     * @return true if the last polling action received a complete message?
     */
    public boolean isPollComplete()
    {
        return isPollComplete;
    }

    /**
     * Get the response code of the last response.
     *
     * @return the response code of the last response.
     */
    public ControlResponseCode code()
    {
        return code;
    }

    /**
     * Get the error message of the last response.
     *
     * @return the error message of the last response.
     */
    public String errorMessage()
    {
        return errorMessage;
    }

    ControlledFragmentHandler.Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (isPollComplete)
        {
            return ControlledFragmentHandler.Action.ABORT;
        }

        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (schemaId != MessageHeaderDecoder.SCHEMA_ID)
        {
            throw new ArchiveException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        final int templateId = messageHeaderDecoder.templateId();

        if (ControlResponseDecoder.TEMPLATE_ID == templateId)
        {
            controlResponseDecoder.wrap(
                buffer,
                offset + MessageHeaderEncoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            if (controlResponseDecoder.controlSessionId() == controlSessionId)
            {
                this.templateId = templateId;
                correlationId = controlResponseDecoder.correlationId();
                relevantId = controlResponseDecoder.relevantId();
                code = controlResponseDecoder.code();
                version = controlResponseDecoder.version();
                errorMessage = controlResponseDecoder.errorMessage();
                isPollComplete = true;

                return ControlledFragmentHandler.Action.BREAK;
            }
        }
        else if (RecordingSignalEventDecoder.TEMPLATE_ID == templateId)
        {
            recordingSignalEventDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            if (recordingSignalEventDecoder.controlSessionId() == controlSessionId)
            {
                this.templateId = templateId;
                correlationId = recordingSignalEventDecoder.correlationId();
                recordingId = recordingSignalEventDecoder.recordingId();
                recordingSubscriptionId = recordingSignalEventDecoder.subscriptionId();
                recordingPosition = recordingSignalEventDecoder.position();
                recordingSignal = recordingSignalEventDecoder.signal();
                isPollComplete = true;

                return ControlledFragmentHandler.Action.BREAK;
            }
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "RecordingSignalPoller{" +
            "controlSessionId=" + controlSessionId +
            ", correlationId=" + correlationId +
            ", relevantId=" + relevantId +
            ", code=" + code +
            ", templateId=" + templateId +
            ", version=" + SemanticVersion.toString(version) +
            ", errorMessage='" + errorMessage + '\'' +
            ", recordingId=" + recordingId +
            ", recordingSubscriptionId=" + recordingSubscriptionId +
            ", recordingPosition=" + recordingPosition +
            ", recordingSignal=" + recordingSignal +
            ", isPollComplete=" + isPollComplete +
            '}';
    }
}
