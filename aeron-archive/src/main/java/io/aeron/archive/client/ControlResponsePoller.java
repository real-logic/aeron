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

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Encapsulate the polling and decoding of archive control protocol response messages.
 */
public class ControlResponsePoller implements ControlledFragmentHandler
{
    private static final int FRAGMENT_LIMIT = 10;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();

    private final Subscription subscription;
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this);
    private long controlSessionId = Aeron.NULL_VALUE;
    private long correlationId = Aeron.NULL_VALUE;
    private long relevantId = Aeron.NULL_VALUE;
    private int templateId = Aeron.NULL_VALUE;
    private ControlResponseCode code;
    private String errorMessage;
    private boolean pollComplete = false;

    /**
     * Create a poller for a given subscription to an archive for control response messages.
     *
     * @param subscription  to poll for new events.
     */
    public ControlResponsePoller(final Subscription subscription)
    {
        this.subscription = subscription;
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
        controlSessionId = -1;
        correlationId = -1;
        relevantId = -1;
        templateId = -1;
        pollComplete = false;

        return subscription.controlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    /**
     * Control session id of the last polled message or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return control session id of the last polled message or {@link Aeron#NULL_VALUE} if unrecognised template.
     */
    public long controlSessionId()
    {
        return controlSessionId;
    }

    /**
     * Correlation id of the last polled message or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return correlation id of the last polled message or {@link Aeron#NULL_VALUE} if unrecognised template.
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
     * Has the last polling action received a complete message?
     *
     * @return true if the last polling action received a complete message?
     */
    public boolean isPollComplete()
    {
        return pollComplete;
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

    public ControlledFragmentAssembler.Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case ControlResponseDecoder.TEMPLATE_ID:
                controlResponseDecoder.wrap(
                    buffer,
                    offset + MessageHeaderEncoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                controlSessionId = controlResponseDecoder.controlSessionId();
                correlationId = controlResponseDecoder.correlationId();
                relevantId = controlResponseDecoder.relevantId();
                code = controlResponseDecoder.code();
                if (ControlResponseCode.ERROR == code)
                {
                    errorMessage = controlResponseDecoder.errorMessage();
                }
                else
                {
                    errorMessage = "";
                }
                break;

            case RecordingDescriptorDecoder.TEMPLATE_ID:
                break;

            default:
                throw new ArchiveException("unknown templateId: " + templateId);
        }

        pollComplete = true;

        return Action.BREAK;
    }
}
