/*
 * Copyright 2014-2021 Real Logic Limited.
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
 * Encapsulate the polling and decoding of archive control protocol response messages.
 */
public final class ControlResponsePoller
{
    /**
     * Limit to apply when polling response messages.
     */
    public static final int FRAGMENT_LIMIT = 10;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final ControlResponseDecoder controlResponseDecoder = new ControlResponseDecoder();
    private final ChallengeDecoder challengeDecoder = new ChallengeDecoder();

    private final Subscription subscription;
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this::onFragment);
    private long controlSessionId = Aeron.NULL_VALUE;
    private long correlationId = Aeron.NULL_VALUE;
    private long relevantId = Aeron.NULL_VALUE;
    private int version = 0;
    private final int fragmentLimit;
    private ControlResponseCode code;
    private String errorMessage;
    private byte[] encodedChallenge = null;
    private boolean isPollComplete = false;

    /**
     * Create a poller for a given subscription to an archive for control response messages.
     *
     * @param subscription  to poll for new events.
     * @param fragmentLimit to apply when polling.
     */
    private ControlResponsePoller(final Subscription subscription, final int fragmentLimit)
    {
        this.subscription = subscription;
        this.fragmentLimit = fragmentLimit;
    }

    /**
     * Create a poller for a given subscription to an archive for control response messages with a default
     * fragment limit for polling as {@link #FRAGMENT_LIMIT}.
     *
     * @param subscription  to poll for new events.
     */
    public ControlResponsePoller(final Subscription subscription)
    {
        this(subscription, FRAGMENT_LIMIT);
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
     * Poll for control response events.
     *
     * @return the number of fragments read during the operation. Zero if no events are available.
     */
    public int poll()
    {
        controlSessionId = Aeron.NULL_VALUE;
        correlationId = Aeron.NULL_VALUE;
        relevantId = Aeron.NULL_VALUE;
        version = 0;
        errorMessage = null;
        encodedChallenge = null;
        isPollComplete = false;

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

    /**
     * Was the last polling action received a challenge message?
     *
     * @return true if the last polling action received was a challenge message, false if not.
     */
    public boolean wasChallenged()
    {
        return null != encodedChallenge;
    }

    /**
     * Get the encoded challenge of the last challenge.
     *
     * @return the encoded challenge of the last challenge.
     */
    public byte[] encodedChallenge()
    {
        return encodedChallenge;
    }

    ControlledFragmentAssembler.Action onFragment(
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

        if (messageHeaderDecoder.templateId() == ControlResponseDecoder.TEMPLATE_ID)
        {
            controlResponseDecoder.wrap(
                buffer,
                offset + MessageHeaderEncoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            controlSessionId = controlResponseDecoder.controlSessionId();
            correlationId = controlResponseDecoder.correlationId();
            relevantId = controlResponseDecoder.relevantId();
            code = controlResponseDecoder.code();
            version = controlResponseDecoder.version();
            errorMessage = controlResponseDecoder.errorMessage();
            isPollComplete = true;

            return ControlledFragmentHandler.Action.BREAK;
        }

        if (messageHeaderDecoder.templateId() == ChallengeDecoder.TEMPLATE_ID)
        {
            challengeDecoder.wrap(
                buffer,
                offset + MessageHeaderEncoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            controlSessionId = challengeDecoder.controlSessionId();
            correlationId = challengeDecoder.correlationId();
            relevantId = Aeron.NULL_VALUE;
            code = ControlResponseCode.NULL_VAL;
            version = challengeDecoder.version();
            errorMessage = "";

            final int encodedChallengeLength = challengeDecoder.encodedChallengeLength();
            encodedChallenge = new byte[encodedChallengeLength];
            challengeDecoder.getEncodedChallenge(encodedChallenge, 0, encodedChallengeLength);

            isPollComplete = true;

            return ControlledFragmentHandler.Action.BREAK;
        }

        return ControlledFragmentHandler.Action.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ControlResponsePoller{" +
            "controlSessionId=" + controlSessionId +
            ", correlationId=" + correlationId +
            ", relevantId=" + relevantId +
            ", code=" + code +
            ", version=" + SemanticVersion.toString(version) +
            ", errorMessage='" + errorMessage + '\'' +
            ", isPollComplete=" + isPollComplete +
            '}';
    }
}
