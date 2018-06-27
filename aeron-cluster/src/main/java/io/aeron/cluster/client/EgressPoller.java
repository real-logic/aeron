/*
 *  Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.cluster.client;

import io.aeron.Aeron;
import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

public class EgressPoller implements ControlledFragmentHandler
{
    private final int fragmentLimit;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SessionEventDecoder sessionEventDecoder = new SessionEventDecoder();
    private final NewLeaderEventDecoder newLeaderEventDecoder = new NewLeaderEventDecoder();
    private final SessionHeaderDecoder sessionHeaderDecoder = new SessionHeaderDecoder();
    private final ChallengeDecoder challengeDecoder = new ChallengeDecoder();
    private final ControlledFragmentAssembler fragmentAssembler = new ControlledFragmentAssembler(this);
    private final Subscription subscription;
    private long clusterSessionId = Aeron.NULL_VALUE;
    private long correlationId = Aeron.NULL_VALUE;
    private int templateId = Aeron.NULL_VALUE;
    private int leaderMemberId = Aeron.NULL_VALUE;
    private boolean pollComplete = false;
    private EventCode eventCode;
    private String detail = "";
    private byte[] encodedChallenge;

    public EgressPoller(final Subscription subscription, final int fragmentLimit)
    {
        this.subscription = subscription;
        this.fragmentLimit = fragmentLimit;
    }

    /**
     * Get the {@link Subscription} used for polling events.
     *
     * @return the {@link Subscription} used for polling events.
     */
    public Subscription subscription()
    {
        return subscription;
    }

    /**
     * Get the template id of the last received event.
     *
     * @return the template id of the last received event.
     */
    public int templateId()
    {
        return templateId;
    }

    /**
     * Cluster session id of the last polled event or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return cluster session id of the last polled event or {@link Aeron#NULL_VALUE} if unrecognised template.
     */
    public long clusterSessionId()
    {
        return clusterSessionId;
    }

    /**
     * Correlation id of the last polled event or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return correlation id of the last polled event or {@link Aeron#NULL_VALUE} if unrecognised template.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Leader cluster member id of the last polled event or {@link Aeron#NULL_VALUE} if poll returned nothing.
     *
     * @return leader cluster member id of the last polled event or {@link Aeron#NULL_VALUE} if poll returned nothing.
     */
    public int leaderMemberId()
    {
        return leaderMemberId;
    }

    /**
     * Get the event code returned from the last session event.
     *
     * @return the event code returned from the last session event.
     */
    public EventCode eventCode()
    {
        return eventCode;
    }

    /**
     * Get the detail returned in the last session event.
     *
     * @return the detail returned in the last session event.
     */
    public String detail()
    {
        return detail;
    }

    /**
     * Get the encoded challenge in the last challenge.
     *
     * @return the encoded challenge in the last challenge or null if last message was not a challenge.
     */
    public byte[] encodedChallenge()
    {
        return encodedChallenge;
    }

    /**
     * Has the last polling action received a complete event?
     *
     * @return true if the last polling action received a complete event.
     */
    public boolean isPollComplete()
    {
        return pollComplete;
    }

    /**
     * Was last message a challenge or not?
     *
     * @return true if last message was a challenge or false if not.
     */
    public boolean isChallenged()
    {
        return ChallengeDecoder.TEMPLATE_ID == templateId;
    }

    public int poll()
    {
        clusterSessionId = Aeron.NULL_VALUE;
        correlationId = Aeron.NULL_VALUE;
        leaderMemberId = Aeron.NULL_VALUE;
        templateId = Aeron.NULL_VALUE;
        eventCode = null;
        detail = "";
        encodedChallenge = null;
        pollComplete = false;

        return subscription.controlledPoll(fragmentAssembler, fragmentLimit);
    }

    public ControlledFragmentAssembler.Action onFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        templateId = messageHeaderDecoder.templateId();
        switch (templateId)
        {
            case SessionEventDecoder.TEMPLATE_ID:
                sessionEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                clusterSessionId = sessionEventDecoder.clusterSessionId();
                correlationId = sessionEventDecoder.correlationId();
                leaderMemberId = sessionEventDecoder.leaderMemberId();
                eventCode = sessionEventDecoder.code();
                detail = sessionEventDecoder.detail();
                break;

            case NewLeaderEventDecoder.TEMPLATE_ID:
                newLeaderEventDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                clusterSessionId = newLeaderEventDecoder.clusterSessionId();
                break;

            case SessionHeaderDecoder.TEMPLATE_ID:
                sessionHeaderDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                clusterSessionId = sessionHeaderDecoder.clusterSessionId();
                correlationId = sessionHeaderDecoder.correlationId();
                break;

            case ChallengeDecoder.TEMPLATE_ID:
                challengeDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                encodedChallenge = new byte[challengeDecoder.encodedChallengeLength()];
                challengeDecoder.getEncodedChallenge(encodedChallenge, 0, challengeDecoder.encodedChallengeLength());

                clusterSessionId = challengeDecoder.clusterSessionId();
                correlationId = challengeDecoder.correlationId();
                break;

            default:
                throw new ClusterException("unknown templateId: " + templateId);
        }

        pollComplete = true;

        return ControlledFragmentAssembler.Action.BREAK;
    }
}
