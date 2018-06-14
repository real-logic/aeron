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
package io.aeron;

import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;

import static io.aeron.ChannelUri.SPY_QUALIFIER;
import static io.aeron.CommonContext.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;

/**
 * Type safe means of building a channel URI associated with a {@link Publication} or {@link Subscription}.
 *
 * @see Aeron#addPublication(String, int)
 * @see Aeron#addSubscription(String, int)
 * @see ChannelUri
 */
public class ChannelUriStringBuilder
{
    public static final String TAG_PREFIX = "tag:";

    private StringBuilder sb = new StringBuilder(64);

    private String prefix;
    private String media;
    private String endpoint;
    private String networkInterface;
    private String controlEndpoint;
    private String controlMode;
    private String tags;
    private Boolean reliable;
    private Integer ttl;
    private Integer mtu;
    private Integer termLength;
    private Integer initialTermId;
    private Integer termId;
    private Integer termOffset;
    private Integer sessionId;
    private Integer linger;
    private boolean isSessionIdTagged;

    /**
     * Clear out all the values thus setting back to the initial state.
     *
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder clear()
    {
        prefix = null;
        media = null;
        endpoint = null;
        networkInterface = null;
        controlEndpoint = null;
        controlMode = null;
        tags = null;
        reliable = null;
        ttl  = null;
        mtu = null;
        termLength = null;
        initialTermId = null;
        termId = null;
        termOffset = null;
        sessionId = null;
        isSessionIdTagged = false;

        return this;
    }

    /**
     * Validates that the collection of set parameters are valid together.
     *
     * @return this for a fluent API.
     * @throws IllegalStateException if the combination of params is invalid.
     */
    public ChannelUriStringBuilder validate()
    {
        if (null == media)
        {
            throw new IllegalStateException("media type is mandatory");
        }

        if (CommonContext.UDP_MEDIA.equals(media) && (null == endpoint && null == controlEndpoint))
        {
            throw new IllegalStateException("Either 'endpoint' or 'control' must be specified for UDP.");
        }

        int count = 0;
        count += null == initialTermId ? 0 : 1;
        count += null == termId ? 0 : 1;
        count += null == termOffset ? 0 : 1;

        if (count > 0 && count < 3)
        {
            throw new IllegalStateException(
                "If any of then a complete set of 'initialTermId', 'termId', and 'termOffset' must be provided");
        }

        return this;
    }

    /**
     * Set the prefix for taking an addition action such as spying on an outgoing publication with "aeron-spy".
     *
     * @param prefix to be applied to the URI before the the scheme.
     * @return this for a fluent API.
     * @see ChannelUri#SPY_QUALIFIER
     */
    public ChannelUriStringBuilder prefix(final String prefix)
    {
        if (null != prefix && !prefix.equals("") && !prefix.equals(SPY_QUALIFIER))
        {
            throw new IllegalArgumentException("Invalid prefix: " + prefix);
        }

        this.prefix = prefix;
        return this;
    }

    /**
     * Get the prefix for the additional action to be taken on the request.
     *
     * @return the prefix for the additional action to be taken on the request.
     */
    public String prefix()
    {
        return prefix;
    }

    /**
     * Set the media for this channel. Valid values are "udp" and "ipc".
     *
     * @param media for this channel.
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder media(final String media)
    {
        switch (media)
        {
            case CommonContext.UDP_MEDIA:
            case CommonContext.IPC_MEDIA:
                break;

            default:
                throw new IllegalArgumentException("Invalid media: " + media);
        }

        this.media = media;
        return this;
    }

    /**
     * The media over which the channel transmits.
     *
     * @return the media over which the channel transmits.
     */
    public String media()
    {
        return media;
    }

    /**
     * Set the endpoint address:port pairing for the channel. This is the address the publication sends to and the
     * address the subscription receives from.
     *
     * @param endpoint address and port for the channel.
     * @return this for a fluent API.
     * @see CommonContext#ENDPOINT_PARAM_NAME
     */
    public ChannelUriStringBuilder endpoint(final String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    /**
     * Get the endpoint address:port pairing for the channel.
     *
     * @return the endpoint address:port pairing for the channel.
     * @see CommonContext#ENDPOINT_PARAM_NAME
     */
    public String endpoint()
    {
        return endpoint;
    }

    /**
     * Set the address of the local interface in the form host:[port]/[subnet mask] for routing traffic.
     *
     * @param networkInterface for routing traffic.
     * @return this for a fluent API.
     * @see CommonContext#INTERFACE_PARAM_NAME
     */
    public ChannelUriStringBuilder networkInterface(final String networkInterface)
    {
        this.networkInterface = networkInterface;
        return this;
    }

    /**
     * Get the address of the local interface in the form host:[port]/[subnet mask] for routing traffic.
     *
     * @return the address of the local interface in the form host:[port]/[subnet mask] for routing traffic.
     * @see CommonContext#INTERFACE_PARAM_NAME
     */
    public String networkInterface()
    {
        return networkInterface;
    }

    /**
     * Set the control address:port pair for dynamically joining a multi-destination-cast publication.
     *
     * @param controlEndpoint for joining a MDC control socket.
     * @return this for a fluent API.
     * @see CommonContext#MDC_CONTROL_PARAM_NAME
     */
    public ChannelUriStringBuilder controlEndpoint(final String controlEndpoint)
    {
        this.controlEndpoint = controlEndpoint;
        return this;
    }

    /**
     * Get the control address:port pair for dynamically joining a multi-destination-cast publication.
     *
     * @return the control address:port pair for dynamically joining a multi-destination-cast publication.
     * @see CommonContext#MDC_CONTROL_PARAM_NAME
     */
    public String controlEndpoint()
    {
        return controlEndpoint;
    }

    /**
     * Set the control mode for multi-destination-cast. Set to "manual" for allowing control from the publication API.
     *
     * @param controlMode for taking control of MDC.
     * @return this for a fluent API.
     * @see Publication#addDestination(String)
     * @see Publication#removeDestination(String)
     * @see CommonContext#MDC_CONTROL_MODE_PARAM_NAME
     * @see CommonContext#MDC_CONTROL_MODE_MANUAL
     * @see CommonContext#MDC_CONTROL_MODE_DYNAMIC
     */
    public ChannelUriStringBuilder controlMode(final String controlMode)
    {
        if (null != controlMode &&
            !controlMode.equals(CommonContext.MDC_CONTROL_MODE_MANUAL) &&
            !controlMode.equals(CommonContext.MDC_CONTROL_MODE_DYNAMIC))
        {
            throw new IllegalArgumentException("Invalid control mode: " + controlMode);
        }

        this.controlMode = controlMode;
        return this;
    }

    /**
     * Get the control mode for multi-destination-cast.
     *
     * @return the control mode for multi-destination-cast.
     * @see CommonContext#MDC_CONTROL_MODE_PARAM_NAME
     * @see CommonContext#MDC_CONTROL_MODE_MANUAL
     * @see CommonContext#MDC_CONTROL_MODE_DYNAMIC
     */
    public String controlMode()
    {
        return controlMode;
    }

    /**
     * Set the subscription semantics for if loss is acceptable, or not, for a reliable message delivery.
     *
     * @param isReliable false if loss can be be gap filled.
     * @return this for a fluent API.
     * @see CommonContext#RELIABLE_STREAM_PARAM_NAME
     */
    public ChannelUriStringBuilder reliable(final Boolean isReliable)
    {
        this.reliable = isReliable;
        return this;
    }

    /**
     * Get the subscription semantics for if loss is acceptable, or not, for a reliable message delivery.
     *
     * @return the subscription semantics for if loss is acceptable, or not, for a reliable message delivery.
     * @see CommonContext#RELIABLE_STREAM_PARAM_NAME
     */
    public Boolean reliable()
    {
        return reliable;
    }

    /**
     * Set the Time To Live (TTL) for a multicast datagram. Valid values are 0-255 for the number of hops the datagram
     * can progress along.
     *
     * @param ttl value for a multicast datagram.
     * @return this for a fluent API.
     * @see CommonContext#TTL_PARAM_NAME
     */
    public ChannelUriStringBuilder ttl(final Integer ttl)
    {
        if (null != ttl && (ttl < 0 || ttl > 255))
        {
            throw new IllegalArgumentException("TTL not in range 0-255: " + ttl);
        }

        this.ttl = ttl;
        return this;
    }

    /**
     * Get the Time To Live (TTL) for a multicast datagram.
     *
     * @return the Time To Live (TTL) for a multicast datagram.
     * @see CommonContext#TTL_PARAM_NAME
     */
    public Integer ttl()
    {
        return ttl;
    }

    /**
     * Set the maximum transmission unit (MTU) including Aeron header for a datagram payload. If this is greater
     * than the network MTU for UDP then the packet will be fragmented and can amplify the impact of loss.
     *
     * @param mtu the maximum transmission unit including Aeron header for a datagram payload.
     * @return this for a fluent API.
     * @see CommonContext#MTU_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder mtu(final Integer mtu)
    {
        if (null != mtu)
        {
            if (mtu < 32 || mtu > 65504)
            {
                throw new IllegalArgumentException("MTU not in range 32-65504: " + mtu);
            }

            if ((mtu & (FRAME_ALIGNMENT - 1)) != 0)
            {
                throw new IllegalArgumentException("MTU not a multiple of FRAME_ALIGNMENT: mtu=" + mtu);
            }
        }

        this.mtu = mtu;
        return this;
    }

    /**
     * Get the maximum transmission unit (MTU) including Aeron header for a datagram payload. If this is greater
     * than the network MTU for UDP then the packet will be fragmented and can amplify the impact of loss.
     *
     * @return the maximum transmission unit (MTU) including Aeron header for a datagram payload.
     * @see CommonContext#MTU_LENGTH_PARAM_NAME
     */
    public Integer mtu()
    {
        return mtu;
    }

    /**
     * Set the length of buffer used for each term of the log. Valid values are powers of 2 in the 64K - 1G range.
     *
     * @param termLength of the buffer used for each term of the log.
     * @return this for a fluent API.
     * @see CommonContext#TERM_LENGTH_PARAM_NAME
     */
    public ChannelUriStringBuilder termLength(final Integer termLength)
    {
        if (null != termLength)
        {
            LogBufferDescriptor.checkTermLength(termLength);
        }

        this.termLength = termLength;
        return this;
    }

    /**
     * Get the length of buffer used for each term of the log.
     *
     * @return the length of buffer used for each term of the log.
     * @see CommonContext#TERM_LENGTH_PARAM_NAME
     */
    public Integer termLength()
    {
        return termLength;
    }

    /**
     * Set the initial term id at which a publication will start.
     *
     * @param initialTermId the initial term id at which a publication will start.
     * @return this for a fluent API.
     * @see CommonContext#INITIAL_TERM_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder initialTermId(final Integer initialTermId)
    {
        this.initialTermId = initialTermId;
        return this;
    }

    /**
     * the initial term id at which a publication will start.
     *
     * @return the initial term id at which a publication will start.
     * @see CommonContext#INITIAL_TERM_ID_PARAM_NAME
     */
    public Integer initialTermId()
    {
        return initialTermId;
    }

    /**
     * Set the current term id at which a publication will start. This when combined with the initial term can
     * establish a starting position.
     *
     * @param termId at which a publication will start.
     * @return this for a fluent API.
     * @see CommonContext#TERM_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder termId(final Integer termId)
    {
        this.termId = termId;
        return this;
    }

    /**
     * Get the current term id at which a publication will start.
     *
     * @return the current term id at which a publication will start.
     * @see CommonContext#TERM_ID_PARAM_NAME
     */
    public Integer termId()
    {
        return termId;
    }

    /**
     * Set the offset within a term at which a publication will start. This when combined with the term id can establish
     * a starting position.
     *
     * @param termOffset within a term at which a publication will start.
     * @return this for a fluent API.
     * @see CommonContext#TERM_OFFSET_PARAM_NAME
     */
    public ChannelUriStringBuilder termOffset(final Integer termOffset)
    {
        if (null != termOffset)
        {
            if ((termOffset < 0 || termOffset > LogBufferDescriptor.TERM_MAX_LENGTH))
            {
                throw new IllegalArgumentException("Term offset not in range 0-1g: " + termOffset);
            }

            if (0 != (termOffset & (FrameDescriptor.FRAME_ALIGNMENT - 1)))
            {
                throw new IllegalArgumentException("Term offset not multiple of FRAME_ALIGNMENT: " + termOffset);
            }
        }

        this.termOffset = termOffset;
        return this;
    }

    /**
     * Get the offset within a term at which a publication will start.
     *
     * @return the offset within a term at which a publication will start.
     * @see CommonContext#TERM_OFFSET_PARAM_NAME
     */
    public Integer termOffset()
    {
        return termOffset;
    }

    /**
     * Set the session id for a publication or restricted subscription.
     *
     * @param sessionId for the publication or a restricted subscription.
     * @return this for a fluent API.
     * @see CommonContext#SESSION_ID_PARAM_NAME
     */
    public ChannelUriStringBuilder sessionId(final Integer sessionId)
    {
        this.sessionId = sessionId;
        return this;
    }

    /**
     * Get the session id for a publication or restricted subscription.
     *
     * @return the session id for a publication or restricted subscription.
     * @see CommonContext#SESSION_ID_PARAM_NAME
     */
    public Integer sessionId()
    {
        return sessionId;
    }

    /**
     * Set the time a publication will linger in nanoseconds after being drained. This time is so that tail loss
     * can be recovered.
     *
     * @param lingerNs time for the publication after it is drained.
     * @return this for a fluent API.
     * @see CommonContext#LINGER_PARAM_NAME
     */
    public ChannelUriStringBuilder linger(final Integer lingerNs)
    {
        if (null != lingerNs && lingerNs < 0)
        {
            throw new IllegalArgumentException("Linger value cannot be negative: " + lingerNs);
        }

        this.linger = lingerNs;
        return this;
    }

    /**
     * Get the time a publication will linger in nanoseconds after being drained. This time is so that tail loss
     * can be recovered.
     *
     * @return the linger time in nanoseconds a publication will wait around after being drained.
     * @see CommonContext#LINGER_PARAM_NAME
     */
    public Integer linger()
    {
        return linger;
    }

    /**
     * Set the tags for a channel, and/or publication or subscription.
     *
     * @param tags for the channel, publication or subscription.
     * @return this for a fluent API.
     * @see CommonContext#TAGS_PARAM_NAME
     */
    public ChannelUriStringBuilder tags(final String tags)
    {
        this.tags = tags;
        return this;
    }

    /**
     * Get the tags for a channel, and/or publication or subscription.
     *
     * @return the tags for a channel, publication or subscription.
     * @see CommonContext#TAGS_PARAM_NAME
     */
    public String tags()
    {
        return tags;
    }

    /**
     * Toggle the value for {@link #sessionId()} being tagged or not.
     *
     * @param isSessionIdTagged for session id
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder isSessionIdTagged(final boolean isSessionIdTagged)
    {
        this.isSessionIdTagged = isSessionIdTagged;
        return this;
    }

    /**
     * Is the value for {@link #sessionId()} a tagged.
     *
     * @return whether the value for {@link #sessionId()} a tag reference or not.
     */
    public boolean isSessionIdTagged()
    {
        return isSessionIdTagged;
    }

    /**
     * Initialise a channel for restarting a publication at a given position.
     *
     * @param position      at which the publication should be started.
     * @param initialTermId what which the stream would start.
     * @param termLength    for the stream.
     * @return this for a fluent API.
     */
    public ChannelUriStringBuilder initialPosition(final long position, final int initialTermId, final int termLength)
    {
        final int bitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);

        this.initialTermId = initialTermId;
        this.termId = LogBufferDescriptor.computeTermIdFromPosition(position, bitsToShift, initialTermId);
        this.termOffset = (int)(position & (termLength - 1));
        this.termLength = termLength;

        return this;
    }

    /**
     * Build a channel URI String for the given parameters.
     *
     * @return a channel URI String for the given parameters.
     */
    public String build()
    {
        sb.setLength(0);

        if (null != prefix && !"".equals(prefix))
        {
            sb.append(prefix).append(':');
        }

        sb.append(ChannelUri.AERON_SCHEME).append(':').append(media).append('?');

        if (null != tags)
        {
            sb.append(TAGS_PARAM_NAME).append('=').append(tags).append('|');
        }

        if (null != endpoint)
        {
            sb.append(ENDPOINT_PARAM_NAME).append('=').append(endpoint).append('|');
        }

        if (null != networkInterface)
        {
            sb.append(INTERFACE_PARAM_NAME).append('=').append(networkInterface).append('|');
        }

        if (null != controlEndpoint)
        {
            sb.append(MDC_CONTROL_PARAM_NAME).append('=').append(controlEndpoint).append('|');
        }

        if (null != controlMode)
        {
            sb.append(MDC_CONTROL_MODE_PARAM_NAME).append('=').append(controlMode).append('|');
        }

        if (null != reliable)
        {
            sb.append(RELIABLE_STREAM_PARAM_NAME).append('=').append(reliable).append('|');
        }

        if (null != ttl)
        {
            sb.append(TTL_PARAM_NAME).append('=').append(ttl.intValue()).append('|');
        }

        if (null != mtu)
        {
            sb.append(MTU_LENGTH_PARAM_NAME).append('=').append(mtu.intValue()).append('|');
        }

        if (null != termLength)
        {
            sb.append(TERM_LENGTH_PARAM_NAME).append('=').append(termLength.intValue()).append('|');
        }

        if (null != initialTermId)
        {
            sb.append(INITIAL_TERM_ID_PARAM_NAME).append('=').append(initialTermId.intValue()).append('|');
        }

        if (null != termId)
        {
            sb.append(TERM_ID_PARAM_NAME).append('=').append(termId.intValue()).append('|');
        }

        if (null != termOffset)
        {
            sb.append(TERM_OFFSET_PARAM_NAME).append('=').append(termOffset.intValue()).append('|');
        }

        if (null != sessionId)
        {
            sb.append(SESSION_ID_PARAM_NAME).append('=').append(prefixTag(isSessionIdTagged, sessionId)).append('|');
        }

        if (null != linger)
        {
            sb.append(LINGER_PARAM_NAME).append('=').append(linger.intValue()).append('|');
        }

        final char lastChar = sb.charAt(sb.length() - 1);
        if (lastChar == '|' || lastChar == '?')
        {
            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }

    /**
     * Call {@link Integer#valueOf(String)} only if the value param is not null. Else pass null on.
     *
     * @param value to check for null and convert if not null.
     * @return null if value param is null or result of {@link Integer#valueOf(String)}.
     * @see Integer#valueOf(String)
     */
    public static Integer integerValueOf(final String value)
    {
        return null == value ? null : Integer.valueOf(value);
    }

    private static String prefixTag(final boolean isTagged, final Integer value)
    {
        return isTagged ? TAG_PREFIX + value.toString() : value.toString();
    }
}
