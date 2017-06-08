/*
 *  Copyright 2017 Real Logic Ltd.
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

import org.agrona.BitUtil;

/**
 * Type safe means of building a channel URI associated with a publication or subscription.
 *
 * @see Aeron#addPublication(String, int)
 * @see Aeron#addSubscription(String, int)
 */
public class ChannelUriBuilder
{
    /**
     * Scheme for the Aeron URI namespace.
     */
    public static final String SCHEME = "aeron";

    private StringBuilder sb = new StringBuilder(64);

    private String prefix;
    private String media;
    private String endpoint;
    private String networkInterface;
    private String controlEndpoint;
    private String controlMode;
    private Boolean reliable;
    private Integer ttl;
    private Integer mtu;
    private Integer termLength;
    private Integer initialTermId;
    private Integer termId;
    private Integer termOffset;

    /**
     * Validates that the collection of set parameters are valid together.
     *
     * @return this for a fluent API.
     * @throws IllegalStateException if the combination of params is invalid.
     */
    public ChannelUriBuilder validate()
    {
        return this;
    }

    /**
     * Set the prefix for taking an addition action such as spying on an outgoing publication with "aeron-spy".
     *
     * @param prefix to be applied to the URI before the the scheme.
     * @return this for a fluent API.
     */
    public ChannelUriBuilder prefix(final String prefix)
    {
        if (null != prefix && !prefix.equals("aeron-spy"))
        {
            throw new IllegalArgumentException("Invalid prefix: " + prefix);
        }

        this.prefix = prefix;
        return this;
    }

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
    public ChannelUriBuilder media(final String media)
    {
        switch (media)
        {
            case "udp":
            case "ipc":
                break;

            default:
                throw new IllegalArgumentException("Invalid media: " + media);
        }

        this.media = media;
        return this;
    }

    public String media()
    {
        return media;
    }

    /**
     * Set the endpoint address:port for the channel. This is the address the publication sends to and the address
     * the subscription receives from.
     *
     * @param endpoint address and port for the channel.
     * @return this for a fluent API.
     */
    public ChannelUriBuilder endpoint(final String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public String endpoint()
    {
        return endpoint;
    }

    /**
     * Set the address of the local interface in the form host:[port]/[subnet mask] for routing traffic.
     *
     * @param networkInterface for routing traffic.
     * @return this for a fluent API.
     */
    public ChannelUriBuilder networkInterface(final String networkInterface)
    {
        this.networkInterface = networkInterface;
        return this;
    }

    public String networkInterface()
    {
        return networkInterface;
    }

    /**
     * Set the control address:port pair for dynamically joining a multi-destination-cast publication.
     *
     * @param controlEndpoint for joining a MDC control socket.
     * @return this for a fluent API.
     */
    public ChannelUriBuilder controlEndpoint(final String controlEndpoint)
    {
        this.controlEndpoint = controlEndpoint;
        return this;
    }

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
     */
    public ChannelUriBuilder controlMode(final String controlMode)
    {
        if (null != controlMode && !controlMode.equals(CommonContext.MDC_CONTROL_MODE_MANUAL))
        {
            throw new IllegalArgumentException("Invalid control mode: " + controlMode);
        }

        this.controlMode = controlMode;
        return this;
    }

    public String controlMode()
    {
        return controlMode;
    }

    /**
     * Set the subscription semantics for if loss is acceptable or not for a reliable delivery.
     *
     * @param isReliable false if loss can be be gap filled.
     * @return this for a fluent API.
     */
    public ChannelUriBuilder reliable(final Boolean isReliable)
    {
        this.reliable = isReliable;
        return this;
    }

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
     */
    public ChannelUriBuilder ttl(final Integer ttl)
    {
        if (null != ttl && (ttl < 0 || ttl > 255))
        {
            throw new IllegalArgumentException("TTL not in range 0-255: " + ttl);
        }

        this.ttl = ttl;
        return this;
    }

    public Integer ttl()
    {
        return ttl;
    }

    /**
     * Set the maximum transmission unit including Aeron header for a datagram payload.
     *
     * @param mtu the maximum transmission unit including Aeron header for a datagram payload.
     * @return this for a fluent API.
     */
    public ChannelUriBuilder mtu(final Integer mtu)
    {
        if (null != mtu && (mtu < 32 || mtu > 65504))
        {
            throw new IllegalArgumentException("MTU not in range 32-65504: " + mtu);
        }

        this.mtu = mtu;
        return this;
    }

    public Integer mtu()
    {
        return mtu;
    }

    /**
     * Set the length of buffer used for each term of the log. Valid values are powers of 2 in the 64K - 1G range.
     *
     * @param termLength of the buffer used for each term of the log.
     * @return this for a fluent API.
     */
    public ChannelUriBuilder termLength(final Integer termLength)
    {
        if (null != termLength)
        {
            if ((termLength < (1024 * 64) || termLength > (1024 * 1024 * 1024)))
            {
                throw new IllegalArgumentException("Term length not in range 64K-1G: " + termLength);
            }

            if (!BitUtil.isPowerOfTwo(termLength))
            {
                throw new IllegalArgumentException("Term length not power of 2: " + termLength);
            }
        }

        this.termLength = termLength;
        return this;
    }

    public Integer termLength()
    {
        return termLength;
    }

    /**
     * Set the initial term id at which a publication will start.
     *
     * @param initialTermId the initial term id at which a publication will start.
     * @return this for a fluent API.
     */
    public ChannelUriBuilder initialTermId(final Integer initialTermId)
    {
        this.initialTermId = initialTermId;
        return this;
    }

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
     */
    public ChannelUriBuilder termId(final Integer termId)
    {
        this.termId = termId;
        return this;
    }

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
     */
    public ChannelUriBuilder termOffset(final Integer termOffset)
    {
        if (null != termOffset)
        {
            if ((termOffset < 0 || termOffset > (1024 * 1024 * 1024)))
            {
                throw new IllegalArgumentException("Term offset not in range 0-1g: " + termOffset);
            }

            if (0 != (termOffset & (32 - 1)))
            {
                throw new IllegalArgumentException("Term offset not divisible by 32: " + termOffset);
            }
        }

        this.termOffset = termOffset;
        return this;
    }

    public Integer termOffset()
    {
        return termOffset;
    }

    public String buildUri()
    {
        sb.setLength(0);

        if (null != prefix)
        {
            sb.append(prefix).append(':');
        }

        sb.append(SCHEME).append(':').append(media).append('?');

        if (null != endpoint)
        {
            sb.append(CommonContext.ENDPOINT_PARAM_NAME).append('=').append(endpoint).append('|');
        }

        if (null != networkInterface)
        {
            sb.append(CommonContext.INTERFACE_PARAM_NAME).append('=').append(networkInterface).append('|');
        }

        if (null != controlEndpoint)
        {
            sb.append(CommonContext.MDC_CONTROL_PARAM_NAME).append('=').append(controlEndpoint).append('|');
        }

        if (null != controlMode)
        {
            sb.append(CommonContext.MDC_CONTROL_MODE_PARAM_NAME).append('=').append(controlMode).append('|');
        }

        if (null != reliable)
        {
            sb.append(CommonContext.RELIABLE_STREAM_PARAM_NAME).append('=').append(reliable).append('|');
        }

        if (null != ttl)
        {
            sb.append(CommonContext.TTL_PARAM_NAME).append('=').append(ttl.intValue()).append('|');
        }

        if (null != mtu)
        {
            sb.append(CommonContext.MTU_LENGTH_PARAM_NAME).append('=').append(mtu.intValue()).append('|');
        }

        if (null != termLength)
        {
            sb.append(CommonContext.TERM_LENGTH_PARAM_NAME).append('=').append(termLength.intValue()).append('|');
        }

        if (null != initialTermId)
        {
            sb.append(CommonContext.INITIAL_TERM_ID_PARAM_NAME)
                .append('=').append(initialTermId.intValue()).append('|');
        }

        if (null != termId)
        {
            sb.append(CommonContext.TERM_ID_PARAM_NAME).append('=').append(termId.intValue()).append('|');
        }

        if (null != termOffset)
        {
            sb.append(CommonContext.TERM_OFFSET_PARAM_NAME).append('=').append(termOffset.intValue()).append('|');
        }

        final char lastChar = sb.charAt(sb.length() - 1);
        if (lastChar == '|' || lastChar == '?')
        {
            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }
}
