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
package io.aeron.driver.media;

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ErrorCode;
import io.aeron.driver.Configuration;
import io.aeron.driver.exceptions.InvalidChannelException;
import org.agrona.BitUtil;

import java.net.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.driver.media.NetworkUtil.*;
import static java.lang.System.lineSeparator;
import static java.net.InetAddress.getByAddress;

/**
 * The media configuration for Aeron UDP channels as an instantiation of the socket addresses for a {@link ChannelUri}.
 *
 * @see ChannelUri
 * @see io.aeron.ChannelUriStringBuilder
 */
public final class UdpChannel
{
    private static final byte[] HEX_TABLE =
    {
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

    private static final AtomicInteger UNIQUE_CANONICAL_FORM_VALUE = new AtomicInteger();

    private final boolean hasExplicitControl;
    private final boolean isMulticast;
    private final boolean hasTag;
    private final boolean hasNoDistinguishingCharacteristic;
    private final long tag;
    private final int multicastTtl;
    private final InetSocketAddress remoteData;
    private final InetSocketAddress localData;
    private final InetSocketAddress remoteControl;
    private final InetSocketAddress localControl;
    private final String uriStr;
    private final String canonicalForm;
    private final NetworkInterface localInterface;
    private final ProtocolFamily protocolFamily;
    private final ChannelUri channelUri;

    private UdpChannel(final Context context)
    {
        hasExplicitControl = context.hasExplicitControl;
        isMulticast = context.isMulticast;
        hasTag = context.hasTagId;
        hasNoDistinguishingCharacteristic = context.hasNoDistinguishingCharacteristic;
        tag = context.tagId;
        multicastTtl = context.multicastTtl;
        remoteData = context.remoteData;
        localData = context.localData;
        remoteControl = context.remoteControl;
        localControl = context.localControl;
        uriStr = context.uriStr;
        canonicalForm = context.canonicalForm;
        localInterface = context.localInterface;
        protocolFamily = context.protocolFamily;
        channelUri = context.channelUri;
    }

    /**
     * Parse channel URI and create a {@link UdpChannel}.
     *
     * @param channelUriString to parse
     * @return a new {@link UdpChannel}
     * @throws InvalidChannelException if an error occurs.
     */
    @SuppressWarnings("MethodLength")
    public static UdpChannel parse(final String channelUriString)
    {
        try
        {
            final ChannelUri channelUri = ChannelUri.parse(channelUriString);
            validateConfiguration(channelUri);

            InetSocketAddress endpointAddress = getEndpointAddress(channelUri);
            final InetSocketAddress explicitControlAddress = getExplicitControlAddress(channelUri);

            final String tagIdStr = channelUri.channelTag();
            final String controlMode = channelUri.get(CommonContext.MDC_CONTROL_MODE_PARAM_NAME);
            final boolean hasNoDistinguishingCharacteristic =
                null == endpointAddress && null == explicitControlAddress && null == tagIdStr;

            if (hasNoDistinguishingCharacteristic && null == controlMode)
            {
                throw new IllegalArgumentException(
                    "Aeron URIs for UDP must specify an endpoint address, control address, tag-id, or control-mode");
            }

            if (null != endpointAddress && endpointAddress.isUnresolved())
            {
                throw new UnknownHostException("could not resolve endpoint address: " + endpointAddress);
            }

            if (null != explicitControlAddress && explicitControlAddress.isUnresolved())
            {
                throw new UnknownHostException("could not resolve control address: " + explicitControlAddress);
            }

            final Context context = new Context()
                .uriStr(channelUriString)
                .channelUri(channelUri)
                .hasNoDistinguishingCharacteristic(hasNoDistinguishingCharacteristic);

            if (null != tagIdStr)
            {
                context.hasTagId(true).tagId(Long.parseLong(tagIdStr));
            }

            if (null == endpointAddress)
            {
                endpointAddress = new InetSocketAddress("0.0.0.0", 0);
            }

            if (endpointAddress.getAddress().isMulticastAddress())
            {
                final InetSocketAddress controlAddress = getMulticastControlAddress(endpointAddress);
                final InterfaceSearchAddress searchAddress = getInterfaceSearchAddress(channelUri);
                final NetworkInterface localInterface = findInterface(searchAddress);
                final InetSocketAddress resolvedAddress = resolveToAddressOfInterface(localInterface, searchAddress);

                context
                    .isMulticast(true)
                    .localControlAddress(resolvedAddress)
                    .remoteControlAddress(controlAddress)
                    .localDataAddress(resolvedAddress)
                    .remoteDataAddress(endpointAddress)
                    .localInterface(localInterface)
                    .multicastTtl(getMulticastTtl(channelUri))
                    .protocolFamily(getProtocolFamily(endpointAddress.getAddress()))
                    .canonicalForm(canonicalise(resolvedAddress, endpointAddress));
            }
            else if (null != explicitControlAddress)
            {
                context
                    .hasExplicitControl(true)
                    .remoteControlAddress(endpointAddress)
                    .remoteDataAddress(endpointAddress)
                    .localControlAddress(explicitControlAddress)
                    .localDataAddress(explicitControlAddress)
                    .protocolFamily(getProtocolFamily(endpointAddress.getAddress()))
                    .canonicalForm(canonicalise(explicitControlAddress, endpointAddress));
            }
            else
            {
                final InterfaceSearchAddress searchAddress = getInterfaceSearchAddress(channelUri);

                final InetSocketAddress localAddress = searchAddress.getInetAddress().isAnyLocalAddress() ?
                    searchAddress.getAddress() :
                    resolveToAddressOfInterface(findInterface(searchAddress), searchAddress);

                final String uniqueCanonicalFormSuffix = hasNoDistinguishingCharacteristic ?
                    ("-" + Integer.toString(UNIQUE_CANONICAL_FORM_VALUE.getAndAdd(1))) : "";

                context
                    .remoteControlAddress(endpointAddress)
                    .remoteDataAddress(endpointAddress)
                    .localControlAddress(localAddress)
                    .localDataAddress(localAddress)
                    .protocolFamily(getProtocolFamily(endpointAddress.getAddress()))
                    .canonicalForm(canonicalise(localAddress, endpointAddress) + uniqueCanonicalFormSuffix);
            }

            return new UdpChannel(context);
        }
        catch (final Exception ex)
        {
            throw new InvalidChannelException(ErrorCode.INVALID_CHANNEL, ex);
        }
    }

    private static InetSocketAddress getMulticastControlAddress(final InetSocketAddress endpointAddress)
        throws UnknownHostException
    {
        final byte[] addressAsBytes = endpointAddress.getAddress().getAddress();
        validateDataAddress(addressAsBytes);

        addressAsBytes[addressAsBytes.length - 1]++;
        return new InetSocketAddress(getByAddress(addressAsBytes), endpointAddress.getPort());
    }

    /**
     * Return a string which is a canonical form of the channel suitable for use as a file or directory
     * name and also as a method of hashing, etc.
     * <p>
     * A canonical form:
     * <ul>
     *     <li>begins with the string "UDP-"</li>
     *     <li>has all addresses converted to hexadecimal</li>
     *     <li>uses "-" as all field separators</li>
     * </ul>
     * <p>
     * The general format is:
     * UDP-interface-localPort-remoteAddress-remotePort
     *
     * @param localData  address/interface for the channel
     * @param remoteData address for the channel
     * @return canonical representation as a string
     */
    public static String canonicalise(final InetSocketAddress localData, final InetSocketAddress remoteData)
    {
        final StringBuilder builder = new StringBuilder(48);

        builder.append("UDP-");

        toHex(builder, localData.getAddress().getAddress())
            .append('-')
            .append(localData.getPort());

        builder.append('-');

        toHex(builder, remoteData.getAddress().getAddress())
            .append('-')
            .append(remoteData.getPort());

        return builder.toString();
    }

    /**
     * Remote data address information
     *
     * @return remote data address information
     */
    public InetSocketAddress remoteData()
    {
        return remoteData;
    }

    /**
     * Local data address information
     *
     * @return local data address information
     */
    public InetSocketAddress localData()
    {
        return localData;
    }

    /**
     * Remote control address information
     *
     * @return remote control address information
     */
    public InetSocketAddress remoteControl()
    {
        return remoteControl;
    }

    /**
     * Local control address information
     *
     * @return local control address information
     */
    public InetSocketAddress localControl()
    {
        return localControl;
    }

    /**
     * Get the {@link ChannelUri} for this channel.
     *
     * @return the {@link ChannelUri} for this channel.
     */
    public ChannelUri channelUri()
    {
        return channelUri;
    }

    /**
     * Multicast TTL information
     *
     * @return multicast TTL value
     */
    public int multicastTtl()
    {
        return multicastTtl;
    }

    /**
     * The canonical form for the channel
     * <p>
     * {@link UdpChannel#canonicalise(java.net.InetSocketAddress, java.net.InetSocketAddress)}
     *
     * @return canonical form for channel
     */
    public String canonicalForm()
    {
        return canonicalForm;
    }

    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final UdpChannel that = (UdpChannel)o;

        return !(canonicalForm != null ? !canonicalForm.equals(that.canonicalForm) : that.canonicalForm != null);
    }

    public int hashCode()
    {
        return canonicalForm != null ? canonicalForm.hashCode() : 0;
    }

    public String toString()
    {
        return canonicalForm;
    }

    /**
     * Does channel represent a multicast or not
     *
     * @return does channel represent a multicast or not
     */
    public boolean isMulticast()
    {
        return isMulticast;
    }

    /**
     * Local interface to be used by the channel.
     *
     * @return {@link NetworkInterface} for the local interface used by the channel
     */
    public NetworkInterface localInterface()
    {
        return localInterface;
    }

    /**
     * Original URI of the channel URI.
     *
     * @return the original uri
     */
    public String originalUriString()
    {
        return uriStr;
    }

    /**
     * Get the {@link ProtocolFamily} for this channel.
     *
     * @return the {@link ProtocolFamily} for this channel.
     */
    public ProtocolFamily protocolFamily()
    {
        return protocolFamily;
    }

    public long tag()
    {
        return tag;
    }

    /**
     * Does the channel have an explicit control address as used with multi-destination-cast or not?
     *
     * @return does channel have an explicit control address or not?
     */
    public boolean hasExplicitControl()
    {
        return hasExplicitControl;
    }

    public boolean hasTag()
    {
        return hasTag;
    }

    public boolean doesTagMatch(final UdpChannel udpChannel)
    {
        if (!hasTag || !udpChannel.hasTag() || tag != udpChannel.tag())
        {
            return false;
        }

        if (udpChannel.remoteData().getAddress().isAnyLocalAddress() &&
            udpChannel.remoteData().getPort() == 0 &&
            udpChannel.localData().getAddress().isAnyLocalAddress() &&
            udpChannel.localData().getPort() == 0)
        {
            return true;
        }

        throw new IllegalArgumentException("matching tag has set endpoint or control address");
    }

    /**
     * Get the endpoint address from the URI.
     *
     * @param uri to check
     * @return endpoint address for URI
     */
    public static InetSocketAddress destinationAddress(final ChannelUri uri)
    {
        try
        {
            validateConfiguration(uri);
            return getEndpointAddress(uri);
        }
        catch (final Exception ex)
        {
            throw new InvalidChannelException(ErrorCode.INVALID_CHANNEL, ex);
        }
    }

    /**
     * Used for debugging to get a human readable description of the channel.
     *
     * @return a human readable description of the channel.
     */
    public String description()
    {
        final StringBuilder builder = new StringBuilder("UdpChannel - ");
        if (null != localInterface)
        {
            builder
                .append("interface: ")
                .append(localInterface.getDisplayName())
                .append(", ");
        }

        builder
            .append("localData: ").append(localData)
            .append(", remoteData: ").append(remoteData)
            .append(", ttl: ").append(multicastTtl);

        return builder.toString();
    }

    private static InterfaceSearchAddress getInterfaceSearchAddress(final ChannelUri uri) throws UnknownHostException
    {
        final String interfaceValue = uri.get(CommonContext.INTERFACE_PARAM_NAME);
        if (null != interfaceValue)
        {
            return InterfaceSearchAddress.parse(interfaceValue);
        }

        return InterfaceSearchAddress.wildcard();
    }

    private static InetSocketAddress getEndpointAddress(final ChannelUri uri)
    {
        final String endpointValue = uri.get(CommonContext.ENDPOINT_PARAM_NAME);
        if (null != endpointValue)
        {
            return SocketAddressUtil.parse(endpointValue);
        }

        return null;
    }

    private static int getMulticastTtl(final ChannelUri uri)
    {
        final String ttlValue = uri.get(CommonContext.TTL_PARAM_NAME);
        if (null != ttlValue)
        {
            return Integer.parseInt(ttlValue);
        }

        return Configuration.SOCKET_MULTICAST_TTL;
    }

    private static InetSocketAddress getExplicitControlAddress(final ChannelUri uri)
    {
        final String controlValue = uri.get(CommonContext.MDC_CONTROL_PARAM_NAME);
        if (null != controlValue)
        {
            return SocketAddressUtil.parse(controlValue);
        }

        return null;
    }

    private static void validateDataAddress(final byte[] addressAsBytes)
    {
        if (BitUtil.isEven(addressAsBytes[addressAsBytes.length - 1]))
        {
            throw new IllegalArgumentException("Multicast data address must be odd");
        }
    }

    private static void validateConfiguration(final ChannelUri uri)
    {
        validateMedia(uri);
    }

    private static void validateMedia(final ChannelUri uri)
    {
        if (!CommonContext.UDP_MEDIA.equals(uri.media()))
        {
            throw new IllegalArgumentException("UdpChannel only supports UDP media: " + uri);
        }
    }

    private static InetSocketAddress resolveToAddressOfInterface(
        final NetworkInterface localInterface, final InterfaceSearchAddress searchAddress)
    {
        final InetAddress interfaceAddress = findAddressOnInterface(
            localInterface, searchAddress.getInetAddress(), searchAddress.getSubnetPrefix());

        if (null == interfaceAddress)
        {
            throw new IllegalStateException();
        }

        return new InetSocketAddress(interfaceAddress, searchAddress.getPort());
    }

    private static NetworkInterface findInterface(final InterfaceSearchAddress searchAddress)
        throws SocketException
    {
        final NetworkInterface[] filteredInterfaces = filterBySubnet(
            searchAddress.getInetAddress(), searchAddress.getSubnetPrefix());

        for (final NetworkInterface networkInterface : filteredInterfaces)
        {
            if (networkInterface.supportsMulticast() || networkInterface.isLoopback())
            {
                return networkInterface;
            }
        }

        throw new IllegalArgumentException(errorNoMatchingInterfaces(filteredInterfaces, searchAddress));
    }

    private static String errorNoMatchingInterfaces(
        final NetworkInterface[] filteredInterfaces, final InterfaceSearchAddress address)
        throws SocketException
    {
        final StringBuilder builder = new StringBuilder()
            .append("Unable to find multicast interface matching criteria: ")
            .append(address.getAddress())
            .append('/')
            .append(address.getSubnetPrefix());

        if (filteredInterfaces.length > 0)
        {
            builder.append(lineSeparator()).append("  Candidates:");

            for (final NetworkInterface ifc : filteredInterfaces)
            {
                builder
                    .append(lineSeparator())
                    .append("  - Name: ")
                    .append(ifc.getDisplayName())
                    .append(", addresses: ")
                    .append(ifc.getInterfaceAddresses())
                    .append(", multicast: ")
                    .append(ifc.supportsMulticast());
            }
        }

        return builder.toString();
    }

    private static StringBuilder toHex(final StringBuilder builder, final byte[] bytes)
    {
        for (final byte b : bytes)
        {
            builder.append((char)(HEX_TABLE[(b >> 4) & 0x0F]));
            builder.append((char)(HEX_TABLE[b & 0x0F]));
        }

        return builder;
    }

    static class Context
    {
        long tagId;
        int multicastTtl;
        InetSocketAddress remoteData;
        InetSocketAddress localData;
        InetSocketAddress remoteControl;
        InetSocketAddress localControl;
        String uriStr;
        String canonicalForm;
        NetworkInterface localInterface;
        ProtocolFamily protocolFamily;
        ChannelUri channelUri;
        boolean hasExplicitControl = false;
        boolean isMulticast = false;
        boolean hasTagId = false;
        boolean hasNoDistinguishingCharacteristic = false;

        Context uriStr(final String uri)
        {
            uriStr = uri;
            return this;
        }

        Context remoteDataAddress(final InetSocketAddress remoteData)
        {
            this.remoteData = remoteData;
            return this;
        }

        Context localDataAddress(final InetSocketAddress localData)
        {
            this.localData = localData;
            return this;
        }

        Context remoteControlAddress(final InetSocketAddress remoteControl)
        {
            this.remoteControl = remoteControl;
            return this;
        }

        Context localControlAddress(final InetSocketAddress localControl)
        {
            this.localControl = localControl;
            return this;
        }

        Context canonicalForm(final String canonicalForm)
        {
            this.canonicalForm = canonicalForm;
            return this;
        }

        Context localInterface(final NetworkInterface networkInterface)
        {
            this.localInterface = networkInterface;
            return this;
        }

        Context protocolFamily(final ProtocolFamily protocolFamily)
        {
            this.protocolFamily = protocolFamily;
            return this;
        }

        Context multicastTtl(final int multicastTtl)
        {
            this.multicastTtl = multicastTtl;
            return this;
        }

        Context tagId(final long tagId)
        {
            this.tagId = tagId;
            return this;
        }

        Context channelUri(final ChannelUri channelUri)
        {
            this.channelUri = channelUri;
            return this;
        }

        Context hasExplicitControl(final boolean hasExplicitControl)
        {
            this.hasExplicitControl = hasExplicitControl;
            return this;
        }

        Context isMulticast(final boolean isMulticast)
        {
            this.isMulticast = isMulticast;
            return this;
        }

        Context hasTagId(final boolean hasTagId)
        {
            this.hasTagId = hasTagId;
            return this;
        }

        Context hasNoDistinguishingCharacteristic(final boolean hasNoDistinguishingCharacteristic)
        {
            this.hasNoDistinguishingCharacteristic = hasNoDistinguishingCharacteristic;
            return this;
        }
    }
}
