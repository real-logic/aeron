/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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

import io.aeron.ErrorCode;
import io.aeron.driver.Configuration;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.driver.uri.AeronUri;
import io.aeron.driver.uri.InterfaceSearchAddress;
import org.agrona.BitUtil;

import java.net.*;
import java.util.Collection;

import static io.aeron.driver.media.NetworkUtil.filterBySubnet;
import static io.aeron.driver.media.NetworkUtil.findAddressOnInterface;
import static io.aeron.driver.media.NetworkUtil.getProtocolFamily;
import static java.lang.System.lineSeparator;
import static java.net.InetAddress.getByAddress;
import static org.agrona.BitUtil.toHex;

/**
 * Encapsulation of UDP Channels.
 *
 * Format of URI as in {@link AeronUri}.
 */
public final class UdpChannel
{
    public static final String UDP_MEDIA_ID = "udp";

    /**
     * The key for the interface or NIC to which this channel is bound.
     */
    public static final String INTERFACE_KEY = "interface";

    /**
     * The key for endpoint address that is the destination for the channel.
     */
    public static final String ENDPOINT_KEY = "endpoint";

    /**
     * The key for Time-To-Live (TTL) for the multicast datagrams being sent.
     */
    public static final String MULTICAST_TTL_KEY = "ttl";

    /**
     * The key for the control channel IP address and port for multi-destination-cast semantics.
     */
    public static final String CONTROL_KEY = "control";

    private final int multicastTtl;
    private final InetSocketAddress remoteData;
    private final InetSocketAddress localData;
    private final InetSocketAddress remoteControl;
    private final InetSocketAddress localControl;
    private final String uriStr;
    private final String canonicalForm;
    private final NetworkInterface localInterface;
    private final ProtocolFamily protocolFamily;
    private final AeronUri aeronUri;
    private final boolean hasExplicitControl;

    private UdpChannel(final Context context)
    {
        this.remoteData = context.remoteData;
        this.localData = context.localData;
        this.remoteControl = context.remoteControl;
        this.localControl = context.localControl;
        this.uriStr = context.uriStr;
        this.canonicalForm = context.canonicalForm;
        this.localInterface = context.localInterface;
        this.protocolFamily = context.protocolFamily;
        this.multicastTtl = context.multicastTtl;
        this.aeronUri = context.aeronUri;
        this.hasExplicitControl = context.hasExplicitControl;
    }

    /**
     * Parse URI and create channel
     *
     * @param uriStr to parse
     * @return created channel
     */
    public static UdpChannel parse(final String uriStr)
    {
        try
        {
            final AeronUri aeronUri = AeronUri.parse(uriStr);

            validateConfiguration(aeronUri);

            final Context context = new Context().uriStr(uriStr).aeronUri(aeronUri);

            InetSocketAddress endpointAddress = getEndpointAddress(aeronUri);
            final InetSocketAddress explicitControlAddress = getExplicitControlAddress(aeronUri);

            if (null == endpointAddress && null == explicitControlAddress)
            {
                throw new IllegalArgumentException(
                    "Aeron URIs for UDP must specify an endpoint address and/or a control address");
            }

            if (null != endpointAddress && endpointAddress.isUnresolved())
            {
                throw new UnknownHostException("could not resolve endpoint address: " + endpointAddress);
            }

            if (null != explicitControlAddress && explicitControlAddress.isUnresolved())
            {
                throw new UnknownHostException("could not resolve control address: " + explicitControlAddress);
            }

            if (null == endpointAddress)
            {
                // just control specified, a multi-destination-cast Publication, so wildcard the endpoint
                endpointAddress = new InetSocketAddress("0.0.0.0", 0);
            }

            if (endpointAddress.getAddress().isMulticastAddress())
            {
                final InetSocketAddress controlAddress = getMulticastControlAddress(endpointAddress);

                final InterfaceSearchAddress searchAddress = getInterfaceSearchAddress(aeronUri);

                context
                    .hasExplicitControl(false)
                    .localControlAddress(resolveToAddressOfInterface(findInterface(searchAddress), searchAddress))
                    .remoteControlAddress(controlAddress)
                    .localDataAddress(resolveToAddressOfInterface(findInterface(searchAddress), searchAddress))
                    .remoteDataAddress(endpointAddress)
                    .localInterface(findInterface(searchAddress))
                    .multicastTtl(getMulticastTtl(aeronUri))
                    .protocolFamily(getProtocolFamily(endpointAddress.getAddress()))
                    .canonicalForm(canonicalise(
                        resolveToAddressOfInterface(findInterface(searchAddress), searchAddress), endpointAddress));
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
                final InterfaceSearchAddress searchAddress = getInterfaceSearchAddress(aeronUri);
                final InetSocketAddress localAddress;
                if (searchAddress.getInetAddress().isAnyLocalAddress())
                {
                    localAddress = searchAddress.getAddress();
                }
                else
                {
                    localAddress = resolveToAddressOfInterface(findInterface(searchAddress), searchAddress);
                }

                context
                    .hasExplicitControl(false)
                    .remoteControlAddress(endpointAddress)
                    .remoteDataAddress(endpointAddress)
                    .localControlAddress(localAddress)
                    .localDataAddress(localAddress)
                    .protocolFamily(getProtocolFamily(endpointAddress.getAddress()))
                    .canonicalForm(canonicalise(localAddress, endpointAddress));
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
     *
     * A canonical form:
     * - begins with the string "UDP-"
     * - has all hostnames converted to hexadecimal
     * - has all fields expanded out
     * - uses "-" as all field separators
     *
     * The general format is:
     * UDP-interface-localPort-remoteAddress-remotePort
     *
     * @param localData  for the channel
     * @param remoteData for the channel
     * @return canonical representation as a string
     */
    public static String canonicalise(final InetSocketAddress localData, final InetSocketAddress remoteData)
    {
        return
            "UDP-" +
                toHex(localData.getAddress().getAddress()) +
                '-' +
                localData.getPort() +
                '-' +
                toHex(remoteData.getAddress().getAddress()) +
                '-' +
                remoteData.getPort();
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
     * Get the {@link AeronUri} for this channel.
     *
     * @return the {@link AeronUri} for this channel.
     */
    public AeronUri aeronUri()
    {
        return aeronUri;
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
     *
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
        return remoteData.getAddress().isMulticastAddress();
    }

    /**
     * Local interface to be used by the channel.
     *
     * @return {@link NetworkInterface} for the local interface used by the channel
     * @throws SocketException if an error occurs
     */
    public NetworkInterface localInterface() throws SocketException
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

    /**
     * Does the channel have an explicit control address as used with multi-destination-cast or not?
     *
     * @return does channel have an explicit control address or not?
     */
    public boolean hasExplicitControl()
    {
        return hasExplicitControl;
    }

    /**
     * Get the endpoint address from the URI.
     *
     * @param uri to check
     * @return endpoint address for URI
     */
    public static InetSocketAddress destinationAddress(final AeronUri uri)
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

    private static InterfaceSearchAddress getInterfaceSearchAddress(final AeronUri uri) throws UnknownHostException
    {
        final InterfaceSearchAddress interfaceSearchAddress;

        if (uri.containsKey(INTERFACE_KEY))
        {
            interfaceSearchAddress = uri.getInterfaceSearchAddress(INTERFACE_KEY, InterfaceSearchAddress.wildcard());
        }
        else
        {
            interfaceSearchAddress = InterfaceSearchAddress.wildcard();
        }

        return interfaceSearchAddress;
    }

    private static InetSocketAddress getEndpointAddress(final AeronUri uri) throws UnknownHostException
    {
        final InetSocketAddress endpointAddress;

        if (uri.containsKey(ENDPOINT_KEY))
        {
            endpointAddress = uri.getSocketAddress(ENDPOINT_KEY);
        }
        else
        {
            endpointAddress = null;
        }

        return endpointAddress;
    }

    private static int getMulticastTtl(final AeronUri uri)
    {
        final int ttl;

        if (uri.containsKey(MULTICAST_TTL_KEY))
        {
            ttl = Integer.parseInt(uri.get(MULTICAST_TTL_KEY));
        }
        else
        {
            ttl = Configuration.SOCKET_MULTICAST_TTL;
        }

        return ttl;
    }

    private static InetSocketAddress getExplicitControlAddress(final AeronUri uri) throws UnknownHostException
    {
        final InetSocketAddress controlAddress;

        if (uri.containsKey(CONTROL_KEY))
        {
            controlAddress = uri.getSocketAddress(CONTROL_KEY);
        }
        else
        {
            controlAddress = null;
        }

        return controlAddress;
    }

    private static void validateDataAddress(final byte[] addressAsBytes)
    {
        if (BitUtil.isEven(addressAsBytes[addressAsBytes.length - 1]))
        {
            throw new IllegalArgumentException("Multicast data address must be odd");
        }
    }

    private static void validateConfiguration(final AeronUri uri)
    {
        validateMedia(uri);
    }

    private static void validateMedia(final AeronUri uri)
    {
        if (!UDP_MEDIA_ID.equals(uri.media()))
        {
            throw new IllegalArgumentException("Udp channel only supports udp media: " + uri);
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
        throws SocketException, UnknownHostException
    {
        final Collection<NetworkInterface> filteredIfcs = filterBySubnet(
            searchAddress.getInetAddress(), searchAddress.getSubnetPrefix());

        // Results are ordered by prefix length, with loopback at the end.
        for (final NetworkInterface ifc : filteredIfcs)
        {
            if (ifc.supportsMulticast() || ifc.isLoopback())
            {
                return ifc;
            }
        }

        throw new IllegalArgumentException(errorNoMatchingInterfaces(filteredIfcs, searchAddress));
    }

    static class Context
    {
        private int multicastTtl;
        private InetSocketAddress remoteData;
        private InetSocketAddress localData;
        private InetSocketAddress remoteControl;
        private InetSocketAddress localControl;
        private String uriStr;
        private String canonicalForm;
        private NetworkInterface localInterface;
        private ProtocolFamily protocolFamily;
        private AeronUri aeronUri;
        private boolean hasExplicitControl;

        public Context uriStr(final String uri)
        {
            uriStr = uri;
            return this;
        }

        public Context remoteDataAddress(final InetSocketAddress remoteData)
        {
            this.remoteData = remoteData;
            return this;
        }

        public Context localDataAddress(final InetSocketAddress localData)
        {
            this.localData = localData;
            return this;
        }

        public Context remoteControlAddress(final InetSocketAddress remoteControl)
        {
            this.remoteControl = remoteControl;
            return this;
        }

        public Context localControlAddress(final InetSocketAddress localControl)
        {
            this.localControl = localControl;
            return this;
        }

        public Context canonicalForm(final String canonicalForm)
        {
            this.canonicalForm = canonicalForm;
            return this;
        }

        public Context localInterface(final NetworkInterface ifc)
        {
            this.localInterface = ifc;
            return this;
        }

        public Context protocolFamily(final ProtocolFamily protocolFamily)
        {
            this.protocolFamily = protocolFamily;
            return this;
        }

        public Context multicastTtl(final int multicastTtl)
        {
            this.multicastTtl = multicastTtl;
            return this;
        }

        public Context aeronUri(final AeronUri aeronUri)
        {
            this.aeronUri = aeronUri;
            return this;
        }

        public Context hasExplicitControl(final boolean hasExplicitControl)
        {
            this.hasExplicitControl = hasExplicitControl;
            return this;
        }
    }

    private static String errorNoMatchingInterfaces(
        final Collection<NetworkInterface> filteredIfcs,
        final InterfaceSearchAddress address)
        throws SocketException
    {
        final StringBuilder builder = new StringBuilder()
            .append("Unable to find multicast interface matching criteria: ")
            .append(address.getAddress())
            .append('/')
            .append(address.getSubnetPrefix());

        if (filteredIfcs.size() > 0)
        {
            builder.append(lineSeparator()).append("  Candidates:");

            for (final NetworkInterface ifc : filteredIfcs)
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
}
