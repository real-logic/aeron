/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.net.InetAddress.getByAddress;
import static uk.co.real_logic.aeron.common.NetworkUtil.filterBySubnet;
import static uk.co.real_logic.aeron.common.NetworkUtil.findAddressOnInterface;
import static uk.co.real_logic.aeron.common.Strings.isEmpty;

import java.net.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import uk.co.real_logic.aeron.common.UriUtil;
import uk.co.real_logic.aeron.common.uri.AeronUri;
import uk.co.real_logic.aeron.common.uri.InterfaceSearchAddress;
import uk.co.real_logic.aeron.driver.exceptions.InvalidChannelException;
import uk.co.real_logic.agrona.BitUtil;

/**
 * Encapsulation of UDP Channels
 * <p>
 * Format of URI:
 * <code>
 * udp://[interface[:port]@]ip:port
 * </code>
 */
public final class UdpChannel
{
    private static final String UDP_MEDIA_ID = "udp";

    private static final String REMOTE_KEY = "remote";
    private static final String LOCAL_KEY = "local";
    private static final String INTERFACE_KEY = "interface";
    private static final String GROUP_KEY = "group";

    private static final String[] UNICAST_KEYS = { LOCAL_KEY, REMOTE_KEY };
    private static final String[] MULTICAST_KEYS = { GROUP_KEY, INTERFACE_KEY };

    private final InetSocketAddress remoteData;
    private final InetSocketAddress localData;
    private final InetSocketAddress remoteControl;
    private final InetSocketAddress localControl;

    private final String uriStr;
    private final String canonicalForm;
    private final NetworkInterface localInterface;

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
            final AeronUri uri = parseIntoAeronUri(uriStr);

            validateConfiguration(uri);

            final Context context = new Context().uriStr(uriStr);

            if (isMulticast(uri))
            {
                final InetSocketAddress dataAddress = uri.getSocketAddress(GROUP_KEY);
                final byte[] addressAsBytes = dataAddress.getAddress().getAddress();

                validateDataAddress(addressAsBytes);

                addressAsBytes[addressAsBytes.length - 1]++;
                final InetSocketAddress controlAddress =
                    new InetSocketAddress(getByAddress(addressAsBytes), dataAddress.getPort());

                final InterfaceSearchAddress searchAddress =
                    uri.getInterfaceSearchAddress(INTERFACE_KEY, InterfaceSearchAddress.wildcard());

                final NetworkInterface localInterface = findInterface(searchAddress);
                final InetSocketAddress localAddress = resolveToAddressOfInterface(localInterface, searchAddress);

                context.localControlAddress(localAddress)
                       .remoteControlAddress(controlAddress)
                       .localDataAddress(localAddress)
                       .remoteDataAddress(dataAddress)
                       .localInterface(localInterface)
                       .canonicalForm(canonicalise(localAddress, dataAddress));
            }
            else
            {
                final InetSocketAddress remoteAddress = uri.getSocketAddress(REMOTE_KEY);
                final InetSocketAddress localAddress = uri.getSocketAddress(LOCAL_KEY, 0, new InetSocketAddress(0));

                context.remoteControlAddress(remoteAddress)
                       .remoteDataAddress(remoteAddress)
                       .localControlAddress(localAddress)
                       .localDataAddress(localAddress)
                       .canonicalForm(canonicalise(localAddress, remoteAddress));
            }

            return new UdpChannel(context);
        }
        catch (final Exception ex)
        {
            throw new InvalidChannelException(ex);
        }
    }

    private static void validateDataAddress(final byte[] addressAsBytes)
    {
        if (BitUtil.isEven(addressAsBytes[addressAsBytes.length - 1]))
        {
            throw new IllegalArgumentException("Multicast data address must be odd");
        }
    }

    private static boolean isMulticast(AeronUri uri)
    {
        return uri.containsKey(GROUP_KEY);
    }

    private static void validateConfiguration(AeronUri uri)
    {
        validateMedia(uri);
        validateUnicastXorMulticast(uri);
    }

    private static void validateMedia(AeronUri uri)
    {
        if (!UDP_MEDIA_ID.equals(uri.getMedia()))
        {
            throw new IllegalArgumentException("Udp channel only supports udp media: " + uri);
        }
    }

    private static void validateUnicastXorMulticast(AeronUri uri)
    {
        final boolean hasMulticastKeys = uri.containsAnyKey(MULTICAST_KEYS);
        final boolean hasUnicastKeys = uri.containsAnyKey(UNICAST_KEYS);

        if (!(hasMulticastKeys ^ hasUnicastKeys))
        {
            final String msg =
                "URI must contain either a unicast configuration (%s) or a multicast configuration (%s) not both";
            throw new IllegalArgumentException(
                format(msg, Arrays.toString(MULTICAST_KEYS), Arrays.toString(UNICAST_KEYS)));
        }
    }

    private static AeronUri parseIntoAeronUri(String uriStr) throws URISyntaxException, UnknownHostException
    {
        if (uriStr.startsWith("udp:"))
        {
            return parseUdpUriToAeronUri(uriStr);
        }
        else if (uriStr.startsWith("aeron:"))
        {
            return AeronUri.parse(uriStr);
        }

        throw new IllegalArgumentException("malformed channel URI: " + uriStr);
    }

    private static AeronUri parseUdpUriToAeronUri(String uriStr) throws URISyntaxException, UnknownHostException
    {
        final URI uri = new URI(uriStr);
        final String userInfo = uri.getUserInfo();
        final int uriPort = uri.getPort();
        final Map<String, String> params = UriUtil.parseQueryString(uri, new HashMap<>());

        if (uriPort < 0)
        {
            throw new IllegalArgumentException("Port must be specified");
        }

        final InetAddress hostAddress = InetAddress.getByName(uri.getHost());

        if (hostAddress.isMulticastAddress())
        {
            final String group = uri.getHost() + ":" + uriPort;
            final String inf = interfaceStringOf(userInfo, params.get("subnetPrefix"));

            return AeronUri.builder()
                .media(UDP_MEDIA_ID)
                .param(GROUP_KEY, group)
                .param(INTERFACE_KEY, inf)
                .newInstance();
        }
        else
        {
            final String remote = uri.getHost() + ":" + uriPort;
            final String local = userInfo;

            return AeronUri.builder()
                .media(UDP_MEDIA_ID)
                .param(REMOTE_KEY, remote)
                .param(LOCAL_KEY, local)
                .newInstance();
        }
    }

    private static String interfaceStringOf(String userInfo, String subnetPrefix)
    {
        if (isEmpty(userInfo))
        {
            return null;
        }

        if (isEmpty(subnetPrefix))
        {
            return userInfo;
        }

        return userInfo + "/" + subnetPrefix;
    }

    private static InetSocketAddress resolveToAddressOfInterface(
        NetworkInterface localInterface, InterfaceSearchAddress searchAddress)
    {
        final InetAddress interfaceAddress =
            findAddressOnInterface(
                localInterface, searchAddress.getInetAddress(), searchAddress.getSubnetPrefix());

        if (null == interfaceAddress)
        {
            throw new IllegalStateException();
        }

        return new InetSocketAddress(interfaceAddress, searchAddress.getPort());
    }

    private static NetworkInterface findInterface(InterfaceSearchAddress searchAddress)
        throws SocketException, UnknownHostException
    {
        final Collection<NetworkInterface> filteredIfcs =
            filterBySubnet(searchAddress.getInetAddress(), searchAddress.getSubnetPrefix());

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

    private UdpChannel(final Context context)
    {
        this.remoteData = context.remoteData;
        this.localData = context.localData;
        this.remoteControl = context.remoteControl;
        this.localControl = context.localControl;
        this.uriStr = context.uriStr;
        this.canonicalForm = context.canonicalForm;
        this.localInterface = context.localInterface;
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

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return canonicalForm;
    }

    /**
     * Return a string which is a canonical form of the channel suitable for use as a file or directory
     * name and also as a method of hashing, etc.
     * <p>
     * A canonical form:
     * - begins with the string "UDP-"
     * - has all hostnames converted to hexadecimal
     * - has all fields expanded out
     * - uses "-" as all field separators
     * <p>
     * The general format is:
     * UDP-interface-localPort-remoteAddress-remotePort
     *
     * @return canonical representation as a string
     */
    public static String canonicalise(final InetSocketAddress localData, final InetSocketAddress remoteData)
    {
        return String.format(
            "UDP-%1$s-%2$d-%3$s-%4$d",
            BitUtil.toHex(localData.getAddress().getAddress()),
            localData.getPort(),
            BitUtil.toHex(remoteData.getAddress().getAddress()),
            remoteData.getPort());
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
     * Local interface to be used by the channel
     *
     * @return {@link NetworkInterface} for the local interface used by the channel
     * @throws SocketException
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

    public ProtocolFamily protocolFamily()
    {
        InetAddress address = localData.getAddress();

        if (address instanceof Inet4Address)
        {
            return StandardProtocolFamily.INET;
        }
        else if (address instanceof Inet6Address)
        {
            return StandardProtocolFamily.INET6;
        }
        else
        {
            throw new IllegalStateException("Unknown ProtocolFamily");
        }
    }

    private static class Context
    {
        private InetSocketAddress remoteData;
        private InetSocketAddress localData;
        private InetSocketAddress remoteControl;
        private InetSocketAddress localControl;
        private String uriStr;
        private String canonicalForm;
        private NetworkInterface localInterface;

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
    }

    private static String errorNoMatchingInterfaces(
        final Collection<NetworkInterface> filteredIfcs, final InterfaceSearchAddress address)
        throws SocketException
    {
        final StringBuilder builder = new StringBuilder();
        builder.append("Unable to find multicast interface matching criteria: ")
               .append(address.getAddress())
               .append("/")
               .append(address.getSubnetPrefix());

        if (filteredIfcs.size() > 0)
        {
            builder.append(lineSeparator())
                   .append("  Candidates:");

            for (final NetworkInterface ifc : filteredIfcs)
            {
                builder.append(lineSeparator())
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
            builder.append("interface: ")
                   .append(localInterface.getDisplayName())
                   .append(", ");
        }

        builder.append("localData: ").append(localData)
               .append(", remoteData: ").append(remoteData);

        return builder.toString();
    }
}
