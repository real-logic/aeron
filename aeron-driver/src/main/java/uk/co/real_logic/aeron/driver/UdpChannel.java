/*
 * Copyright 2014 Real Logic Ltd.
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

import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.aeron.common.NetworkUtil;
import uk.co.real_logic.aeron.common.UriUtil;
import uk.co.real_logic.aeron.driver.exceptions.InvalidChannelException;

import java.net.*;
import java.util.HashMap;
import java.util.Map;

import static java.net.InetAddress.getByAddress;

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
    private static final String MASK_KEY = "mask";
    private static final int LAST_MULTICAST_DIGIT = 3;
    private static final NetworkInterface DEFAULT_MULTICAST_INTERFACE = NetworkUtil.determineDefaultMulticastInterface();

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
            final URI uri = new URI(uriStr);
            final String userInfo = uri.getUserInfo();
            final int uriPort = uri.getPort();
            final Map<String, String> params = UriUtil.parseQueryString(uri, new HashMap<>());

            if (!"udp".equals(uri.getScheme()))
            {
                return malformedUri(uriStr);
            }

            final Context context = new Context()
                .uriStr(uriStr);

            final InetAddress hostAddress = InetAddress.getByName(uri.getHost());

            if (hostAddress.isMulticastAddress())
            {
                final byte[] addressAsBytes = hostAddress.getAddress();
                if (BitUtil.isEven(addressAsBytes[LAST_MULTICAST_DIGIT]))
                {
                    throw new IllegalArgumentException("Multicast data address must be odd");
                }

                addressAsBytes[LAST_MULTICAST_DIGIT]++;
                final InetSocketAddress controlAddress = new InetSocketAddress(getByAddress(addressAsBytes), uriPort);
                final InetSocketAddress dataAddress = new InetSocketAddress(hostAddress, uriPort);

                final InetSocketAddress localAddress = determineLocalAddressFromUserInfo(userInfo);

                NetworkInterface localInterface = findInterface(localAddress, params);

                if (null == localInterface)
                {
                    if (null == DEFAULT_MULTICAST_INTERFACE)
                    {
                        throw new IllegalArgumentException("Interface not specified and default not set");
                    }

                    localInterface = DEFAULT_MULTICAST_INTERFACE;
                }

                context.localControlAddress(localAddress)
                       .remoteControlAddress(controlAddress)
                       .localDataAddress(localAddress)
                       .remoteDataAddress(dataAddress)
                       .localInterface(localInterface)
                       .canonicalForm(canonicalise(localAddress, dataAddress));
            }
            else
            {
                if (uriPort == -1)
                {
                    return malformedUri(uriStr);
                }

                final InetSocketAddress remoteAddress = new InetSocketAddress(hostAddress, uriPort);
                final InetSocketAddress localAddress = determineLocalAddressFromUserInfo(userInfo);

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

    private static NetworkInterface findInterface(final InetSocketAddress localAddress, Map<String, String> params)
        throws SocketException
    {
        if (params.containsKey(MASK_KEY))
        {
            int mask = Integer.parseInt(params.get(MASK_KEY));

            NetworkInterface netInterface = NetworkUtil.findByInetAddressAndMask(localAddress, mask);

            return netInterface;
        }
        else
        {
            return NetworkInterface.getByInetAddress(localAddress.getAddress());
        }
    }

    private static UdpChannel malformedUri(final String uriStr)
    {
        throw new IllegalArgumentException("malformed channel URI: " + uriStr);
    }

    private static InetSocketAddress determineLocalAddressFromUserInfo(final String userInfo)
    {
        try
        {
            InetSocketAddress localAddress = new InetSocketAddress(0);
            if (null != userInfo)
            {
                final int colonIndex = userInfo.indexOf(":");
                if (-1 == colonIndex)
                {
                    localAddress = new InetSocketAddress(InetAddress.getByName(userInfo), 0);
                }
                else
                {
                    final InetAddress specifiedLocalHost = InetAddress.getByName(userInfo.substring(0, colonIndex));
                    final int localPort = Integer.parseInt(userInfo.substring(colonIndex + 1));
                    localAddress = new InetSocketAddress(specifiedLocalHost, localPort);
                }
            }

            return localAddress;
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
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
}
