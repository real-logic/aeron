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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either exprUdpess or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.BitUtil;
import uk.co.real_logic.aeron.common.event.EventCode;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.driver.exceptions.InvalidDestinationException;

import java.net.*;
import java.util.Enumeration;

import static java.net.InetAddress.getByAddress;

/**
 * Encapsulation of UDP destinations
 * <p>
 * Format of URI:
 * <code>
 * udp://[interface[:port]@]ip:port
 * </code>
 */
public class UdpDestination
{
    private static final int LAST_MULTICAST_DIGIT = 3;

    private final InetSocketAddress remoteData;
    private final InetSocketAddress localData;
    private final InetSocketAddress remoteControl;
    private final InetSocketAddress localControl;

    private final String uriStr;
    private final String canonicalRepresentation;
    private final long consistentHash;
    private final NetworkInterface localInterface;

    private static final NetworkInterface DEFAULT_MULTICAST_INTERFACE;

    // TODO: stop doing static initialisation in this class. Move this to its own NetworkUtil class
    static
    {
        NetworkInterface savedIfc = null;

        /*
         * Try to set the default multicast interface.
         * If the system property is set, try to find the interface by name and use it.
         * If not set, then scan interfaces and pick one that is UP and MULTICAST. Prefer non-loopback, but settle
         * for loopback if nothing else.
         */
        try
        {
            final String ifcName = System.getProperty(MediaDriver.MULTICAST_DEFAULT_INTERFACE_PROP_NAME);

            if (null != ifcName)
            {
                savedIfc = NetworkInterface.getByName(ifcName);

                if (null == savedIfc)
                {
                    EventLogger.log(EventCode.COULD_NOT_FIND_INTERFACE, ifcName);
                }
            }
            else
            {
                final Enumeration<NetworkInterface> ifcs = NetworkInterface.getNetworkInterfaces();

                while (ifcs.hasMoreElements())
                {
                    final NetworkInterface ifc = ifcs.nextElement();

                    // search for UP, MULTICAST interface. Preferring non-loopback. But settle for loopback. Break
                    // once we find one.
                    if (ifc.isUp() && ifc.supportsMulticast())
                    {
                        savedIfc = ifc;

                        if (ifc.isLoopback())
                        {
                            continue;
                        }

                        break;
                    }
                }
            }
        }
        catch (final Exception ex)
        {
            new EventLogger().logException(ex);
        }

        DEFAULT_MULTICAST_INTERFACE = savedIfc;

        if (null != savedIfc)
        {
            System.setProperty(MediaDriver.MULTICAST_DEFAULT_INTERFACE_PROP_NAME, savedIfc.getName());
        }
    }

    /**
     * Parse URI and create destination
     *
     * @param destinationUri to parse
     * @return created destination
     */
    public static UdpDestination parse(final String destinationUri)
    {
        try
        {
            final URI uri = new URI(destinationUri);
            final String userInfo = uri.getUserInfo();
            final int uriPort = uri.getPort();

            if (!"udp".equals(uri.getScheme()))
            {
                return malformedUri(destinationUri);
            }

            final Context context = new Context()
                .uriStr(destinationUri);

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

                NetworkInterface localInterface = NetworkInterface.getByInetAddress(localAddress.getAddress());

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
                       .canonicalRepresentation(generateCanonicalRepresentation(localAddress, dataAddress));
            }
            else
            {
                if (uriPort == -1)
                {
                    return malformedUri(destinationUri);
                }

                final InetSocketAddress remoteAddress = new InetSocketAddress(hostAddress, uriPort);
                final InetSocketAddress localAddress = determineLocalAddressFromUserInfo(userInfo);

                context.remoteControlAddress(remoteAddress)
                       .remoteDataAddress(remoteAddress)
                       .localControlAddress(localAddress)
                       .localDataAddress(localAddress)
                       .canonicalRepresentation(generateCanonicalRepresentation(localAddress, remoteAddress));
            }

            context.consistentHash(BitUtil.generateConsistentHash(context.canonicalRepresentation.getBytes()));

            return new UdpDestination(context);
        }
        catch (final Exception ex)
        {
            throw new InvalidDestinationException(ex);
        }
    }

    private static UdpDestination malformedUri(final String destinationUri)
    {
        throw new IllegalArgumentException("malformed destination URI: " + destinationUri);
    }

    private static InetSocketAddress determineLocalAddressFromUserInfo(final String userInfo) throws Exception
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

    private UdpDestination(final Context context)
    {
        this.remoteData = context.remoteData;
        this.localData = context.localData;
        this.remoteControl = context.remoteControl;
        this.localControl = context.localControl;
        this.uriStr = context.uriStr;
        this.consistentHash = context.consistentHash;
        this.canonicalRepresentation = context.canonicalRepresentation;
        this.localInterface = context.localInterface;
    }

    /**
     * Return consistent hash of destination information
     *
     * {@link BitUtil#generateConsistentHash(byte[])}
     *
     * @return consistent hash of destination information
     */
    public long consistentHash()
    {
        return consistentHash;
    }

    /**
     * Return the canonical representation of the destination
     *
     * {@link UdpDestination#generateCanonicalRepresentation(java.net.InetSocketAddress, java.net.InetSocketAddress)}
     *
     * @return canonical representation of destination
     */
    public String canonicalRepresentation()
    {
        return canonicalRepresentation;
    }

    /** {@inheritDoc} */
    public int hashCode()
    {
        return (int)consistentHash;
    }

    /** {@inheritDoc} */
    public boolean equals(Object obj)
    {
        if (null != obj && obj instanceof UdpDestination)
        {
            final UdpDestination rhs = (UdpDestination)obj;

            return rhs.localData.equals(this.localData) && rhs.remoteData.equals(this.remoteData);
        }

        return false;
    }

    /** {@inheritDoc} */
    public String toString()
    {
        return canonicalRepresentation;
    }

    /**
     * Return a string which is a canonical representation of the destination suitable for use as a file or directory
     * name and also as a method of hashing, etc.
     *
     * A canonical representation:
     * - begins with the string "UDP-"
     * - has all hostnames converted to hexadecimal
     * - has all fields expanded out
     * - uses "-" as all field separators
     *
     * The general format is:
     * UDP-interface-localPort-destinationAddress-destinationPort
     *
     * @return canonical representation as a string
     */
    public static String generateCanonicalRepresentation(final InetSocketAddress localData, final InetSocketAddress remoteData)
        throws Exception
    {
        return String.format("UDP-%1$s-%2$d-%3$s-%4$d",
                             BitUtil.toHex(localData.getAddress().getAddress()),
                             localData.getPort(),
                             BitUtil.toHex(remoteData.getAddress().getAddress()),
                             remoteData.getPort());
    }

    /**
     * Does destination represent a multicast or not
     *
     * @return does destination represent a multicast or not
     */
    public boolean isMulticast()
    {
        return remoteData.getAddress().isMulticastAddress();
    }

    /**
     * Local interface to be used by the destination
     *
     * @return {@link NetworkInterface} for the local interface used by the destination
     * @throws SocketException
     */
    public NetworkInterface localInterface() throws SocketException
    {
        return localInterface;
    }

    /**
     * Original URI of the destination URI.
     *
     * @return the original uri
     */
    public String originalUriAsString()
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
        private String canonicalRepresentation;
        private long consistentHash;
        private NetworkInterface localInterface;

        public Context uriStr(final String uri)
        {
            uriStr = uri;
            return this;
        }

        public Context consistentHash(final long hash)
        {
            consistentHash = hash;
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

        public Context canonicalRepresentation(final String canonicalRepresentation)
        {
            this.canonicalRepresentation = canonicalRepresentation;
            return this;
        }

        public Context localInterface(final NetworkInterface ifc)
        {
            this.localInterface = ifc;
            return this;
        }
    }
}
