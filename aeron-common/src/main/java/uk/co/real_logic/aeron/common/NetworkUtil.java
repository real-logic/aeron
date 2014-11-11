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
package uk.co.real_logic.aeron.common;

import static java.lang.Integer.compare;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.util.Collections.sort;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;

/**
 * Collection of network specific utility functions
 */
public class NetworkUtil
{
    /**
     * Try to set the default multicast interface.
     *
     * If the {@link CommonContext#MULTICAST_DEFAULT_INTERFACE_PROP_NAME} system property is set,
     * try to find the interface by name and use it. If not set, then scan interfaces and pick one that is UP
     * and MULTICAST. Prefer non-loopback, but settle for loopback if nothing else.
     *
     * @return default multicast interface or null if could not be found
     */
    public static NetworkInterface determineDefaultMulticastInterface()
    {
        NetworkInterface savedIfc = null;

        try
        {
            final String ifcName = System.getProperty(CommonContext.MULTICAST_DEFAULT_INTERFACE_PROP_NAME);

            if (null != ifcName)
            {
                savedIfc = NetworkInterface.getByName(ifcName);
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
            throw new RuntimeException(ex);
        }

        if (null != savedIfc)
        {
            System.setProperty(CommonContext.MULTICAST_DEFAULT_INTERFACE_PROP_NAME, savedIfc.getName());
        }

        return savedIfc;
    }

    public static Collection<NetworkInterface> filterBySubnet(
        NetworkInterfaceShim shim, InetAddress address, int subnetPrefix)
        throws SocketException
    {
        final Enumeration<NetworkInterface> ifcs = shim.getNetworkInterfaces();
        final List<FilterResult> filterResults = new ArrayList<>();
        final byte[] queryAddress = address.getAddress();

        while (ifcs.hasMoreElements())
        {
            final NetworkInterface ifc = ifcs.nextElement();
            final InterfaceAddress interfaceAddress = findAddressOnInterface(shim, ifc, queryAddress, subnetPrefix);

            if (null != interfaceAddress)
            {
                filterResults.add(new FilterResult(interfaceAddress, ifc));
            }
        }

        sort(filterResults);

        final List<NetworkInterface> results = new ArrayList<>();
        filterResults.forEach(filterResult -> results.add(filterResult.ifc));

        return results;
    }

    public static NetworkInterface findByInetAddressAndSubnetPrefix(
        InetSocketAddress localAddress, int prefixLength)
        throws SocketException
    {
        final Collection<NetworkInterface> ifcs =
            filterBySubnet(NetworkInterfaceShim.DEFAULT, localAddress.getAddress(), prefixLength);

        if (ifcs.size() == 0)
        {
            return null;
        }

        return ifcs.iterator().next();
    }

    public static InetAddress findAddressOnInterface(NetworkInterface ifc, InetAddress address, int subnetPrefix)
    {
        final InterfaceAddress interfaceAddress =
            findAddressOnInterface(NetworkInterfaceShim.DEFAULT, ifc, address.getAddress(), subnetPrefix);

        if (null == interfaceAddress)
        {
            return null;
        }

        return interfaceAddress.getAddress();
    }

    static InterfaceAddress findAddressOnInterface(
        NetworkInterfaceShim shim, NetworkInterface ifc, byte[] queryAddress, int prefixLength)
    {
        final List<InterfaceAddress> interfaceAddresses = shim.getInterfaceAddresses(ifc);

        for (final InterfaceAddress interfaceAddress : interfaceAddresses)
        {
            final byte[] candidateAddress = interfaceAddress.getAddress().getAddress();
            if (isMatchWithPrefix(candidateAddress, queryAddress, prefixLength))
            {
                return interfaceAddress;
            }
        }

        return null;
    }

    //
    // Byte matching and calculation.
    //

    static boolean isMatchWithPrefix(byte[] a, byte[] b, int prefixLength)
    {
        if (a.length != b.length)
        {
            return false;
        }

        if (a.length == 4)
        {
            final int mask = prefixLengthToIpV4Mask(prefixLength);
            return (toInt(a) & mask) == (toInt(b) & mask);
        }

        throw new IllegalArgumentException("How many bytes does an IP address have again?");
    }

    static int calculateMatchLength(final byte[] bs, int subnetPrefix)
    {
        if (bs.length == 4)
        {
            final int addressAsInt = toInt(bs);
            final int mask = prefixLengthToIpV4Mask(subnetPrefix);
            return 32 - numberOfTrailingZeros(addressAsInt & mask);
        }

        throw new IllegalArgumentException("How many bytes does an IP address have again?");
    }

    private static int prefixLengthToIpV4Mask(int subnetPrefix)
    {
        return 0xFFFFFFFF ^ ((1 << 32 - subnetPrefix) - 1);
    }

    // TODO: Should these be common?
    private static int toInt(byte[] b)
    {
        return ((b[3] & 0xFF)) + ((b[2] & 0xFF) << 8) + ((b[1] & 0xFF) << 16) + ((b[0]) << 24);
    }

    static long toLong(byte[] b)
    {
        return ((b[7] & 0xFFL)) + ((b[6] & 0xFFL) << 8) + ((b[5] & 0xFFL) << 16) + ((b[4] & 0xFFL) << 24) +
            ((b[3] & 0xFFL) << 32) + ((b[2] & 0xFFL) << 40) + ((b[1] & 0xFFL) << 48) + (((long) b[0]) << 56);
    }

    private static class FilterResult implements Comparable<FilterResult>
    {
        private final InterfaceAddress interfaceAddress;
        private final NetworkInterface ifc;

        public FilterResult(InterfaceAddress interfaceAddress, NetworkInterface ifc)
        {
            this.interfaceAddress = interfaceAddress;
            this.ifc = ifc;
        }

        @Override
        public int compareTo(FilterResult o)
        {
            return -compare(
                interfaceAddress.getNetworkPrefixLength(),
                o.interfaceAddress.getNetworkPrefixLength());
        }
    }
}
