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
package uk.co.real_logic.aeron.common;

import static java.lang.Boolean.compare;
import static java.lang.Integer.compare;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.sort;

import java.net.*;
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
     * Search for a list of network interfaces that match the specified address and subnet prefix.
     * The results will be ordered by the length of the subnet prefix
     * ({@link InterfaceAddress#getNetworkPrefixLength()}).  If no results match, then the collection
     * will be empty.
     *
     * @param address      to search for on the {@link NetworkInterface}s.
     * @param subnetPrefix to limit the search.
     * @return {@link NetworkInterface}s that match the supplied criteria, ordered by the length
     * of the subnet prefix. Empty if none match.
     * @throws SocketException Exception
     */
    public static Collection<NetworkInterface> filterBySubnet(final InetAddress address, final int subnetPrefix)
        throws SocketException
    {
        return filterBySubnet(NetworkInterfaceShim.DEFAULT, address, subnetPrefix);
    }

    static Collection<NetworkInterface> filterBySubnet(
        final NetworkInterfaceShim shim, final InetAddress address, final int subnetPrefix)
        throws SocketException
    {
        final List<FilterResult> filterResults = new ArrayList<>();
        final byte[] queryAddress = address.getAddress();

        final Enumeration<NetworkInterface> ifcs = shim.getNetworkInterfaces();
        while (ifcs.hasMoreElements())
        {
            final NetworkInterface ifc = ifcs.nextElement();
            final InterfaceAddress interfaceAddress = findAddressOnInterface(shim, ifc, queryAddress, subnetPrefix);

            if (null != interfaceAddress)
            {
                filterResults.add(new FilterResult(interfaceAddress, ifc, shim.isLoopback(ifc)));
            }
        }

        sort(filterResults);

        final List<NetworkInterface> results = new ArrayList<>();
        filterResults.forEach((filterResult) -> results.add(filterResult.ifc));

        return results;
    }

    public static InetAddress findAddressOnInterface(
        final NetworkInterface ifc, final InetAddress address, final int subnetPrefix)
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
        final NetworkInterfaceShim shim, final NetworkInterface ifc, final byte[] queryAddress, final int prefixLength)
    {
        InterfaceAddress foundInterfaceAddress = null;

        for (final InterfaceAddress interfaceAddress : shim.getInterfaceAddresses(ifc))
        {
            final byte[] candidateAddress = interfaceAddress.getAddress().getAddress();
            if (isMatchWithPrefix(candidateAddress, queryAddress, prefixLength))
            {
                foundInterfaceAddress = interfaceAddress;
                break;
            }
        }

        return foundInterfaceAddress;
    }

    //
    // Byte matching and calculation.
    //

    static boolean isMatchWithPrefix(final byte[] a, final byte[] b, final int prefixLength)
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
        else if (a.length == 16)
        {
            final long upperMask = prefixLengthToIpV6Mask(min(prefixLength, 64));
            final long lowerMask = prefixLengthToIpV6Mask(max(prefixLength - 64, 0));

            return
                (upperMask & toLong(a, 0)) == (upperMask & toLong(b, 0)) &&
                (lowerMask & toLong(b, 8)) == (lowerMask & toLong(b, 8));
        }

        throw new IllegalArgumentException("How many bytes does an IP address have again?");
    }

    private static int prefixLengthToIpV4Mask(final int subnetPrefix)
    {
        return 0 == subnetPrefix ? 0 : ~((1 << 32 - subnetPrefix) - 1);
    }

    private static long prefixLengthToIpV6Mask(final int subnetPrefix)
    {
        return 0 == subnetPrefix ? 0 : ~((1L << 64 - subnetPrefix) - 1);
    }

    // TODO: Should these be common?
    private static int toInt(final byte[] b)
    {
        return ((b[3] & 0xFF)) + ((b[2] & 0xFF) << 8) + ((b[1] & 0xFF) << 16) + ((b[0]) << 24);
    }

    static long toLong(final byte[] b, final int offset)
    {
        return ((b[offset + 7] & 0xFFL)) + ((b[offset + 6] & 0xFFL) << 8) + ((b[offset + 5] & 0xFFL) << 16) +
            ((b[offset + 4] & 0xFFL) << 24) + ((b[offset + 3] & 0xFFL) << 32) + ((b[offset + 2] & 0xFFL) << 40) +
            ((b[offset + 1] & 0xFFL) << 48) + (((long) b[offset]) << 56);
    }

    public static ProtocolFamily getProtocolFamily(InetAddress address)
    {
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

    private static class FilterResult implements Comparable<FilterResult>
    {
        private final InterfaceAddress interfaceAddress;
        private final NetworkInterface ifc;
        private final boolean isLoopback;

        public FilterResult(final InterfaceAddress interfaceAddress, final NetworkInterface ifc, final boolean isLoopback)
            throws SocketException
        {
            this.interfaceAddress = interfaceAddress;
            this.ifc = ifc;
            this.isLoopback = isLoopback;
        }

        @Override
        public int compareTo(final FilterResult o)
        {
            if (isLoopback == o.isLoopback)
            {
                return -compare(
                    interfaceAddress.getNetworkPrefixLength(),
                    o.interfaceAddress.getNetworkPrefixLength());
            }
            else
            {
                return compare(isLoopback, o.isLoopback);
            }
        }
    }
}
