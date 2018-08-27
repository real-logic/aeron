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

import org.agrona.BufferUtil;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;

import static java.lang.Boolean.compare;
import static java.lang.Integer.compare;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.sort;

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
     * @throws SocketException if an error occurs
     */
    public static NetworkInterface[] filterBySubnet(final InetAddress address, final int subnetPrefix)
        throws SocketException
    {
        return filterBySubnet(NetworkInterfaceShim.DEFAULT, address, subnetPrefix);
    }

    static NetworkInterface[] filterBySubnet(
        final NetworkInterfaceShim shim, final InetAddress address, final int subnetPrefix)
        throws SocketException
    {
        final ArrayList<FilterResult> filterResults = new ArrayList<>();
        final byte[] queryAddress = address.getAddress();

        final Enumeration<NetworkInterface> interfaces = shim.getNetworkInterfaces();
        while (interfaces.hasMoreElements())
        {
            final NetworkInterface networkInterface = interfaces.nextElement();
            final InterfaceAddress interfaceAddress = findAddressOnInterface(
                shim, networkInterface, queryAddress, subnetPrefix);

            if (null != interfaceAddress)
            {
                filterResults.add(new FilterResult(
                    interfaceAddress, networkInterface, shim.isLoopback(networkInterface)));
            }
        }

        sort(filterResults);

        final int size = filterResults.size();
        final NetworkInterface[] results = new NetworkInterface[size];
        for (int i = 0; i < size; i++)
        {
            results[i] = filterResults.get(i).networkInterface;
        }

        return results;
    }

    public static InetAddress findAddressOnInterface(
        final NetworkInterface networkInterface, final InetAddress address, final int subnetPrefix)
    {
        final InterfaceAddress interfaceAddress =
            findAddressOnInterface(NetworkInterfaceShim.DEFAULT, networkInterface, address.getAddress(), subnetPrefix);

        if (null == interfaceAddress)
        {
            return null;
        }

        return interfaceAddress.getAddress();
    }

    static InterfaceAddress findAddressOnInterface(
        final NetworkInterfaceShim shim,
        final NetworkInterface networkInterface,
        final byte[] queryAddress,
        final int prefixLength)
    {
        InterfaceAddress foundInterfaceAddress = null;

        for (final InterfaceAddress interfaceAddress : shim.getInterfaceAddresses(networkInterface))
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

    static boolean isMatchWithPrefix(final byte[] candidate, final byte[] expected, final int prefixLength)
    {
        if (candidate.length != expected.length)
        {
            return false;
        }

        if (candidate.length == 4)
        {
            final int mask = prefixLengthToIpV4Mask(prefixLength);

            return (toInt(candidate) & mask) == (toInt(expected) & mask);
        }
        else if (candidate.length == 16)
        {
            final long upperMask = prefixLengthToIpV6Mask(min(prefixLength, 64));
            final long lowerMask = prefixLengthToIpV6Mask(max(prefixLength - 64, 0));

            return
                (upperMask & toLong(candidate, 0)) == (upperMask & toLong(expected, 0)) &&
                    (lowerMask & toLong(candidate, 8)) == (lowerMask & toLong(expected, 8));
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

    private static int toInt(final byte[] b)
    {
        return ((b[3] & 0xFF)) + ((b[2] & 0xFF) << 8) + ((b[1] & 0xFF) << 16) + ((b[0]) << 24);
    }

    static long toLong(final byte[] b, final int offset)
    {
        return ((b[offset + 7] & 0xFFL)) +
            ((b[offset + 6] & 0xFFL) << 8) +
            ((b[offset + 5] & 0xFFL) << 16) +
            ((b[offset + 4] & 0xFFL) << 24) +
            ((b[offset + 3] & 0xFFL) << 32) +
            ((b[offset + 2] & 0xFFL) << 40) +
            ((b[offset + 1] & 0xFFL) << 48) +
            (((long)b[offset]) << 56);
    }

    public static ProtocolFamily getProtocolFamily(final InetAddress address)
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

    static class FilterResult implements Comparable<FilterResult>
    {
        private final InterfaceAddress interfaceAddress;
        private final NetworkInterface networkInterface;
        private final boolean isLoopback;

        FilterResult(
            final InterfaceAddress interfaceAddress,
            final NetworkInterface networkInterface,
            final boolean isLoopback)
        {
            this.interfaceAddress = interfaceAddress;
            this.networkInterface = networkInterface;
            this.isLoopback = isLoopback;
        }

        public int compareTo(final FilterResult other)
        {
            if (isLoopback == other.isLoopback)
            {
                return -compare(
                    interfaceAddress.getNetworkPrefixLength(),
                    other.interfaceAddress.getNetworkPrefixLength());
            }
            else
            {
                return compare(isLoopback, other.isLoopback);
            }
        }
    }

    /**
     * Allocate a direct {@link ByteBuffer} that is padded at the end with at least alignment bytes.
     *
     * @param capacity  for the buffer.
     * @param alignment for the buffer.
     * @return the direct {@link ByteBuffer}.
     */
    public static ByteBuffer allocateDirectAlignedAndPadded(final int capacity, final int alignment)
    {
        final ByteBuffer buffer = BufferUtil.allocateDirectAligned(capacity + alignment, alignment);

        buffer.limit(buffer.limit() - alignment);

        return buffer.slice();
    }
}
