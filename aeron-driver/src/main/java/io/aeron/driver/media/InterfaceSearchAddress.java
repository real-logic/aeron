/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import org.agrona.AsciiEncoding;
import org.agrona.Strings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

class InterfaceSearchAddress
{
    private static final InterfaceSearchAddress WILDCARD = new InterfaceSearchAddress(new InetSocketAddress(0), 0);
    private final InetSocketAddress address;
    private final int subnetPrefix;

    InterfaceSearchAddress(final InetSocketAddress address, final int subnetPrefix)
    {
        this.address = address;
        this.subnetPrefix = subnetPrefix;
    }

    InetSocketAddress getAddress()
    {
        return address;
    }

    InetAddress getInetAddress()
    {
        return address.getAddress();
    }

    int getSubnetPrefix()
    {
        return subnetPrefix;
    }

    int getPort()
    {
        return address.getPort();
    }

    static InterfaceSearchAddress wildcard()
    {
        return WILDCARD;
    }

    static InterfaceSearchAddress parse(final String s) throws UnknownHostException
    {
        if (Strings.isEmpty(s))
        {
            throw new IllegalArgumentException("search address string is null or empty");
        }

        int slashIndex = -1;
        int colonIndex = -1;
        int rightAngleBraceIndex = -1;

        for (int i = 0, length = s.length(); i < length; i++)
        {
            switch (s.charAt(i))
            {
                case '/':
                    slashIndex = i;
                    break;

                case ':':
                    colonIndex = i;
                    break;

                case ']':
                    rightAngleBraceIndex = i;
                    break;
            }
        }

        final String addressString = getAddress(s, slashIndex, colonIndex, rightAngleBraceIndex);
        final InetAddress hostAddress = InetAddress.getByName(addressString);
        final int port = getPort(s, slashIndex, colonIndex, rightAngleBraceIndex);
        final int defaultSubnetPrefix = hostAddress.getAddress().length * 8;
        final int subnetPrefix = getSubnet(s, slashIndex, defaultSubnetPrefix);

        return new InterfaceSearchAddress(new InetSocketAddress(hostAddress, port), subnetPrefix);
    }

    private static int getSubnet(final String s, final int slashIndex, final int defaultSubnetPrefix)
    {
        if (slashIndex < 0)
        {
            return defaultSubnetPrefix;
        }
        else if (s.length() - 1 == slashIndex)
        {
            throw new IllegalArgumentException("invalid subnet: " + s);
        }

        final int subnetStringBegin = slashIndex + 1;

        return AsciiEncoding.parseIntAscii(s, subnetStringBegin, s.length() - subnetStringBegin);
    }

    private static int getPort(
        final String s, final int slashIndex, final int colonIndex, final int rightAngleBraceIndex)
    {
        if (colonIndex < 0 || rightAngleBraceIndex > colonIndex)
        {
            return 0;
        }
        else if (s.length() - 1 == colonIndex)
        {
            throw new IllegalArgumentException("invalid port: " + s);
        }

        final int portStringBegin = colonIndex + 1;
        final int portStringEnd = slashIndex > 0 ? slashIndex : s.length();

        return AsciiEncoding.parseIntAscii(s, portStringBegin, portStringEnd - portStringBegin);
    }

    private static String getAddress(
        final String s, final int slashIndex, final int colonIndex, final int rightAngleBraceIndex)
    {
        int addressEnd = s.length();

        if (slashIndex >= 0)
        {
            addressEnd = slashIndex;
        }

        if (colonIndex >= 0 && colonIndex > rightAngleBraceIndex)
        {
            addressEnd = colonIndex;
        }

        return s.substring(0, addressEnd);
    }
}
