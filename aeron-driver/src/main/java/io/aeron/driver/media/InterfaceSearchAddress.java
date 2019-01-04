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

import org.agrona.Strings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.agrona.Strings.parseIntOrDefault;

class InterfaceSearchAddress
{
    private static final Pattern IPV4_ADDRESS_PATTERN = Pattern.compile(
        "([^:/]+)(?::(?<port>[0-9]+))?(?:/(?<subnet>[0-9]+))?");
    private static final Pattern IPV6_ADDRESS_PATTERN = Pattern.compile(
        "\\[([0-9A-Fa-f:]+)](?::(?<port>[0-9]+))?(?:/(?<subnet>[0-9]+))?");

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

    static InterfaceSearchAddress parse(final String s) throws UnknownHostException
    {
        if (Strings.isEmpty(s))
        {
            throw new IllegalArgumentException("search address string is null or empty");
        }

        final Matcher matcher = getMatcher(s);

        final InetAddress hostAddress = InetAddress.getByName(matcher.group(1));
        final int defaultSubnetPrefix = hostAddress.getAddress().length * 8;
        final int port = parseIntOrDefault(matcher.group("port"), 0);
        final int subnetPrefix = parseIntOrDefault(matcher.group("subnet"), defaultSubnetPrefix);

        return new InterfaceSearchAddress(new InetSocketAddress(hostAddress, port), subnetPrefix);
    }

    static InterfaceSearchAddress wildcard()
    {
        return new InterfaceSearchAddress(new InetSocketAddress(0), 0);
    }

    private static Matcher getMatcher(final CharSequence cs)
    {
        final Matcher ipV4Matcher = IPV4_ADDRESS_PATTERN.matcher(cs);

        if (ipV4Matcher.matches())
        {
            return ipV4Matcher;
        }

        final Matcher ipV6Matcher = IPV6_ADDRESS_PATTERN.matcher(cs);

        if (ipV6Matcher.matches())
        {
            return ipV6Matcher;
        }

        throw new IllegalArgumentException("invalid search address: " + cs);
    }
}
