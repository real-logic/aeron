/*
 * Copyright 2014-2022 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver.media;

import io.aeron.driver.NameResolver;
import org.agrona.AsciiEncoding;
import org.agrona.Strings;

import java.net.InetAddress;
import java.net.InetSocketAddress;

class SocketAddressParser
{
    enum IpV4State
    {
        HOST, PORT
    }

    enum IpV6State
    {
        START_ADDR, HOST, SCOPE, END_ADDR, PORT
    }

    /**
     * Parse socket addresses from a {@link String}.
     * <p>
     * Supports hostname:port, ipV4Address:port, [ipV6Address]:port, and name.
     *
     * @param value          to be parsed for the socket address.
     * @param uriParamName   for the parse.
     * @param isReResolution for the parse.
     * @param nameResolver   to be used for resolving hostnames.
     * @return An {@link InetSocketAddress} for the parsed input.
     */
    static InetSocketAddress parse(
        final String value, final String uriParamName, final boolean isReResolution, final NameResolver nameResolver)
    {
        if (Strings.isEmpty(value))
        {
            throw new NullPointerException("input string must not be null or empty");
        }

        final String nameAndPort = nameResolver.lookup(value, uriParamName, isReResolution);
        InetSocketAddress address = tryParseIpV4(nameAndPort, uriParamName, isReResolution, nameResolver);

        if (null == address)
        {
            address = tryParseIpV6(nameAndPort, uriParamName, isReResolution, nameResolver);
        }

        if (null == address)
        {
            throw new IllegalArgumentException("invalid format: " + value);
        }

        return address;
    }

    private static InetSocketAddress tryParseIpV4(
        final String str, final String uriParamName, final boolean isReResolution, final NameResolver nameResolver)
    {
        IpV4State state = IpV4State.HOST;
        int separatorIndex = -1;
        final int length = str.length();

        for (int i = 0; i < length; i++)
        {
            final char c = str.charAt(i);
            switch (state)
            {
                case HOST:
                    if (':' == c)
                    {
                        separatorIndex = i;
                        state = IpV4State.PORT;
                    }
                    break;

                case PORT:
                    if (':' == c)
                    {
                        return null;
                    }
                    else if (c < '0' || '9' < c)
                    {
                        return null;
                    }
            }
        }

        if (-1 != separatorIndex && 1 < length - separatorIndex)
        {
            final String hostname = str.substring(0, separatorIndex);
            final int portIndex = separatorIndex + 1;
            final int port = AsciiEncoding.parseIntAscii(str, portIndex, length - portIndex);
            final InetAddress inetAddress = nameResolver.resolve(hostname, uriParamName, isReResolution);

            return null == inetAddress ?
                InetSocketAddress.createUnresolved(hostname, port) : new InetSocketAddress(inetAddress, port);
        }

        throw new IllegalArgumentException("address 'port' is required for ipv4: " + str);
    }

    private static InetSocketAddress tryParseIpV6(
        final String str, final String uriParamName, final boolean isReResolution, final NameResolver nameResolver)
    {
        IpV6State state = IpV6State.START_ADDR;
        int portIndex = -1;
        int scopeIndex = -1;
        final int length = str.length();

        for (int i = 0; i < length; i++)
        {
            final char c = str.charAt(i);

            switch (state)
            {
                case START_ADDR:
                    if ('[' == c)
                    {
                        state = IpV6State.HOST;
                    }
                    else
                    {
                        return null;
                    }
                    break;

                case HOST:
                    if (']' == c)
                    {
                        state = IpV6State.END_ADDR;
                    }
                    else if ('%' == c)
                    {
                        scopeIndex = i;
                        state = IpV6State.SCOPE;
                    }
                    else if (':' != c && (c < 'a' || 'f' < c) && (c < 'A' || 'F' < c) && (c < '0' || '9' < c))
                    {
                        return null;
                    }
                    break;

                case SCOPE:
                    if (']' == c)
                    {
                        state = IpV6State.END_ADDR;
                    }
                    else if ('_' != c && '.' != c && '~' != c && '-' != c &&
                        (c < 'a' || 'z' < c) && (c < 'A' || 'Z' < c) && (c < '0' || '9' < c))
                    {
                        return null;
                    }
                    break;

                case END_ADDR:
                    if (':' == c)
                    {
                        portIndex = i;
                        state = IpV6State.PORT;
                    }
                    else
                    {
                        return null;
                    }
                    break;

                case PORT:
                    if (':' == c)
                    {
                        return null;
                    }
                    else if (c < '0' || '9' < c)
                    {
                        return null;
                    }
            }
        }

        if (-1 != portIndex && 1 < length - portIndex)
        {
            final String hostname = str.substring(1, scopeIndex != -1 ? scopeIndex : portIndex - 1);
            portIndex++;
            final int port = AsciiEncoding.parseIntAscii(str, portIndex, length - portIndex);
            final InetAddress inetAddress = nameResolver.resolve(hostname, uriParamName, isReResolution);

            return null == inetAddress ?
                InetSocketAddress.createUnresolved(hostname, port) : new InetSocketAddress(inetAddress, port);
        }

        throw new IllegalArgumentException("address 'port' is required for ipv6: " + str);
    }
}
