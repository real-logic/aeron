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

import org.agrona.AsciiEncoding;

import java.net.InetSocketAddress;

class SocketAddressUtil
{
    /**
     * Utility for parsing socket addresses from a {@link CharSequence}. Supports
     * hostname:port, ipV4Address:port and [ipV6Address]:port
     *
     * @param cs to be parsed for the socket address.
     * @return An {@link InetSocketAddress} for the parsed input.
     */
    static InetSocketAddress parse(final CharSequence cs)
    {
        if (null == cs || cs.length() == 0)
        {
            throw new NullPointerException("Input string must not be null or empty");
        }

        InetSocketAddress address = tryParseIpV4(cs);

        if (null == address)
        {
            address = tryParseIpV6(cs);
        }

        if (null == address)
        {
            throw new IllegalArgumentException("Invalid format: " + cs);
        }

        return address;
    }

    enum IpV4State
    {
        HOST, PORT
    }

    enum IpV6State
    {
        START_ADDR, HOST, SCOPE, END_ADDR, PORT
    }

    private static InetSocketAddress tryParseIpV4(final CharSequence cs)
    {
        IpV4State state = IpV4State.HOST;
        int separatorIndex = -1;
        final int length = cs.length();

        for (int i = 0; i < length; i++)
        {
            final char c = cs.charAt(i);
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
            final String hostname = cs.subSequence(0, separatorIndex).toString();
            final int portIndex = separatorIndex + 1;
            final int port = AsciiEncoding.parseIntAscii(cs, portIndex, length - portIndex);

            return new InetSocketAddress(hostname, port);
        }

        throw new IllegalArgumentException("The 'port' portion of the address is required");
    }

    private static InetSocketAddress tryParseIpV6(final CharSequence cs)
    {
        IpV6State state = IpV6State.START_ADDR;
        int portIndex = -1;
        int scopeIndex = -1;
        final int length = cs.length();

        for (int i = 0; i < length; i++)
        {
            final char c = cs.charAt(i);

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
            final String hostname = cs.subSequence(1, scopeIndex != -1 ? scopeIndex : portIndex - 1).toString();
            portIndex++;
            final int port = AsciiEncoding.parseIntAscii(cs, portIndex, length - portIndex);

            return new InetSocketAddress(hostname, port);
        }

        throw new IllegalArgumentException("The 'port' portion of the address is required");
    }
}
