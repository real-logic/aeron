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
package uk.co.real_logic.aeron.common.uri;

import static java.lang.Integer.parseInt;
import static uk.co.real_logic.aeron.common.Strings.parseIntOrDefault;

import java.net.InetSocketAddress;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SocketAddressUtil
{
    private static final Pattern IPV4_ADDRESS_PATTERN = Pattern.compile("([^:]+)(?::([0-9]+))?");
    private static final Pattern IPV6_ADDRESS_PATTERN = Pattern.compile(
        "\\[([0-9A-Fa-f:]+)(?:%[a-zA-Z0-9_.~-]+)?\\](?::([0-9]+))?");

    private static InetSocketAddress parse(final CharSequence cs, final BiFunction<String, String, InetSocketAddress> consumer)
    {
        if (null == cs)
        {
            throw new NullPointerException("Input string must not be null");
        }

        final Matcher ipV4Matcher = IPV4_ADDRESS_PATTERN.matcher(cs);

        if (ipV4Matcher.matches())
        {
            final String host = ipV4Matcher.group(1);
            final String portString = ipV4Matcher.group(2);

            return consumer.apply(host, portString);
        }

        final Matcher ipV6Matcher = IPV6_ADDRESS_PATTERN.matcher(cs);

        if (ipV6Matcher.matches())
        {
            final String host = ipV6Matcher.group(1);
            final String portString = ipV6Matcher.group(2);

            return consumer.apply(host, portString);
        }

        throw new IllegalArgumentException("Invalid format: " + cs);
    }

    /**
     * Utility for parsing socket addresses from a {@link CharSequence}.  Supports
     * hostname:port, ipV4Address:port and [ipV6Address]:port
     *
     * @param cs Input string
     * @return An InetSocketAddress parsed from the input.
     */
    public static InetSocketAddress parse(final CharSequence cs)
    {
        return parse(cs, (hostString, portString) ->
        {
            if (null == portString)
            {
                throw new IllegalArgumentException("The 'port' portion of the address is required");
            }

            return new InetSocketAddress(hostString, parseInt(portString));
        });
    }

    public static InetSocketAddress parse(final CharSequence cs, final int defaultPort)
    {
        return parse(cs, (hostString, portString) ->
        {
            final int port = parseIntOrDefault(portString, defaultPort);
            return new InetSocketAddress(hostString, port);
        });
    }
}
