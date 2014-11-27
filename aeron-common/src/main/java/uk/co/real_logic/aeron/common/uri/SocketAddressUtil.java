package uk.co.real_logic.aeron.common.uri;

import static uk.co.real_logic.aeron.common.Strings.parseIntOrDefault;

import java.net.InetSocketAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SocketAddressUtil
{
    private static final Pattern BASIC_ADDRESS_PATTERN = Pattern.compile("([^:]+):([0-9]+)");
    private static final Pattern OPTIONAL_PORT_ADDRESS_PATTERN = Pattern.compile("([^:]+)(?::([0-9]+))?");

    /**
     * Utility for parsing socket addresses from a {@link CharSequence}.  Supports
     * hostname:port, ipV4Address:port and [ipV6Address]:port
     *
     * @param cs Input string
     * @return An InetSocketAddress parsed from the input.
     */
    public static InetSocketAddress parse(CharSequence cs)
    {
        if (null == cs)
        {
            throw new NullPointerException("Input string must not be null");
        }

        final Matcher matcher = BASIC_ADDRESS_PATTERN.matcher(cs);

        if (matcher.matches())
        {
            final String host = matcher.group(1);
            final int port = Integer.parseInt(matcher.group(2));

            return new InetSocketAddress(host, port);
        }
        else
        {
            throw new IllegalArgumentException("Invalid format: " + cs);
        }
    }

    public static InetSocketAddress parse(CharSequence cs, int defaultPort)
    {
        if (null == cs)
        {
            throw new NullPointerException("Input string must not be null");
        }

        final Matcher matcher = OPTIONAL_PORT_ADDRESS_PATTERN.matcher(cs);

        if (matcher.matches())
        {
            final String host = matcher.group(1);
            final int port = parseIntOrDefault(matcher.group(2), defaultPort);

            return new InetSocketAddress(host, port);
        }
        else
        {
            throw new IllegalArgumentException("Invalid format: " + cs);
        }
    }
}
