package uk.co.real_logic.aeron.common.uri;

import static uk.co.real_logic.aeron.common.Strings.parseIntOrDefault;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import uk.co.real_logic.aeron.common.Strings;

public class InterfaceSearchAddress
{
    private static final Pattern ADDRESS_PATTERN = Pattern.compile("([^:/]+)(?::(?<port>[0-9]+))?(?:/(?<subnet>[0-9]+))?");

    private final InetSocketAddress address;
    private final int subnetPrefix;

    public InterfaceSearchAddress(InetSocketAddress address, int subnetPrefix)
    {
        this.address = address;
        this.subnetPrefix = subnetPrefix;
    }

    public InetSocketAddress getAddress()
    {
        return address;
    }

    public InetAddress getInetAddress()
    {
        return address.getAddress();
    }

    public int getSubnetPrefix()
    {
        return subnetPrefix;
    }

    public int getPort()
    {
        return address.getPort();
    }

    public static InterfaceSearchAddress parse(String s) throws UnknownHostException
    {
        if (Strings.isEmpty(s))
        {
            throw new IllegalArgumentException("Search address string is null or empty");
        }

        final Matcher matcher = ADDRESS_PATTERN.matcher(s);

        if (!matcher.matches())
        {
            throw new IllegalArgumentException("Invalid search address: " + s);
        }

        final InetAddress hostAddress = InetAddress.getByName(matcher.group(1));
        final int defaultSubnetPrefix = hostAddress.getAddress().length * 8;
        final int port = parseIntOrDefault(matcher.group("port"), 0);
        final int subnetPrefix = parseIntOrDefault(matcher.group("subnet"), defaultSubnetPrefix);

        return new InterfaceSearchAddress(new InetSocketAddress(hostAddress, port), subnetPrefix);
    }

    public static InterfaceSearchAddress wildcard()
    {
        return new InterfaceSearchAddress(new InetSocketAddress(0), 0);
    }
}
