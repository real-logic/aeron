/*
 * Copyright 2014-2025 Real Logic Limited.
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

import org.agrona.collections.IntHashSet;

import java.net.BindException;
import java.net.InetSocketAddress;

/**
 * Class for managing wildcard ports for UDP channel transports within a specific range.
 * <p>
 * Does not track per interface, but as a range over all interfaces.
 */
public class WildcardPortManager implements PortManager
{
    /**
     * Placeholder to represent an empty port range.
     */
    public static final int[] EMPTY_PORT_RANGE = new int[2];

    final IntHashSet portSet = new IntHashSet();
    final int highPort;
    final int lowPort;
    final boolean isOsWildcard;
    final boolean isSender;
    int nextPort;

    /**
     * Instantiate a wildcard port manager with the given port range.
     *
     * @param portRange for the port manager
     * @param isSender for this port manager
     */
    public WildcardPortManager(final int[] portRange, final boolean isSender)
    {
        this.lowPort = portRange[0];
        this.highPort = portRange[1];
        this.isOsWildcard = lowPort == highPort && 0 == lowPort;
        this.nextPort = lowPort;
        this.isSender = isSender;
    }

    /**
     * {@inheritDoc}
     */
    public InetSocketAddress getManagedPort(
        final UdpChannel udpChannel,
        final InetSocketAddress bindAddress) throws BindException
    {
        InetSocketAddress address = bindAddress;

        if (bindAddress.getPort() != 0)
        {
            portSet.add(bindAddress.getPort());
        }
        else if (!isOsWildcard)
        {
            // do not map if not a subscription and does not have a control address. We want to use an ephemeral port
            // for the control channel on publications.
            if (!isSender || udpChannel.hasExplicitControl())
            {
                address = new InetSocketAddress(bindAddress.getAddress(), allocateOpenPort());
            }
        }

        return address;
    }

    /**
     * {@inheritDoc}
     */
    public void freeManagedPort(final InetSocketAddress bindAddress)
    {
        if (bindAddress.getPort() != 0)
        {
            portSet.remove(bindAddress.getPort());
        }
    }

    /**
     * Parse a port range in the format "low high".
     *
     * @param value for the port range in string format.
     * @return port range as low index 0 and high index 1.
     */
    public static int[] parsePortRange(final String value)
    {
        if (null == value || value.isEmpty())
        {
            return EMPTY_PORT_RANGE;
        }

        final String[] ports = value.split(" +");
        if (ports.length != 2)
        {
            throw new IllegalArgumentException("port range \"" + value + "\" incorrect format");
        }

        final int[] portRange = new int[2];

        try
        {
            portRange[0] = Integer.parseUnsignedInt(ports[0]);
            portRange[1] = Integer.parseUnsignedInt(ports[1]);

            if (portRange[0] > 65535 || portRange[1] > 65535)
            {
                throw new NumberFormatException(value + ":port must be an integer value between 0 and 65535");
            }

            if (portRange[0] > portRange[1])
            {
                throw new NumberFormatException(value + ":low port value must be lower than high port value");
            }
        }
        catch (final NumberFormatException ex)
        {
            throw new IllegalArgumentException("invalid port value " + value, ex);
        }

        return portRange;
    }

    private int findOpenPort()
    {
        for (int i = nextPort; i <= highPort; i++)
        {
            if (!portSet.contains(i))
            {
                return i;
            }
        }

        for (int i = lowPort; i < nextPort; i++)
        {
            if (!portSet.contains(i))
            {
                return i;
            }
        }

        return 0;
    }

    private int allocateOpenPort() throws BindException
    {
        final int port = findOpenPort();

        if (0 == port)
        {
            throw new BindException("no available ports in range " + this.lowPort + " " + this.highPort);
        }

        nextPort = port + 1;
        if (nextPort > highPort)
        {
            nextPort = lowPort;
        }

        portSet.add(port);

        return port;
    }
}
