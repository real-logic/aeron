/*
 * Copyright 2014 Real Logic Ltd.
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

import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

/**
 * Collection of network specific utility functions
 */
public class NetworkUtil
{
    /**
     * Try to set the default multicast interface.
     *
     * If the {@link CommonContext#MULTICAST_DEFAULT_INTERFACE_PROP_NAME} system property is set,
     * try to find the interface by name and use it. If not set, then scan interfaces and pick one that is UP
     * and MULTICAST. Prefer non-loopback, but settle for loopback if nothing else.
     *
     * @return default multicast interface or null if could not be found
     */
    public static NetworkInterface determineDefaultMulticastInterface()
    {
        NetworkInterface savedIfc = null;

        try
        {
            final String ifcName = System.getProperty(CommonContext.MULTICAST_DEFAULT_INTERFACE_PROP_NAME);

            if (null != ifcName)
            {
                savedIfc = NetworkInterface.getByName(ifcName);
            }
            else
            {
                final Enumeration<NetworkInterface> ifcs = NetworkInterface.getNetworkInterfaces();

                while (ifcs.hasMoreElements())
                {
                    final NetworkInterface ifc = ifcs.nextElement();

                    // search for UP, MULTICAST interface. Preferring non-loopback. But settle for loopback. Break
                    // once we find one.
                    if (ifc.isUp() && ifc.supportsMulticast())
                    {
                        savedIfc = ifc;

                        if (ifc.isLoopback())
                        {
                            continue;
                        }

                        break;
                    }
                }
            }
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }

        if (null != savedIfc)
        {
            System.setProperty(CommonContext.MULTICAST_DEFAULT_INTERFACE_PROP_NAME, savedIfc.getName());
        }

        return savedIfc;
    }

    private static final int[] MASK_TABLE = { 128, 192, 224, 240, 248, 252, 254 };

    static int leadingBitMask(int prefixLength)
    {
        return MASK_TABLE[prefixLength - 1];
    }

    static boolean areEqual(byte[] a, byte[] b, int prefixLength)
    {
        if (a.length != b.length)
        {
            return false;
        }

        int currentLength = prefixLength;
        int index = 0;
        while (currentLength > 0 && index < a.length)
        {
            final int mask = (currentLength < 8) ? leadingBitMask(currentLength) : 0xFF;
            if ((a[index] & mask) != (b[index] & mask))
            {
                return false;
            }
            index++;
            currentLength -= 8;
        }

        return true;
    }

    public static NetworkInterface findByInetAddressAndMask(
        InetSocketAddress localAddress, int prefixLength)
        throws SocketException
    {
        final Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements())
        {
            final byte[] queryAddress = localAddress.getAddress().getAddress();

            final NetworkInterface ifc = interfaces.nextElement();
            final List<InterfaceAddress> interfaceAddresses = ifc.getInterfaceAddresses();

            for (final InterfaceAddress interfaceAddress : interfaceAddresses)
            {
                final byte[] candidateAddress = interfaceAddress.getAddress().getAddress();
                if (areEqual(candidateAddress, queryAddress, prefixLength))
                {
                    return ifc;
                }
            }
        }

        return null;
    }
}
