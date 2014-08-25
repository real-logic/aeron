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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either exprUdpess or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.aeron.common;

import java.net.NetworkInterface;
import java.util.Enumeration;

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
}
