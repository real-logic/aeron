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
package io.aeron.test;

import java.net.InetAddress;
import java.net.ServerSocket;

public class NetworkTestingUtil
{
    /**
     * Return an error message if this address can't be bound, null if successful.
     *
     * @param address The address to attempt to bind to.
     * @return null if successful, error message otherwise.
     */
    @SuppressWarnings("try")
    public static String isBindAddressAvailable(final String address)
    {
        try (ServerSocket ignored = new ServerSocket(0, 1024, InetAddress.getByName(address)))
        {
            return null;
        }
        catch (final Exception ex)
        {
            return "Binding to " + address + " failed, error: " + ex.getMessage();
        }
    }
}
