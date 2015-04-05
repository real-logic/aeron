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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.aeron.common.protocol.DataHeaderFlyweight;

import java.net.InetSocketAddress;

@FunctionalInterface
public interface DataPacketHandler
{
    /**
     * Handle a Data Frame from the network.
     *
     * @param header of the first Data Frame in the packet (may be re-wrapped if needed)
     * @param buffer holding the data (always starts at 0 offset)
     * @param length of the packet (may be longer than the header frame length)
     * @param srcAddress of the packet
     * @return the number of bytes received.
     */
    int onDataPacket(DataHeaderFlyweight header, UnsafeBuffer buffer, int length, InetSocketAddress srcAddress);
}
