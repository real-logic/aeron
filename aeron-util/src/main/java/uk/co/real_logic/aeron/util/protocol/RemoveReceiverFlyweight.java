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
package uk.co.real_logic.aeron.util.protocol;

import java.nio.ByteOrder;

/**
 * Control message for removing a receiver.
 *
 * <p>
 * 0                   1                   2                   3
 * 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 * |  Vers |S|E|H|R| Type (=0x00)  |   Frame Length (=data + 20)   |
 * +-------+-+-+-+-+---------------+-------------------------------+
 * |      Destination Length       |   Destination               ...
 * |                                                             ...
 * +---------------------------------------------------------------+
 */
public class RemoveReceiverFlyweight extends HeaderFlyweight
{
    private static final int DESTINATION_OFFSET = 0;

    /**
     * return destination field
     *
     * @return destination field
     */
    public String destination()
    {
        return stringGet(atomicBuffer, offset + DESTINATION_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    /**
     * set destination field
     *
     * @param destination field value
     * @return flyweight
     */
    public RemoveReceiverFlyweight destination(final String destination)
    {
        stringPut(atomicBuffer, offset + DESTINATION_OFFSET, destination, ByteOrder.LITTLE_ENDIAN);
        return this;
    }

}
