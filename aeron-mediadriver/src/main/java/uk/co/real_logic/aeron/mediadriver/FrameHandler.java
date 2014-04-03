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
package uk.co.real_logic.aeron.mediadriver;

import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.NakFlyweight;

import java.net.InetSocketAddress;

/**
 * Callback interface for various Frame types. For mediadriver, specifically, this is Data or Control.
 *
 * This interface should expand to handle various addressing models. But InetSocketAddress is exposed
 * at the moment.
 *
 * All methods have empty defaults, since not every implementation needs to handle every type of
 * frame.
 */
public interface FrameHandler
{
    /**
     * Handle a Data Frame.
     *
     * @param header of the Frame
     * @param srcAddr of the Frame
     */
    default void onDataFrame(final DataHeaderFlyweight header, final InetSocketAddress srcAddr)
    {

    }

    /**
     * Handle a Control Frame, such as a NAK, CONN, SM, etc.
     *
     * @param header of the Frame
     * @param srcAddr of the Frame
     */
    default void onControlFrame(final HeaderFlyweight header, final InetSocketAddress srcAddr)
    {

    }

    /**
     * Handle a Control Frame, such as a NAK, CONN, SM, etc.
     *
     * @param nak the nak Frame
     * @param srcAddr of the Frame
     */
    default void onNakFrame(final NakFlyweight nak, final InetSocketAddress srcAddr)
    {

    }

}
