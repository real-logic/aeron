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

import uk.co.real_logic.aeron.util.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.util.protocol.DataHeaderFlyweight;
import uk.co.real_logic.aeron.util.protocol.NakFlyweight;
import uk.co.real_logic.aeron.util.protocol.StatusMessageFlyweight;

import java.net.InetSocketAddress;

/**
 * Callback interface for various Frame types. For media driver, specifically, this is Data or Control.
 *
 * Subclasses must explicitly handle all cases for clarity.
 */
public interface FrameHandler
{
    /**
     * Handle a Data Frame.
     *
     * @param header of the first Data Frame in the message (may be re-wrapped if needed)
     * @param buffer holding the data (always starts at 0 offset)
     * @param length of the Frame (may be longer than the header frame length)
     * @param srcAddress of the Frame
     */
    void onDataFrame(final DataHeaderFlyweight header, final AtomicBuffer buffer,
                     final long length, final InetSocketAddress srcAddress);

    /**
     * Handle a Status Message Frame
     *
     * @param header of the first Status Message Frame in the message (may be re-wrapped if needed)
     * @param buffer holding the NAK (always starts at 0 offset)
     * @param length of the Frame (may be longer than the header frame length)
     * @param srcAddress of the Frame
     */
    void onStatusMessageFrame(final StatusMessageFlyweight header, final AtomicBuffer buffer,
                              final long length, final InetSocketAddress srcAddress);

    /**
     * Handle a NAK Frame
     *
     * @param header the first NAK Frame in the message (may be re-wrapped if needed)
     * @param buffer holding the Status Message (always starts at 0 offset)
     * @param length of the Frame (may be longer than the header frame length)
     * @param srcAddress of the Frame
     */
    void onNakFrame(final NakFlyweight header, final AtomicBuffer buffer,
                    final long length, final InetSocketAddress srcAddress);
}
