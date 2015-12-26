/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED;

public class AssemblyHeader extends Header
{
    private int frameLength;

    public AssemblyHeader reset(final Header base, final int msgLength)
    {
        positionBitsToShift(base.positionBitsToShift());
        initialTermId(base.initialTermId());
        offset(base.offset());
        buffer(base.buffer());
        frameLength = msgLength + DataHeaderFlyweight.HEADER_LENGTH;

        return this;
    }

    public int frameLength()
    {
        return frameLength;
    }

    public byte flags()
    {
        return (byte)(super.flags() | UNFRAGMENTED);
    }

    public int termOffset()
    {
        return offset() - (frameLength - super.frameLength());
    }
}
