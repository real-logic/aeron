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
package io.aeron.driver;

import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.BufferUtil;

import java.nio.ByteBuffer;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

final class NetworkPublicationThreadLocals
{
    private final ByteBuffer heartbeatBuffer;
    private final DataHeaderFlyweight dataHeader;
    private final ByteBuffer setupBuffer;
    private final SetupFlyweight setupHeader;
    private final ByteBuffer rttMeasurementBuffer;
    private final RttMeasurementFlyweight rttMeasurementHeader;

    NetworkPublicationThreadLocals()
    {
        final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(CACHE_LINE_LENGTH * 4, CACHE_LINE_LENGTH);

        byteBuffer.limit(DataHeaderFlyweight.HEADER_LENGTH);
        heartbeatBuffer = byteBuffer.slice();
        dataHeader = new DataHeaderFlyweight(heartbeatBuffer);

        int position = CACHE_LINE_LENGTH;
        byteBuffer.limit(position + SetupFlyweight.HEADER_LENGTH).position(position);
        setupBuffer = byteBuffer.slice();
        setupHeader = new SetupFlyweight(setupBuffer);

        position += CACHE_LINE_LENGTH;
        byteBuffer.limit(position + RttMeasurementFlyweight.HEADER_LENGTH).position(position);
        rttMeasurementBuffer = byteBuffer.slice();
        rttMeasurementHeader = new RttMeasurementFlyweight(rttMeasurementBuffer);

        dataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(0);

        setupHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
            .frameLength(SetupFlyweight.HEADER_LENGTH);

        rttMeasurementHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_RTTM)
            .frameLength(RttMeasurementFlyweight.HEADER_LENGTH);
    }

    ByteBuffer heartbeatBuffer()
    {
        return heartbeatBuffer;
    }

    DataHeaderFlyweight heartbeatDataHeader()
    {
        return dataHeader;
    }

    ByteBuffer setupBuffer()
    {
        return setupBuffer;
    }

    SetupFlyweight setupHeader()
    {
        return setupHeader;
    }

    ByteBuffer rttMeasurementBuffer()
    {
        return rttMeasurementBuffer;
    }

    RttMeasurementFlyweight rttMeasurementHeader()
    {
        return rttMeasurementHeader;
    }
}
