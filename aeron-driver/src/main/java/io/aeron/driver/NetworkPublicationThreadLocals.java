/*
 * Copyright 2016 Real Logic Ltd.
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
package io.aeron.driver;

import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.SetupFlyweight;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;

import java.nio.ByteBuffer;

public class NetworkPublicationThreadLocals
{
    private final ByteBuffer heartbeatBuffer;
    private final DataHeaderFlyweight dataHeader;
    private final ByteBuffer setupBuffer;
    private final SetupFlyweight setupHeader;

    public NetworkPublicationThreadLocals()
    {
        final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(128, BitUtil.CACHE_LINE_LENGTH);

        byteBuffer.limit(DataHeaderFlyweight.HEADER_LENGTH);
        heartbeatBuffer = byteBuffer.slice();
        dataHeader = new DataHeaderFlyweight(heartbeatBuffer);

        byteBuffer.limit(64 + SetupFlyweight.HEADER_LENGTH).position(64);
        setupBuffer = byteBuffer.slice();
        setupHeader = new SetupFlyweight(setupBuffer);

        dataHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .flags((byte)DataHeaderFlyweight.BEGIN_AND_END_FLAGS)
            .headerType(HeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(0);

        setupHeader
            .version(HeaderFlyweight.CURRENT_VERSION)
            .headerType(HeaderFlyweight.HDR_TYPE_SETUP)
            .frameLength(SetupFlyweight.HEADER_LENGTH);
    }

    public ByteBuffer heartbeatBuffer()
    {
        return heartbeatBuffer;
    }

    public DataHeaderFlyweight dataHeader()
    {
        return dataHeader;
    }

    public ByteBuffer setupBuffer()
    {
        return setupBuffer;
    }

    public SetupFlyweight setupHeader()
    {
        return setupHeader;
    }
}
