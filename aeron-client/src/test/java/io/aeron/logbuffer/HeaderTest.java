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
package io.aeron.logbuffer;

import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HeaderTest
{
    @Test
    void constructorInitializedData()
    {
        final int initialTermId = 18;
        final BigDecimal context = BigDecimal.valueOf(Long.MAX_VALUE);
        final int positionBitsToShift = 2;
        final Header header = new Header(initialTermId, positionBitsToShift, context);

        assertEquals(initialTermId, header.initialTermId());
        assertEquals(positionBitsToShift, header.positionBitsToShift());
        assertEquals(context, header.context());
    }

    @ParameterizedTest
    @CsvSource({ "0,2,100,1024,5,1172", "42,16,13,4096,46,266272", "1,30,1024,1073741824,111,119185343488" })
    void positionCalculationTheEndOfTheMessageInTheLog(
        final int initialTermId,
        final int positionBitsToShift,
        final int frameLength,
        final int termOffset,
        final int termId,
        final long expectedPosition)
    {
        final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
        final Header header = new Header(initialTermId, positionBitsToShift);
        header.buffer(dataHeaderFlyweight);
        header.offset(0);
        dataHeaderFlyweight.wrap(new byte[64], 16, 32);
        dataHeaderFlyweight.frameLength(frameLength);
        dataHeaderFlyweight.termId(termId);
        dataHeaderFlyweight.termOffset(termOffset);

        assertEquals(expectedPosition, header.position());
    }

    @Test
    void offsetIsRelativeToTheBufferStart()
    {
        final Header header = new Header(42, 3, "xyz");

        assertEquals(0, header.offset());

        header.offset(142);

        assertEquals(142, header.offset());
    }

    @ParameterizedTest
    @CsvSource({ "103,0x3,0x1A,0x6,2080,-46234,333,5,909090909090909",
        "512,0x1,0xC,0x1,1073741824,42,-876,1543,-4632842384627834687" })
    void shouldReadDataFromTheBuffer(
        final int frameLength,
        final byte version,
        final byte flags,
        final short type,
        final int termOffset,
        final int sessionId,
        final int streamId,
        final int termId,
        final long reservedValue)
    {
        final byte[] array = new byte[100];
        final int offset = 16;
        final DataHeaderFlyweight dataHeaderFlyweight = new DataHeaderFlyweight();
        dataHeaderFlyweight.wrap(array, offset, 64);

        dataHeaderFlyweight.frameLength(frameLength)
            .version(version)
            .flags(flags)
            .headerType(type);

        dataHeaderFlyweight
            .termOffset(termOffset)
            .sessionId(sessionId)
            .streamId(streamId)
            .termId(termId)
            .reservedValue(reservedValue);


        final Header header = new Header(5, 22);
        header.buffer(new UnsafeBuffer(array));
        header.offset(offset);

        assertEquals(frameLength, header.frameLength());
        assertEquals(flags, header.flags());
        assertEquals(type, header.type());
        assertEquals(termOffset, header.termOffset());
        assertEquals(sessionId, header.sessionId());
        assertEquals(streamId, header.streamId());
        assertEquals(termId, header.termId());
        assertEquals(reservedValue, header.reservedValue());
    }

    @Test
    void shouldOverrideInitialTermId()
    {
        final int initialTermId = -178;
        final int newInitialTermId = 871;
        final Header header = new Header(initialTermId, 3);
        assertEquals(initialTermId, header.initialTermId());

        header.initialTermId(newInitialTermId);
        assertEquals(newInitialTermId, header.initialTermId());
    }

    @Test
    void shouldOverridePositionBitsToShift()
    {
        final int positionBitsToShift = -6;
        final int newPositionBitsToShift = 20;
        final Header header = new Header(42, positionBitsToShift);
        assertEquals(positionBitsToShift, header.positionBitsToShift());

        header.positionBitsToShift(newPositionBitsToShift);
        assertEquals(newPositionBitsToShift, header.positionBitsToShift());
    }
}
