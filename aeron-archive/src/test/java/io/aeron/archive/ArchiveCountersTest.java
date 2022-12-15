/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InOrder;

import static io.aeron.Aeron.NULL_VALUE;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.*;

class ArchiveCountersTest
{
    @Test
    void allocateCounterUsingAeronClientIdAsArchiveIdentifier()
    {
        final int typeId = 999;
        final String name = "<test counter>";
        final long archiveId = -1832178932131546L;
        final String expectedLabel = name + " - archiveId=" + archiveId;
        final Aeron aeron = mock(Aeron.class);
        final MutableDirectBuffer tempBuffer = new UnsafeBuffer(new byte[SIZE_OF_LONG + expectedLabel.length()]);
        final Counter counter = mock(Counter.class);
        when(aeron.clientId()).thenReturn(archiveId);
        when(aeron.addCounter(typeId, tempBuffer, 0, SIZE_OF_LONG, tempBuffer, SIZE_OF_LONG, expectedLabel.length()))
            .thenReturn(counter);

        final Counter result = ArchiveCounters.allocate(aeron, tempBuffer, typeId, name, aeron.clientId());

        assertSame(counter, result);
        final InOrder inOrder = inOrder(aeron);
        inOrder.verify(aeron).clientId();
        inOrder.verify(aeron).addCounter(anyInt(), any(), anyInt(), anyInt(), any(), anyInt(), anyInt());
        inOrder.verifyNoMoreInteractions();
        assertEquals(archiveId, tempBuffer.getLong(0));
        assertEquals(expectedLabel,
            tempBuffer.getStringWithoutLengthAscii(SIZE_OF_LONG, tempBuffer.capacity() - SIZE_OF_LONG));
    }

    @ParameterizedTest
    @CsvSource({ "5,8", "42,-10", "-19, 61312936129398123" })
    void findReturnsNullValueIfCounterNotFound(final int typeId, final long archiveId)
    {
        final CountersManager countersManager = new CountersManager(
            new UnsafeBuffer(new byte[2 * METADATA_LENGTH]), new UnsafeBuffer(new byte[2 * COUNTER_LENGTH]));
        assertEquals(1, countersManager.maxCounterId());
        countersManager.allocate(
            "test",
            42,
            (keyBuffer) ->
            {
                keyBuffer.putLong(0, 61312936129398123L);
            });

        assertEquals(NULL_VALUE, ArchiveCounters.find(countersManager, typeId, archiveId));
    }

    @Test
    void findReturnsFirstMatchingCounter()
    {
        final CountersManager countersManager = new CountersManager(
            new UnsafeBuffer(new byte[8 * METADATA_LENGTH]), new UnsafeBuffer(new byte[8 * COUNTER_LENGTH]));
        final int typeId = 7;
        final long archiveId = Long.MIN_VALUE / 13;
        countersManager.allocate(
            "test 1",
            typeId,
            (keyBuffer) -> {});
        countersManager.allocate(
            "test 2",
            typeId,
            (keyBuffer) -> keyBuffer.putLong(0, 42));
        countersManager.allocate(
            "test 3",
            21,
            (keyBuffer) -> keyBuffer.putLong(0, archiveId));
        final int counter4 = countersManager.allocate(
            "test 4",
            typeId,
            (keyBuffer) -> keyBuffer.putLong(0, archiveId));
        countersManager.allocate(
            "test 5",
            typeId,
            (keyBuffer) -> keyBuffer.putLong(0, archiveId));

        assertEquals(counter4, ArchiveCounters.find(countersManager, typeId, archiveId));
    }
}
