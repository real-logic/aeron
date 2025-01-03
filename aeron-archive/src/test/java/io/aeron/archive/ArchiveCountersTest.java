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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.Counter;
import io.aeron.test.Tests;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import static io.aeron.Aeron.NULL_VALUE;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.COUNTER_LENGTH;
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
        final MutableDirectBuffer tempBuffer = new UnsafeBuffer(new byte[200]);
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
        assertEquals(expectedLabel, tempBuffer.getStringWithoutLengthAscii(SIZE_OF_LONG, expectedLabel.length()));
    }

    @Test
    void allocateErrorCounter()
    {
        final long archiveId = 24623864;
        final String expectedLabel = "Archive Errors - archiveId=" + archiveId + " " +
            AeronCounters.formatVersionInfo(ArchiveVersion.VERSION, ArchiveVersion.GIT_SHA);
        final Aeron aeron = mock(Aeron.class);
        final MutableDirectBuffer tempBuffer = new UnsafeBuffer(new byte[200]);
        final Counter counter = mock(Counter.class);
        when(aeron.clientId()).thenReturn(archiveId);
        when(aeron.addCounter(
            AeronCounters.ARCHIVE_ERROR_COUNT_TYPE_ID,
            tempBuffer,
            0,
            SIZE_OF_LONG,
            tempBuffer,
            SIZE_OF_LONG,
            expectedLabel.length()))
            .thenReturn(counter);

        final Counter result = ArchiveCounters.allocateErrorCounter(aeron, tempBuffer, aeron.clientId());

        assertSame(counter, result);
        final InOrder inOrder = inOrder(aeron);
        inOrder.verify(aeron).clientId();
        inOrder.verify(aeron).addCounter(anyInt(), any(), anyInt(), anyInt(), any(), anyInt(), anyInt());
        inOrder.verifyNoMoreInteractions();
        assertEquals(archiveId, tempBuffer.getLong(0));
        assertEquals(expectedLabel, tempBuffer.getStringWithoutLengthAscii(SIZE_OF_LONG, expectedLabel.length()));
    }

    @ParameterizedTest
    @CsvSource({ "5,8", "42,-10", "-19, 61312936129398123" })
    void findReturnsNullValueIfCounterNotFound(final int typeId, final long archiveId)
    {
        final CountersManager countersManager = Tests.newCountersManager(2 * COUNTER_LENGTH);
        assertEquals(1, countersManager.maxCounterId());
        countersManager.allocate(
            "test",
            42,
            (keyBuffer) -> keyBuffer.putLong(0, 61312936129398123L));

        assertEquals(NULL_VALUE, ArchiveCounters.find(countersManager, typeId, archiveId));
    }

    @Test
    void findReturnsFirstMatchingCounter()
    {
        final CountersManager countersManager = Tests.newCountersManager(8 * COUNTER_LENGTH);
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

    @ParameterizedTest
    @ValueSource(longs = { Long.MIN_VALUE, Long.MAX_VALUE, 0, -1, 56436747823L, -1235127312317278312L })
    void lengthOfArchiveIdLabelCountsNumberOfAsciiDigitsRequiredToEncodeArchiveId(final long archiveId)
    {
        final int archiveIdLength = String.valueOf(archiveId).length();

        assertEquals(
            ArchiveCounters.ARCHIVE_ID_LABEL_PREFIX.length() + archiveIdLength,
            ArchiveCounters.lengthOfArchiveIdLabel(archiveId));
    }

    @Test
    void appendArchiveIdLabel()
    {
        final int offset = 13;
        final long archiveId = -23462384L;
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[100]);

        final int length = ArchiveCounters.appendArchiveIdLabel(buffer, offset, archiveId);

        assertEquals(ArchiveCounters.lengthOfArchiveIdLabel(archiveId), length);
        final int prefixLength = ArchiveCounters.ARCHIVE_ID_LABEL_PREFIX.length();
        assertEquals(
            ArchiveCounters.ARCHIVE_ID_LABEL_PREFIX,
            buffer.getStringWithoutLengthAscii(offset, prefixLength));
        assertEquals(archiveId, buffer.parseLongAscii(offset + prefixLength, length - prefixLength));
    }
}
