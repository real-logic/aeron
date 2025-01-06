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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

import static io.aeron.archive.CatalogIndex.DEFAULT_INDEX_SIZE;
import static io.aeron.archive.CatalogIndex.NULL_VALUE;
import static org.junit.jupiter.api.Assertions.*;

class CatalogIndexTest
{
    private final CatalogIndex catalogIndex = new CatalogIndex();

    @Test
    void defaultIndexCapacityIsTenEntries()
    {
        final long[] emptyIndex = new long[20];

        assertArrayEquals(emptyIndex, catalogIndex.index());
    }

    @Test
    void sizeReturnsZeroForAnEmptyIndex()
    {
        assertEquals(0, catalogIndex.size());
    }

    @Test
    void addThrowsIllegalArgumentExceptionIfRecordingIdIsNegative()
    {
        assertThrows(IllegalArgumentException.class, () -> catalogIndex.add(-1, 0));
    }

    @Test
    void addThrowsIllegalArgumentExceptionIfRecordingOffsetIsNegative()
    {
        assertThrows(IllegalArgumentException.class, () -> catalogIndex.add(1024, Integer.MIN_VALUE));
    }

    @Test
    void addOneRecording()
    {
        final long recordingId = 3;
        final long recordingOffset = 100;

        catalogIndex.add(recordingId, recordingOffset);

        final long[] expected = new long[20];
        expected[0] = recordingId;
        expected[1] = recordingOffset;
        assertArrayEquals(expected, catalogIndex.index());
        assertEquals(1, catalogIndex.size());
    }

    @Test
    void addAppendsToTheEndOfTheIndex()
    {
        final long recordingId1 = 6;
        final long recordingOffset1 = 50;
        final long recordingId2 = 21;
        final long recordingOffset2 = 64;

        catalogIndex.add(recordingId1, recordingOffset1);
        catalogIndex.add(recordingId2, recordingOffset2);

        final long[] expected = new long[20];
        expected[0] = recordingId1;
        expected[1] = recordingOffset1;
        expected[2] = recordingId2;
        expected[3] = recordingOffset2;
        assertArrayEquals(expected, catalogIndex.index());
        assertEquals(2, catalogIndex.size());
    }

    @ParameterizedTest
    @ValueSource(longs = { 5, 100 })
    void addAppendsThrowsIllegalArgumentExceptionIfNewRecordingIdIsLessThanOrEqualToTheExistingRecordingId(
        final long recordingId2)
    {
        catalogIndex.add(100, 0);

        final IllegalArgumentException exception =
            assertThrows(IllegalArgumentException.class, () -> catalogIndex.add(recordingId2, 200));
        assertEquals("recordingId " + recordingId2 + " is less than or equal to the last recordingId 100",
            exception.getMessage());
    }

    @Test
    void addExpandsIndexWhenFull()
    {
        final long[] expectedIndex = new long[(DEFAULT_INDEX_SIZE + (DEFAULT_INDEX_SIZE >> 1)) << 1];

        int pos = 0;
        for (int i = 0; i < DEFAULT_INDEX_SIZE; i++, pos += 2)
        {
            expectedIndex[pos] = i;
            expectedIndex[pos + 1] = i;
            catalogIndex.add(i, i);
        }
        expectedIndex[pos] = Long.MAX_VALUE;
        expectedIndex[pos + 1] = Long.MAX_VALUE;
        catalogIndex.add(Long.MAX_VALUE, Long.MAX_VALUE);

        assertArrayEquals(expectedIndex, catalogIndex.index());
        assertEquals(DEFAULT_INDEX_SIZE + 1, catalogIndex.size());
    }

    @Test
    void getThrowsIllegalArgumentExceptionIfRecordingIdIsNegative()
    {
        assertThrows(IllegalArgumentException.class, () -> catalogIndex.recordingOffset(-100));
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 1, DEFAULT_INDEX_SIZE, DEFAULT_INDEX_SIZE << 1, 100, Long.MAX_VALUE })
    void getReturnsNullValueOnAnEmptyIndex(final long recordingId)
    {
        assertEquals(NULL_VALUE, catalogIndex.recordingOffset(recordingId));
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 199, 201, Long.MAX_VALUE })
    void getReturnsNullValueIfRecordingIdIsNotFoundSingleEntry(final long recordingId)
    {
        catalogIndex.add(200, 0);

        assertEquals(NULL_VALUE, catalogIndex.recordingOffset(recordingId));
    }

    @ParameterizedTest
    @ValueSource(longs = { 9, 101, 1_000_000_000, 4002662252L, Long.MAX_VALUE })
    void getReturnsNullValueIfRecordingIdIsNotFoundMultipleEntries(final long recordingId)
    {
        for (int i = 10; i < 100; i++)
        {
            catalogIndex.add(i, i);
        }

        assertEquals(NULL_VALUE, catalogIndex.recordingOffset(recordingId));
    }

    @ParameterizedTest
    @CsvSource(value = { "0,0", "1,1", "2,3", "100,777", "1000,2000", "1001,2002" })
    void getReturnsOffsetAssociatedWithRecordingId(final long recordingId, final long recordingOffset)
    {
        catalogIndex.add(0, 0);
        catalogIndex.add(1, 1);
        catalogIndex.add(2, 3);
        catalogIndex.add(100, 777);
        catalogIndex.add(1000, 2_000);
        catalogIndex.add(1001, 2_002);

        assertEquals(recordingOffset, catalogIndex.recordingOffset(recordingId));
    }

    @ParameterizedTest
    @ValueSource(longs = { 0, 100, Long.MAX_VALUE })
    void getSingleElement(final long recordingId)
    {
        catalogIndex.add(recordingId, recordingId * 3);

        assertEquals(recordingId * 3, catalogIndex.recordingOffset(recordingId));
    }

    @Test
    void getFirstElement()
    {
        catalogIndex.add(0, 10);
        catalogIndex.add(1, 20);
        catalogIndex.add(2, 30);

        assertEquals(10, catalogIndex.recordingOffset(0));
    }

    @Test
    void getLastElement()
    {
        catalogIndex.add(1, 10);
        catalogIndex.add(5, 20);
        catalogIndex.add(12, 30);

        assertEquals(30, catalogIndex.recordingOffset(12));
    }

    @Test
    void getLastElementWhenIndexIsFull()
    {
        final long[] values = new Random(8327434)
            .longs(0, Long.MAX_VALUE)
            .distinct()
            .limit(DEFAULT_INDEX_SIZE)
            .sorted()
            .toArray();

        for (int i = 0; i < DEFAULT_INDEX_SIZE; i++)
        {
            catalogIndex.add(values[i], i + 1);
        }

        final long lastValue = values[values.length - 1];
        assertEquals(DEFAULT_INDEX_SIZE, catalogIndex.recordingOffset(lastValue));
    }

    @Test
    void removeThrowsIllegalArgumentExceptionIfNegativeRecordingIdIsProvided()
    {
        assertThrows(IllegalArgumentException.class, () -> catalogIndex.remove(-1));
    }

    @Test
    void removeReturnsNullValueWhenIndexIsEmpty()
    {
        assertEquals(NULL_VALUE, catalogIndex.remove(1));
    }

    @Test
    void removeReturnsNullValueWhenNonExistingRecordingIdIsProvided()
    {
        catalogIndex.add(1, 0);
        catalogIndex.add(20, 10);

        assertEquals(NULL_VALUE, catalogIndex.remove(7));

        assertEquals(2, catalogIndex.size());
    }

    @Test
    void removeTheOnlyEntryFromTheIndex()
    {
        final long recordingId = 0;
        final long recordingOffset = 1024;
        catalogIndex.add(recordingId, recordingOffset);

        assertEquals(recordingOffset, catalogIndex.remove(recordingId));
        assertEquals(0, catalogIndex.size());

        assertEquals(NULL_VALUE, catalogIndex.recordingOffset(recordingId));
    }

    @Test
    void removeFirstElement()
    {
        catalogIndex.add(0, 500);
        catalogIndex.add(1, 1000);
        catalogIndex.add(2, 1500);

        assertEquals(500, catalogIndex.remove(0));
        assertEquals(2, catalogIndex.size());

        assertEquals(NULL_VALUE, catalogIndex.recordingOffset(0));
        assertEquals(1000, catalogIndex.recordingOffset(1));
        assertEquals(1500, catalogIndex.recordingOffset(2));
    }

    @Test
    void removeLastElement()
    {
        catalogIndex.add(0, 500);
        catalogIndex.add(1, 1000);
        catalogIndex.add(2, 1500);

        assertEquals(1500, catalogIndex.remove(2));
        assertEquals(2, catalogIndex.size());

        assertEquals(NULL_VALUE, catalogIndex.recordingOffset(2));
        assertEquals(500, catalogIndex.recordingOffset(0));
        assertEquals(1000, catalogIndex.recordingOffset(1));
    }

    @Test
    void removeMiddleElement()
    {
        catalogIndex.add(0, 500);
        catalogIndex.add(10, 1000);
        catalogIndex.add(20, 1500);
        catalogIndex.add(30, 7777);

        assertEquals(1000, catalogIndex.remove(10));
        assertEquals(3, catalogIndex.size());

        assertEquals(NULL_VALUE, catalogIndex.recordingOffset(10));
        assertEquals(500, catalogIndex.recordingOffset(0));
        assertEquals(1500, catalogIndex.recordingOffset(20));
        assertEquals(7777, catalogIndex.recordingOffset(30));
    }

    @Test
    void removeLastElementFromTheFullIndex()
    {
        for (int i = 1; i <= DEFAULT_INDEX_SIZE; i++)
        {
            catalogIndex.add(i, i);
        }

        assertEquals(DEFAULT_INDEX_SIZE, catalogIndex.remove(DEFAULT_INDEX_SIZE));
        assertEquals(DEFAULT_INDEX_SIZE - 1, catalogIndex.size());

        assertEquals(NULL_VALUE, catalogIndex.recordingOffset(DEFAULT_INDEX_SIZE));
    }
}
