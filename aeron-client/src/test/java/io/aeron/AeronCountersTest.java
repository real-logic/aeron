/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron;

import io.aeron.test.Tests;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import static org.junit.jupiter.api.Assertions.*;

class AeronCountersTest
{
    @Test
    void shouldNotHaveOverlappingCounterTypeIds()
    {
        final Int2ObjectHashMap<Field> fieldByTypeId = new Int2ObjectHashMap<>();
        final Int2ObjectHashMap<List<Field>> duplicates = new Int2ObjectHashMap<>();
        final Consumer<Field> duplicateChecker =
            (f) ->
            {
                try
                {
                    final int typeId = (Integer)f.get(null);
                    final Field field = fieldByTypeId.putIfAbsent(typeId, f);
                    if (null != field)
                    {
                        final List<Field> duplicatesForKey = duplicates.computeIfAbsent(
                            typeId, (s) -> new ArrayList<>());
                        if (!duplicatesForKey.contains(f))
                        {
                            duplicatesForKey.add(f);
                        }
                        duplicatesForKey.add(field);
                    }
                }
                catch (final IllegalAccessException e)
                {
                    throw new RuntimeException(e);
                }
            };

        Arrays.stream(AeronCounters.class.getFields())
            .filter((f) -> Modifier.isStatic(f.getModifiers()))
            .filter((f) -> f.getName().endsWith("_TYPE_ID"))
            .filter((f) -> Integer.TYPE.isAssignableFrom(f.getType()))
            .forEach(duplicateChecker);

        if (!duplicates.isEmpty())
        {
            fail("Duplicate typeIds: " + duplicates);
        }
    }

    @Test
    @Disabled
    void printLargestCounterId()
    {
        final ToIntFunction<Field> getValue = (field) ->
        {
            try
            {
                return (Integer)field.get(null);
            }
            catch (final IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
        };

        final OptionalInt maxValue = Arrays.stream(AeronCounters.class.getFields())
            .filter((f) -> Modifier.isStatic(f.getModifiers()))
            .filter((f) -> f.getName().endsWith("_TYPE_ID"))
            .filter((f) -> Integer.TYPE.isAssignableFrom(f.getType()))
            .mapToInt(getValue)
            .max();

        System.out.println(maxValue);
    }

    @ParameterizedTest
    @CsvSource({
        "1.42.1, 8165495befc07e997a7f2f7743beab9d3846b0a5, version=1.42.1 " +
            "commit=8165495befc07e997a7f2f7743beab9d3846b0a5",
        "1.43.0-SNAPSHOT, abc, version=1.43.0-SNAPSHOT commit=abc",
        "NIL, 12345678, version=NIL commit=12345678" })
    void shouldFormatVersionInfo(final String fullVersion, final String commitHash, final String expected)
    {
        assertEquals(expected, AeronCounters.formatVersionInfo(fullVersion, commitHash));
    }

    @ParameterizedTest
    @CsvSource({
        "xyz, 1234567890, version=xyz commit=1234567890",
        "1.43.0-SNAPSHOT, abc, version=1.43.0-SNAPSHOT commit=abc" })
    void shouldAppendVersionInfo(final String fullVersion, final String commitHash, final String formatted)
    {
        final String expected = " " + formatted;
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(32);
        final int offset = 5;
        buffer.setMemory(0, buffer.capacity(), (byte)-1);

        final int length = AeronCounters.appendVersionInfo(buffer, offset, fullVersion, commitHash);

        assertEquals(expected.length(), length);
        assertEquals(expected, buffer.getStringWithoutLengthAscii(offset, length));
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1 })
    void appendToLabelThrowsIllegalArgumentExceptionIfCounterIsNegative(final int counterId)
    {
        final IllegalArgumentException exception = assertThrowsExactly(
            IllegalArgumentException.class, () -> AeronCounters.appendToLabel(new UnsafeBuffer(), counterId, "test"));
        assertEquals("counter id " + counterId + " is negative", exception.getMessage());
    }

    @Test
    void appendToLabelThrowsNullPointerExceptionIfBufferIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class, () -> AeronCounters.appendToLabel(null, 5, "test"));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1_000_000, Integer.MAX_VALUE })
    void appendToLabelThrowsIllegalArgumentExceptionIfCounterIsOutOfRange(final int counterId)
    {
        final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(new byte[CountersReader.METADATA_LENGTH * 3]);

        final IllegalArgumentException exception = assertThrowsExactly(
            IllegalArgumentException.class, () -> AeronCounters.appendToLabel(metaDataBuffer, counterId, "test"));
        assertEquals("counter id " + counterId + " out of range: 0 - maxCounterId=2", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { CountersReader.RECORD_UNUSED, CountersReader.RECORD_RECLAIMED })
    void appendToLabelThrowsIllegalArgumentExceptionIfCounterIsInWrongState(final int state)
    {
        final UnsafeBuffer metaDataBuffer = new UnsafeBuffer(new byte[CountersReader.METADATA_LENGTH * 2]);
        final int counterId = 1;
        final int metaDataOffset = CountersReader.metaDataOffset(counterId);
        metaDataBuffer.putInt(metaDataOffset, state);

        final IllegalArgumentException exception = assertThrowsExactly(
            IllegalArgumentException.class, () -> AeronCounters.appendToLabel(metaDataBuffer, counterId, "test"));
        assertEquals("counter id 1 is not allocated, state: " + state, exception.getMessage());
    }

    @Test
    void appendToLabelShouldAddSuffix()
    {
        final CountersManager countersManager = new CountersManager(
            new UnsafeBuffer(new byte[CountersReader.METADATA_LENGTH]),
            new UnsafeBuffer(ByteBuffer.allocateDirect(CountersReader.COUNTER_LENGTH)),
            StandardCharsets.US_ASCII);
        final int counterId = countersManager.allocate("initial value: ");

        final int length = AeronCounters.appendToLabel(countersManager.metaDataBuffer(), counterId, "test");

        assertEquals(4, length);
        assertEquals("initial value: test", countersManager.getCounterLabel(counterId));
    }

    @Test
    void appendToLabelShouldAddAPortionOfSuffixUpToTheMaxLength()
    {
        final CountersManager countersManager = new CountersManager(
            new UnsafeBuffer(new byte[CountersReader.METADATA_LENGTH]),
            new UnsafeBuffer(ByteBuffer.allocateDirect(CountersReader.COUNTER_LENGTH)),
            StandardCharsets.US_ASCII);
        final String initialLabel = "this is a test counter";
        final int counterId = countersManager.allocate(initialLabel);
        final String hugeSuffix = Tests.generateStringWithSuffix(" - 42", "x", CountersReader.MAX_LABEL_LENGTH);

        final int length = AeronCounters.appendToLabel(countersManager.metaDataBuffer(), counterId, hugeSuffix);

        assertNotEquals(hugeSuffix.length(), length);
        assertEquals(CountersReader.MAX_LABEL_LENGTH - initialLabel.length(), length);
        assertEquals(initialLabel + hugeSuffix.substring(0, length), countersManager.getCounterLabel(counterId));
    }

    @Test
    void appendToLabelIsANoOpIfThereIsNoSpaceInTheLabel()
    {
        final CountersManager countersManager = new CountersManager(
            new UnsafeBuffer(new byte[CountersReader.METADATA_LENGTH]),
            new UnsafeBuffer(ByteBuffer.allocateDirect(CountersReader.COUNTER_LENGTH)),
            StandardCharsets.US_ASCII);
        final String label = Tests.generateStringWithSuffix("", "a", CountersReader.MAX_LABEL_LENGTH);
        final int counterId = countersManager.allocate(label);

        final int length = AeronCounters.appendToLabel(countersManager.metaDataBuffer(), counterId, "test");

        assertEquals(0, length);
        assertEquals(label, countersManager.getCounterLabel(counterId));
    }
}
