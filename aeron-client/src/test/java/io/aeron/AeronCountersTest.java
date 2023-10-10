/*
 * Copyright 2014-2023 Real Logic Limited.
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

import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Int2ObjectHashMap;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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
        "1.42.1, 8165495befc07e997a7f2f7743beab9d3846b0a5, version=1.42.1 commit=8165495b",
        "1.43.0-SNAPSHOT, abc, version=1.43.0-SNAPSHOT commit=abc",
        "NIL, 12345678, version=NIL commit=12345678" })
    void shouldFormatVersionInfo(final String fullVersion, final String commitHash, final String expected)
    {
        assertEquals(expected, AeronCounters.formatVersionInfo(fullVersion, commitHash));
    }

    @ParameterizedTest
    @CsvSource({
        "xyz, 1234567890, version=xyz commit=12345678",
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
}
