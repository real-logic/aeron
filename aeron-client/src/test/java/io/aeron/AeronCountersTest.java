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
package io.aeron;

import org.agrona.collections.Int2ObjectHashMap;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

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
}