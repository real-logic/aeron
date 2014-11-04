/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.agrona.collections;


import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.Long.valueOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertThat;

public class Long2ObjectHashMapTest
{
    private final Long2ObjectHashMap<String> longToObjectMap = new Long2ObjectHashMap<>();

    @Test
    public void shouldDoPutAndThenGet()
    {
        final String value = "Seven";
        longToObjectMap.put(7, value);

        assertThat(longToObjectMap.get(7), is(value));
    }

    @Test
    public void shouldReplaceExistingValueForTheSameKey()
    {
        final long key = 7;
        final String value = "Seven";
        longToObjectMap.put(key, value);

        final String newValue = "New Seven";
        final String oldValue = longToObjectMap.put(key, newValue);

        assertThat(longToObjectMap.get(key), is(newValue));
        assertThat(oldValue, is(value));
        assertThat(valueOf(longToObjectMap.size()), is(valueOf(1)));
    }

    @Test
    public void shouldGrowWhenThresholdExceeded()
    {
        final double loadFactor = 0.5d;
        final Long2ObjectHashMap<String> map = new Long2ObjectHashMap<>(32, loadFactor);
        for (int i = 0; i < 16; i++)
        {
            map.put(i, Long.toString(i));
        }

        assertThat(valueOf(map.getResizeThreshold()), is(valueOf(16)));
        assertThat(valueOf(map.getCapacity()), is(valueOf(32)));
        assertThat(valueOf(map.size()), is(valueOf(16)));

        map.put(16, "16");

        assertThat(valueOf(map.getResizeThreshold()), is(valueOf(32)));
        assertThat(valueOf(map.getCapacity()), is(valueOf(64)));
        assertThat(valueOf(map.size()), is(valueOf(17)));

        assertThat(map.get(16), equalTo("16"));
        assertThat(loadFactor, closeTo(map.getLoadFactor(), 0.0));

    }

    @Test
    public void shouldHandleCollisionAndThenLinearProbe()
    {
        final double loadFactor = 0.5d;
        final Long2ObjectHashMap<String> map = new Long2ObjectHashMap<>(32, loadFactor);
        final long key = 7;
        final String value = "Seven";
        map.put(key, value);

        final long collisionKey = key + map.getCapacity();
        final String collisionValue = Long.toString(collisionKey);
        map.put(collisionKey, collisionValue);

        assertThat(map.get(key), is(value));
        assertThat(map.get(collisionKey), is(collisionValue));
        assertThat(loadFactor, closeTo(map.getLoadFactor(), 0.0));
    }

    @Test
    public void shouldClearCollection()
    {
        for (int i = 0; i < 15; i++)
        {
            longToObjectMap.put(i, Long.toString(i));
        }

        assertThat(valueOf(longToObjectMap.size()), is(valueOf(15)));
        assertThat(longToObjectMap.get(1), is("1"));

        longToObjectMap.clear();

        assertThat(valueOf(longToObjectMap.size()), is(valueOf(0)));
        Assert.assertNull(longToObjectMap.get(1));
    }

    @Test
    public void shouldCompactCollection()
    {
        final int totalItems = 50;
        for (int i = 0; i < totalItems; i++)
        {
            longToObjectMap.put(i, Long.toString(i));
        }

        for (int i = 0, limit = totalItems - 4; i < limit; i++)
        {
            longToObjectMap.remove(i);
        }

        final int capacityBeforeCompaction = longToObjectMap.getCapacity();
        longToObjectMap.compact();

        assertThat(valueOf(longToObjectMap.getCapacity()), lessThan(valueOf(capacityBeforeCompaction)));
    }

    @Test
    public void shouldContainValue()
    {
        final long key = 7;
        final String value = "Seven";

        longToObjectMap.put(key, value);

        Assert.assertTrue(longToObjectMap.containsValue(value));
        Assert.assertFalse(longToObjectMap.containsValue("NoKey"));
    }

    @Test
    public void shouldContainKey()
    {
        final long key = 7;
        final String value = "Seven";

        longToObjectMap.put(key, value);

        Assert.assertTrue(longToObjectMap.containsKey(key));
        Assert.assertFalse(longToObjectMap.containsKey(0));
    }

    @Test
    public void shouldRemoveEntry()
    {
        final long key = 7;
        final String value = "Seven";

        longToObjectMap.put(key, value);

        Assert.assertTrue(longToObjectMap.containsKey(key));

        longToObjectMap.remove(key);

        Assert.assertFalse(longToObjectMap.containsKey(key));
    }

    @Test
    public void shouldRemoveEntryAndCompactCollisionChain()
    {
        final long key = 12;
        final String value = "12";

        longToObjectMap.put(key, value);
        longToObjectMap.put(13, "13");

        final long collisionKey = key + longToObjectMap.getCapacity();
        final String collisionValue = Long.toString(collisionKey);

        longToObjectMap.put(collisionKey, collisionValue);
        longToObjectMap.put(14, "14");

        assertThat(longToObjectMap.remove(key), is(value));
    }

    @Test
    public void shouldIterateValues()
    {
        final Collection<String> initialSet = new HashSet<>();

        for (int i = 0; i < 11; i++)
        {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add(value);
        }

        final Collection<String> copyToSet = longToObjectMap.values().stream().collect(Collectors.toSet());

        assertThat(copyToSet, is(initialSet));
    }

    @Test
    public void shouldIterateKeysGettingLongAsPrimitive()
    {
        final Collection<Long> initialSet = new HashSet<>();

        for (int i = 0; i < 11; i++)
        {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add(valueOf(i));
        }

        final Collection<Long> copyToSet = new HashSet<>();

        for (final Long2ObjectHashMap.KeyIterator iter = longToObjectMap.keySet().iterator(); iter.hasNext();)
        {
            copyToSet.add(valueOf(iter.nextLong()));
        }

        assertThat(copyToSet, is(initialSet));
    }

    @Test
    public void shouldIterateKeys()
    {
        final Collection<Long> initialSet = new HashSet<>();

        for (int i = 0; i < 11; i++)
        {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add(valueOf(i));
        }

        final Collection<Long> copyToSet = longToObjectMap.keySet().stream().collect(Collectors.toSet());

        assertThat(copyToSet, is(initialSet));
    }

    @Test
    public void shouldIterateAndHandleRemove()
    {
        final Collection<Long> initialSet = new HashSet<>();

        final int count = 11;
        for (int i = 0; i < count; i++)
        {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
            initialSet.add(valueOf(i));
        }

        final Collection<Long> copyOfSet = new HashSet<>();

        int i = 0;
        for (final Iterator<Long> iter = longToObjectMap.keySet().iterator(); iter.hasNext();)
        {
            final Long item = iter.next();
            if (i++ == 7)
            {
                iter.remove();
            }
            else
            {
                copyOfSet.add(item);
            }
        }

        final int reducedSetSize = count - 1;

        assertThat(valueOf(initialSet.size()), is(valueOf(count)));
        assertThat(valueOf(longToObjectMap.size()), is(valueOf(reducedSetSize)));
        assertThat(valueOf(copyOfSet.size()), is(valueOf(reducedSetSize)));
    }

    @Test
    public void shouldIterateEntries()
    {
        final int count = 11;
        for (int i = 0; i < count; i++)
        {
            final String value = Long.toString(i);
            longToObjectMap.put(i, value);
        }

        final String testValue = "Wibble";
        for (final Map.Entry<Long, String> entry : longToObjectMap.entrySet())
        {
            assertThat(entry.getKey(), equalTo(valueOf(entry.getValue())));

            if (entry.getKey().intValue() == 7)
            {
                entry.setValue(testValue);
            }
        }

        assertThat(longToObjectMap.get(7), equalTo(testValue));
    }

    @Test
    public void shouldGenerateStringRepresentation()
    {
        final int[] testEntries = {3, 1, 19, 7, 11, 12, 7};

        for (final int testEntry : testEntries)
        {
            longToObjectMap.put(testEntry, String.valueOf(testEntry));
        }

        final String mapAsAString = "{7=7, 12=12, 19=19, 3=3, 11=11, 1=1}";
        assertThat(longToObjectMap.toString(), equalTo(mapAsAString));
    }
}

