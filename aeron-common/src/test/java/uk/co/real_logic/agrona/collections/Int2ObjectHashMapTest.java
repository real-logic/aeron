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

import static java.lang.Integer.valueOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.number.IsCloseTo.closeTo;
import static org.hamcrest.number.OrderingComparison.lessThan;
import static org.junit.Assert.assertThat;

public class Int2ObjectHashMapTest
{
    private final Int2ObjectHashMap<String> intToObjectMap = new Int2ObjectHashMap<>();

    @Test
    public void shouldDoPutAndThenGet()
    {
        final String value = "Seven";
        intToObjectMap.put(7, value);

        assertThat(intToObjectMap.get(7), is(value));
    }

    @Test
    public void shouldReplaceExistingValueForTheSameKey()
    {
        final int key = 7;
        final String value = "Seven";
        intToObjectMap.put(key, value);

        final String newValue = "New Seven";
        final String oldValue = intToObjectMap.put(key, newValue);

        assertThat(intToObjectMap.get(key), is(newValue));
        assertThat(oldValue, is(value));
        assertThat(valueOf(intToObjectMap.size()), is(valueOf(1)));
    }

    @Test
    public void shouldGrowWhenThresholdExceeded()
    {
        final double loadFactor = 0.5d;
        final Int2ObjectHashMap<String> map = new Int2ObjectHashMap<>(32, loadFactor);
        for (int i = 0; i < 16; i++)
        {
            map.put(i, Integer.toString(i));
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
        final Int2ObjectHashMap<String> map = new Int2ObjectHashMap<>(32, loadFactor);
        final int key = 7;
        final String value = "Seven";
        map.put(key, value);

        final int collisionKey = key + map.getCapacity();
        final String collisionValue = Integer.toString(collisionKey);
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
            intToObjectMap.put(i, Integer.toString(i));
        }

        assertThat(valueOf(intToObjectMap.size()), is(valueOf(15)));
        assertThat(intToObjectMap.get(1), is("1"));

        intToObjectMap.clear();

        assertThat(valueOf(intToObjectMap.size()), is(valueOf(0)));
        Assert.assertNull(intToObjectMap.get(1));
    }

    @Test
    public void shouldCompactCollection()
    {
        final int totalItems = 50;
        for (int i = 0; i < totalItems; i++)
        {
            intToObjectMap.put(i, Integer.toString(i));
        }

        for (int i = 0, limit = totalItems - 4; i < limit; i++)
        {
            intToObjectMap.remove(i);
        }

        final int capacityBeforeCompaction = intToObjectMap.getCapacity();
        intToObjectMap.compact();

        assertThat(valueOf(intToObjectMap.getCapacity()), lessThan(valueOf(capacityBeforeCompaction)));
    }

    @Test
    public void shouldContainValue()
    {
        final int key = 7;
        final String value = "Seven";

        intToObjectMap.put(key, value);

        Assert.assertTrue(intToObjectMap.containsValue(value));
        Assert.assertFalse(intToObjectMap.containsValue("NoKey"));
    }

    @Test
    public void shouldContainKey()
    {
        final int key = 7;
        final String value = "Seven";

        intToObjectMap.put(key, value);

        Assert.assertTrue(intToObjectMap.containsKey(key));
        Assert.assertFalse(intToObjectMap.containsKey(0));
    }

    @Test
    public void shouldRemoveEntry()
    {
        final int key = 7;
        final String value = "Seven";

        intToObjectMap.put(key, value);

        Assert.assertTrue(intToObjectMap.containsKey(key));

        intToObjectMap.remove(key);

        Assert.assertFalse(intToObjectMap.containsKey(key));
    }

    @Test
    public void shouldRemoveEntryAndCompactCollisionChain()
    {
        final int key = 12;
        final String value = "12";

        intToObjectMap.put(key, value);
        intToObjectMap.put(13, "13");

        final int collisionKey = key + intToObjectMap.getCapacity();
        final String collisionValue = Integer.toString(collisionKey);

        intToObjectMap.put(collisionKey, collisionValue);
        intToObjectMap.put(14, "14");

        assertThat(intToObjectMap.remove(key), is(value));
    }

    @Test
    public void shouldIterateValues()
    {
        final Collection<String> initialSet = new HashSet<>();

        for (int i = 0; i < 11; i++)
        {
            final String value = Integer.toString(i);
            intToObjectMap.put(i, value);
            initialSet.add(value);
        }

        final Collection<String> copyToSet = new HashSet<>();

        for (final String s : intToObjectMap.values())
        {
            copyToSet.add(s);
        }

        assertThat(copyToSet, is(initialSet));
    }

    @Test
    public void shouldIterateKeysGettingIntAsPrimitive()
    {
        final Collection<Integer> initialSet = new HashSet<>();

        for (int i = 0; i < 11; i++)
        {
            final String value = Integer.toString(i);
            intToObjectMap.put(i, value);
            initialSet.add(valueOf(i));
        }

        final Collection<Integer> copyToSet = new HashSet<>();

        for (final Int2ObjectHashMap.KeyIterator iter = intToObjectMap.keySet().iterator(); iter.hasNext();)
        {
            copyToSet.add(valueOf(iter.nextInt()));
        }

        assertThat(copyToSet, is(initialSet));
    }

    @Test
    public void shouldIterateKeys()
    {
        final Collection<Integer> initialSet = new HashSet<>();

        for (int i = 0; i < 11; i++)
        {
            final String value = Integer.toString(i);
            intToObjectMap.put(i, value);
            initialSet.add(valueOf(i));
        }

        final Collection<Integer> copyToSet = new HashSet<>();

        for (final Integer aInteger : intToObjectMap.keySet())
        {
            copyToSet.add(aInteger);
        }

        assertThat(copyToSet, is(initialSet));
    }

    @Test
    public void shouldIterateAndHandleRemove()
    {
        final Collection<Integer> initialSet = new HashSet<>();

        final int count = 11;
        for (int i = 0; i < count; i++)
        {
            final String value = Integer.toString(i);
            intToObjectMap.put(i, value);
            initialSet.add(valueOf(i));
        }

        final Collection<Integer> copyOfSet = new HashSet<>();

        int i = 0;
        for (final Iterator<Integer> iter = intToObjectMap.keySet().iterator(); iter.hasNext();)
        {
            final Integer item = iter.next();
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
        assertThat(valueOf(intToObjectMap.size()), is(valueOf(reducedSetSize)));
        assertThat(valueOf(copyOfSet.size()), is(valueOf(reducedSetSize)));
    }

    @Test
    public void shouldIterateEntries()
    {
        final int count = 11;
        for (int i = 0; i < count; i++)
        {
            final String value = Integer.toString(i);
            intToObjectMap.put(i, value);
        }

        final String testValue = "Wibble";
        for (final Map.Entry<Integer, String> entry : intToObjectMap.entrySet())
        {
            assertThat(entry.getKey(), equalTo(valueOf(entry.getValue())));

            if (entry.getKey() == 7)
            {
                entry.setValue(testValue);
            }
        }

        assertThat(intToObjectMap.get(7), equalTo(testValue));
    }

    @Test
    public void shouldGenerateStringRepresentation()
    {
        final int[] testEntries = {3, 1, 19, 7, 11, 12, 7};

        for (final int testEntry : testEntries)
        {
            intToObjectMap.put(testEntry, String.valueOf(testEntry));
        }

        final String mapAsAString = "{7=7, 12=12, 19=19, 3=3, 11=11, 1=1}";
        assertThat(intToObjectMap.toString(), equalTo(mapAsAString));
    }
}

