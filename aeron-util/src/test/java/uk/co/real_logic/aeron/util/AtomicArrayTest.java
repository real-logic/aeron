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
package uk.co.real_logic.aeron.util;

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static java.lang.Integer.valueOf;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AtomicArrayTest
{
    @Test
    public void shouldHandleAddToEmptyArray()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        array.add(valueOf(10));
        assertThat(valueOf(array.length()), is(valueOf(1)));
        assertThat(array.get(0), is(valueOf(10)));
    }

    @Test
    public void shouldHandleAddToNonEmptyArray()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        array.add(valueOf(10));
        array.add(valueOf(20));

        assertThat(valueOf(array.length()), is(valueOf(2)));
        assertThat(array.get(0), is(valueOf(10)));
        assertThat(array.get(1), is(valueOf(20)));
    }

    @Test
    public void shouldHandleRemoveFromEmptyArray()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        array.remove(valueOf(10));

        assertThat(valueOf(array.length()), is(valueOf(0)));
    }

    @Test
    public void shouldHandleRemoveFromOneElementArray()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        array.add(valueOf(10));
        array.remove(valueOf(10));

        assertThat(valueOf(array.length()), is(valueOf(0)));
    }

    @Test
    public void shouldHandleRemoveOfNonExistentElementFromOneElementArray()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        array.add(valueOf(10));
        array.remove(valueOf(20));

        assertThat(valueOf(array.length()), is(valueOf(1)));
        assertThat(array.get(0), is(valueOf(10)));
    }

    @Test
    public void shouldHandleRemoveOfNonExistentElementFromArray()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        array.add(valueOf(10));
        array.add(valueOf(20));
        array.remove(valueOf(30));

        assertThat(valueOf(array.length()), is(valueOf(2)));
        assertThat(array.get(0), is(valueOf(10)));
        assertThat(array.get(1), is(valueOf(20)));
    }

    @Test
    public void shouldHandleRemoveElementFromArrayEnd()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        array.add(valueOf(10));
        array.add(valueOf(20));
        array.remove(valueOf(20));

        assertThat(valueOf(array.length()), is(valueOf(1)));
        assertThat(array.get(0), is(valueOf(10)));
    }

    @Test
    public void shouldHandleRemoveElementFromArrayBegin()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        array.add(valueOf(10));
        array.add(valueOf(20));
        array.remove(valueOf(10));

        assertThat(valueOf(array.length()), is(valueOf(1)));
        assertThat(array.get(0), is(valueOf(20)));
    }

    @Test
    public void shouldHandleRemoveElementFromArrayMiddle()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        array.add(valueOf(10));
        array.add(valueOf(20));
        array.add(valueOf(30));
        array.remove(valueOf(20));

        assertThat(valueOf(array.length()), is(valueOf(2)));
        assertThat(array.get(0), is(valueOf(10)));
        assertThat(array.get(1), is(valueOf(30)));
    }

    @Test
    public void forEachShouldIterateOverValuesInTheArray()
    {
        for (int start : new int[] { 0, 1 })
        {
            // given
            Set<Integer> values = new HashSet<>(asList(10, 20, 30));
            AtomicArray<Integer> array = new AtomicArray<>();
            values.forEach(array::add);

            // when
            array.forEach(start, values::remove);

            // then
            assertThat(values, empty());
        }
    }

    @Test
    public void shouldHandleStartTooLargeTransparently()
    {
        // given
        Set<Integer> values = new HashSet<>(asList(10, 20, 30));
        AtomicArray<Integer> array = new AtomicArray<>();
        values.forEach(array::add);

        // when
        array.forEach(4, values::remove);

        // then
        assertThat(values, empty());
    }

    @Test
    public void arrayInitiallyUnchanged()
    {
        AtomicArray<Integer> array = new AtomicArray<>();

        assertFalse(array.changedSinceLastMark());
    }

    @Test
    public void changesIdentified()
    {
        AtomicArray<Integer> array = new AtomicArray<>();
        array.add(1);

        assertTrue(array.changedSinceLastMark());
    }

    @Test
    public void changesOnlySinceLastMark()
    {
        AtomicArray<Integer> array = new AtomicArray<>();
        array.add(1);
        array.mark();

        assertFalse(array.changedSinceLastMark());

        array.add(2);

        assertTrue(array.changedSinceLastMark());
    }

}
