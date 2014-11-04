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
package uk.co.real_logic.agrona.concurrent;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AtomicArrayTest
{
    private final AtomicArray<Integer> array = new AtomicArray<>();

    @Test
    public void shouldHandleAddToEmptyArray()
    {
        array.add(10);
        assertThat(array.size(), is(1));
        assertThat(array.get(0), is(10));
    }

    @Test
    public void shouldHandleAddToNonEmptyArray()
    {
        array.add(10);
        array.add(20);

        assertThat(array.size(), is(2));
        assertThat(array.get(0), is(10));
        assertThat(array.get(1), is(20));
    }

    @Test
    public void shouldHandleRemoveFromEmptyArray()
    {
        array.remove(10);

        assertThat(array.size(), is(0));
    }

    @Test
    public void shouldHandleRemoveFromOneElementArray()
    {
        array.add(10);
        array.remove(10);

        assertThat(array.size(), is(0));
    }

    @Test
    public void shouldHandleRemoveOfNonExistentElementFromOneElementArray()
    {
        array.add(10);
        array.remove(20);

        assertThat(array.size(), is(1));
        assertThat(array.get(0), is(10));
    }

    @Test
    public void shouldHandleRemoveOfNonExistentElementFromArray()
    {
        array.add(10);
        array.add(20);
        array.remove(30);

        assertThat(array.size(), is(2));
        assertThat(array.get(0), is(10));
        assertThat(array.get(1), is(20));
    }

    @Test
    public void shouldHandleRemoveElementFromArrayEnd()
    {
        array.add(10);
        array.add(20);
        array.remove(20);

        assertThat(array.size(), is(1));
        assertThat(array.get(0), is(10));
    }

    @Test
    public void shouldHandleRemoveElementFromArrayBegin()
    {
        array.add(10);
        array.add(20);
        array.remove(10);

        assertThat(array.size(), is(1));
        assertThat(array.get(0), is(20));
    }

    @Test
    public void shouldHandleRemoveElementFromArrayMiddle()
    {
        array.add(10);
        array.add(20);
        array.add(30);
        array.remove(20);

        assertThat(array.size(), is(2));
        assertThat(array.get(0), is(10));
        assertThat(array.get(1), is(30));
    }

    @Test
    public void shouldIterateOverValuesInTheArray()
    {
        for (int start : new int[]{0, 1})
        {
            final Set<Integer> values = new HashSet<>(asList(10, 20, 30));
            final AtomicArray<Integer> array = new AtomicArray<>();
            values.forEach(array::add);

            assertThat(array.doAction(start, (e) -> values.remove(e) ? 1 : 0), is(3));
            assertThat(values, empty());
        }
    }

    @Test
    public void shouldHandleStartTooLargeTransparently()
    {
        final Set<Integer> values = new HashSet<>(asList(10, 20, 30));
        values.forEach(array::add);

        assertThat(array.doAction(4, (e) -> values.remove(e) ? 1 : 0), is(3));
        assertThat(values, empty());
    }

    @Test
    public void shouldNotFindInEmptyArray()
    {
        final Integer found = array.findFirst((e) -> e.equals(7));

        assertNull(found);
    }

    @Test
    public void shouldFindInArray()
    {
        final Integer matchItem = 3;
        asList(1, 2, 3, 4, 5).forEach(array::add);

        final Integer found = array.findFirst((e) -> e.equals(matchItem));

        assertThat(found, is(matchItem));
    }

    @Test
    public void shouldIterateValues()
    {
        asList(1, 2, 3, 4, 5).forEach(array::add);

        int i = 1;
        for (final int v : array)
        {
            assertThat(v, is(i++));
        }
    }

    @Test
    public void shouldReportEmpty()
    {
        assertTrue(array.isEmpty());
    }

    @Test
    public void shouldLimitedAction()
    {
        asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).forEach(array::add);
        final List<Integer> values = new ArrayList<>();

        final AtomicArray.ToIntLimitedFunction<Integer> action =
            (e, limit) ->
            {
                final int actionCount = Math.min(e, limit);

                for (int i = 0; i < actionCount; i++)
                {
                    values.add(e);
                }

                return actionCount;
            };

        final int actionCount = array.doLimitedAction(0, 10, action);

        assertThat(actionCount, is(10));
        assertThat(values, is(asList(1, 2, 2, 3, 3, 3, 4, 4, 4, 4)));
    }
}
