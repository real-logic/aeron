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

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CollectionUtilTest
{
    @Test
    public void getOrDefaultUsesSupplier()
    {
        final Map<Integer, Integer> ints = new HashMap<>();
        final Integer result = CollectionUtil.getOrDefault(ints, 0, x -> x + 1);

        assertThat(result, is(1));
    }

    @Test
    public void getOrDefaultDoesNotCreateNewValueWhenOneExists()
    {
        final Map<Integer, Integer> ints = new HashMap<>();
        ints.put(0, 0);
        final Integer result = CollectionUtil.getOrDefault(
            ints,
            0,
            (x) ->
            {
                Assert.fail("Shouldn't be called");
                return x + 1;
            });

        assertThat(result, is(0));
    }
}
