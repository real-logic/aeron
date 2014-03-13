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

import static java.lang.Integer.valueOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.util.BitUtil.findNextPositivePowerOfTwo;

public class BitUtilTest
{
    @Test
    public void shouldReturnNextPositivePowerOfTwo()
    {
        assertThat(valueOf(findNextPositivePowerOfTwo(Integer.MIN_VALUE)), is(valueOf(Integer.MIN_VALUE)));
        assertThat(valueOf(findNextPositivePowerOfTwo(Integer.MIN_VALUE + 1)), is(valueOf(1)));
        assertThat(valueOf(findNextPositivePowerOfTwo(-1)), is(valueOf(1)));
        assertThat(valueOf(findNextPositivePowerOfTwo(0)), is(valueOf(1)));
        assertThat(valueOf(findNextPositivePowerOfTwo(1)), is(valueOf(1)));
        assertThat(valueOf(findNextPositivePowerOfTwo(2)), is(valueOf(2)));
        assertThat(valueOf(findNextPositivePowerOfTwo(3)), is(valueOf(4)));
        assertThat(valueOf(findNextPositivePowerOfTwo(4)), is(valueOf(4)));
        assertThat(valueOf(findNextPositivePowerOfTwo(31)), is(valueOf(32)));
        assertThat(valueOf(findNextPositivePowerOfTwo(32)), is(valueOf(32)));
        assertThat(valueOf(findNextPositivePowerOfTwo(1 << 30)), is(valueOf(1 << 30)));
        assertThat(valueOf(findNextPositivePowerOfTwo((1 << 30) + 1)), is(valueOf(Integer.MIN_VALUE)));
    }
}
