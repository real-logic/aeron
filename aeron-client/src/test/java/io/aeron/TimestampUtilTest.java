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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TimestampUtilTest
{
    static boolean hasReachedDeadline(final long now, final long deadline)
    {
        return deadline - now < 0;
    }

    @Test
    void shouldCorrectlyReachDeadline()
    {
        assertFalse(hasReachedDeadline(1, 2));
        assertFalse(hasReachedDeadline(2, 2));
        assertTrue(hasReachedDeadline(3, 2));
        assertFalse(hasReachedDeadline(Long.MAX_VALUE, Long.MAX_VALUE));
        //noinspection NumericOverflow
        assertFalse(hasReachedDeadline(Long.MAX_VALUE, Long.MAX_VALUE + 1));
        assertFalse(hasReachedDeadline(Long.MIN_VALUE, Long.MIN_VALUE + 1));
        assertFalse(hasReachedDeadline(-1, -1 + Long.MAX_VALUE));
        assertTrue(hasReachedDeadline(-1, Long.MAX_VALUE));
    }
}