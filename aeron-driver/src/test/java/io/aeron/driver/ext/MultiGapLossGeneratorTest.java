/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.driver.ext;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiGapLossGeneratorTest
{
    @Test
    void singleSmallGap()
    {
        final MultiGapLossGenerator generator = new MultiGapLossGenerator(0, 8, 4, 1);
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 0, 8));
        assertTrue(generator.shouldDropFrame(null, null, 123, 456, 0, 8, 8));
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 16, 8));
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 24, 8));

        assertFalse(generator.shouldDropFrame(null, null, 124, 456, 0, 0, 8));
        assertTrue(generator.shouldDropFrame(null, null, 124, 456, 0, 8, 8));
        assertFalse(generator.shouldDropFrame(null, null, 124, 456, 0, 16, 8));
        assertFalse(generator.shouldDropFrame(null, null, 124, 456, 0, 24, 8));
    }

    @Test
    void singleSmallGapRX()
    {
        final MultiGapLossGenerator generator = new MultiGapLossGenerator(0, 8, 4, 1);
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 0, 8));
        assertTrue(generator.shouldDropFrame(null, null, 123, 456, 0, 8, 8));
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 16, 8));

        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 8, 8));
    }

    @Test
    void multiSmallGap()
    {
        final MultiGapLossGenerator generator = new MultiGapLossGenerator(0, 16, 8, 4);
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 0, 8));
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 8, 8));
        assertTrue(generator.shouldDropFrame(null, null, 123, 456, 0, 16, 8));
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 24, 8));
        assertTrue(generator.shouldDropFrame(null, null, 123, 456, 0, 32, 8));
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 40, 8));
        assertTrue(generator.shouldDropFrame(null, null, 123, 456, 0, 48, 4));
        assertTrue(generator.shouldDropFrame(null, null, 123, 456, 0, 52, 8));
        assertFalse(generator.shouldDropFrame(null, null, 123, 456, 0, 60, 2));
        assertTrue(generator.shouldDropFrame(null, null, 123, 456, 0, 62, 10));
    }
}