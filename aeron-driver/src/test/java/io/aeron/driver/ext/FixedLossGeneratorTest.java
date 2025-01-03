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

import static org.junit.jupiter.api.Assertions.*;

class FixedLossGeneratorTest
{
    @Test
    void shouldDropSingleFrameOnce()
    {
        final FixedLossGenerator fixedLossGenerator = new FixedLossGenerator(0, 0, 1408);
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 0, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 0, 1408));
    }

    @Test
    void shouldDropFirstTwoFramesOnceUnaligned()
    {
        final FixedLossGenerator fixedLossGenerator = new FixedLossGenerator(0, 50, 1408);
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 0, 1408));
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 2 * 1408, 1408));

        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 0, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 2 * 1408, 1408));
    }

    @Test
    void shouldDropForIndependentStreams()
    {
        final FixedLossGenerator fixedLossGenerator = new FixedLossGenerator(0, 50, 1408);
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 0, 1408));
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 2 * 1408, 1408));

        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 123, 457, 0, 0, 1408));
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 123, 457, 0, 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 457, 0, 2 * 1408, 1408));

        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 124, 456, 0, 0, 1408));
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 124, 456, 0, 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 124, 456, 0, 2 * 1408, 1408));

        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 124, 457, 0, 0, 1408));
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 124, 457, 0, 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 124, 457, 0, 2 * 1408, 1408));
    }

    @Test
    void shouldOnlyDropInMatchingFrames()
    {
        final FixedLossGenerator fixedLossGenerator = new FixedLossGenerator(1, 2000, 1408);
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 0, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 2 * 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 0, 3 * 1408, 1408));

        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 1, 0, 1408));
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 1, 1408, 1408));
        assertTrue(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 1, 2 * 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 1, 3 * 1408, 1408));

        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 2, 0, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 2, 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 2, 2 * 1408, 1408));
        assertFalse(fixedLossGenerator.shouldDropFrame(null, null, 123, 456, 2, 3 * 1408, 1408));
    }
}