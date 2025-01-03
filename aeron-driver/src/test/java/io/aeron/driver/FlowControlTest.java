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
package io.aeron.driver;

import org.junit.jupiter.api.Test;

import static io.aeron.driver.FlowControl.calculateRetransmissionLength;
import static org.junit.jupiter.api.Assertions.*;

class FlowControlTest
{
    @Test
    void shouldUseResendLengthIfSmallestValue()
    {
        final int resendLength = 1024;

        assertEquals(resendLength, calculateRetransmissionLength(resendLength, 64 * 1024, 0, 16));
    }

    @Test
    void shouldClampToTheEndOfTheBuffer()
    {
        final int expectedLength = 512;
        final int termLength = 64 * 1024;
        final int termOffset = termLength - expectedLength;

        assertEquals(expectedLength, calculateRetransmissionLength(1024, termLength, termOffset, 16));
    }

    @Test
    void shouldClampToReceiverWindow()
    {
        final int multiplier = 16;
        final int expectedLength = Configuration.INITIAL_WINDOW_LENGTH_DEFAULT * multiplier;

        assertEquals(expectedLength, calculateRetransmissionLength(4 * 1024 * 1024, 8 * 1024 * 1024, 0, 16));
    }
}