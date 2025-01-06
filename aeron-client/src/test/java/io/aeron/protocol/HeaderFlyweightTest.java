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
package io.aeron.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HeaderFlyweightTest
{
    @Test
    void shouldConvertFlags()
    {
        final short flags = 0b01101000;

        final char[] flagsAsChars = HeaderFlyweight.flagsToChars(flags);
        assertEquals("01101000", new String(flagsAsChars));
    }

    @Test
    void shouldAppendFlags()
    {
        final short flags = 0b01100000;
        final StringBuilder builder = new StringBuilder();

        HeaderFlyweight.appendFlagsAsChars(flags, builder);
        assertEquals("01100000", builder.toString());
    }
}
