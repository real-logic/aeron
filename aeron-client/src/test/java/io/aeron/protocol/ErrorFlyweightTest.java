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
package io.aeron.protocol;

import org.junit.jupiter.api.Test;

import static io.aeron.protocol.ErrorFlyweight.HAS_GROUP_ID_FLAG;
import static io.aeron.protocol.ErrorFlyweight.MAX_ERROR_FRAME_LENGTH;
import static java.nio.ByteBuffer.allocate;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ErrorFlyweightTest
{
    @Test
    void shouldCorrectlySetFlagsForGroupTag()
    {
        final ErrorFlyweight errorFlyweight = new ErrorFlyweight(allocate(MAX_ERROR_FRAME_LENGTH));

        assertEquals(0, errorFlyweight.flags());
        errorFlyweight.groupTag(10L);
        assertEquals(HAS_GROUP_ID_FLAG, errorFlyweight.flags());

        errorFlyweight.groupTag(null);
        assertNotEquals(HAS_GROUP_ID_FLAG, errorFlyweight.flags());
    }

    @Test
    void shouldCorrectlyUpdateExistingFlagsForGroupTag()
    {
        final ErrorFlyweight errorFlyweight = new ErrorFlyweight(allocate(MAX_ERROR_FRAME_LENGTH));

        final short initialFlags = (short)0x3;

        errorFlyweight.flags(initialFlags);
        assertEquals(initialFlags, errorFlyweight.flags());
        errorFlyweight.groupTag(10L);
        assertEquals(HAS_GROUP_ID_FLAG, HAS_GROUP_ID_FLAG & errorFlyweight.flags());
        assertEquals(initialFlags, initialFlags & errorFlyweight.flags());

        errorFlyweight.groupTag(null);
        assertEquals(0, HAS_GROUP_ID_FLAG & errorFlyweight.flags());
        assertEquals(initialFlags, errorFlyweight.flags());
    }
}