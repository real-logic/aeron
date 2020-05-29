/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.agent;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.aeron.agent.ArchiveEventEncoder.*;
import static io.aeron.agent.CommonEventEncoder.*;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ArchiveEventEncoderTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);

    @Test
    void testEncodeReplicationStateChange()
    {
        final int offset = 24;
        final TimeUnit from = DAYS;
        final TimeUnit to = MILLISECONDS;
        final int replaySessionId = 521;
        final String payload = from.name() + STATE_SEPARATOR + to.name();
        final int length = payload.length() + SIZE_OF_INT * 2;
        final int captureLength = captureLength(length);

        final int encodedLength =
            encodeReplicationStateChange(buffer, offset, captureLength, length, from, to, replaySessionId);

        assertEquals(encodedLength(stateChangeLength(from, to)), encodedLength);
        assertEquals(captureLength, buffer.getInt(offset, LITTLE_ENDIAN));
        assertEquals(length, buffer.getInt(offset + SIZE_OF_INT, LITTLE_ENDIAN));
        assertNotEquals(0, buffer.getLong(offset + SIZE_OF_INT * 2, LITTLE_ENDIAN));
        assertEquals(replaySessionId, buffer.getInt(offset + LOG_HEADER_LENGTH));
        assertEquals(payload, buffer.getStringAscii(offset + LOG_HEADER_LENGTH + SIZE_OF_INT));
    }
}
