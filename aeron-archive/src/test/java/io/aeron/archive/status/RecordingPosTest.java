/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.archive.status;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.BitUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static io.aeron.archive.status.RecordingPos.RECORDING_POSITION_TYPE_ID;
import static io.aeron.test.Tests.generateStringWithSuffix;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecordingPosTest
{
    @Test
    void allocateMaxKeyAndLabel()
    {
        final long archiveId = Long.MIN_VALUE;
        final long recordingId = 54311;
        final int sessionId = 42;
        final int streamId = -13;
        final String strippedChannel = generateStringWithSuffix("stripped channel", ".", 1000);
        final String sourceIdentity = generateStringWithSuffix("source identity", "X", 5000);
        final UnsafeBuffer tempBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);
        final Counter counter = mock(Counter.class);
        final Aeron aeron = mock(Aeron.class);
        when(aeron.addCounter(
            eq(RECORDING_POSITION_TYPE_ID),
            eq(tempBuffer),
            eq(0),
            eq(MAX_KEY_LENGTH),
            eq(tempBuffer),
            eq(MAX_KEY_LENGTH),
            eq(MAX_LABEL_LENGTH)))
            .thenReturn(counter);

        final Counter result = RecordingPos.allocate(
            aeron,
            tempBuffer,
            archiveId,
            recordingId,
            sessionId,
            streamId,
            strippedChannel,
            sourceIdentity);

        assertSame(counter, result);
        int offset = 0;
        assertEquals(archiveId, tempBuffer.getLong(offset));
        offset += SIZE_OF_LONG;
        assertEquals(recordingId, tempBuffer.getLong(offset));
        offset += SIZE_OF_LONG;
        assertEquals(sessionId, tempBuffer.getInt(offset));
        offset += SIZE_OF_INT;
        final int expectedSourceIdentityLength = MAX_KEY_LENGTH - offset - SIZE_OF_INT;
        assertEquals(expectedSourceIdentityLength, tempBuffer.getInt(offset));
        assertTrue(expectedSourceIdentityLength < sourceIdentity.length());
        offset += SIZE_OF_INT;
        assertEquals(
            sourceIdentity.substring(0, expectedSourceIdentityLength),
            tempBuffer.getStringWithoutLengthAscii(offset, expectedSourceIdentityLength));

        offset = BitUtil.align(offset + expectedSourceIdentityLength, SIZE_OF_INT);
        assertEquals("rec-pos: -9223372036854775808 54311 42 -13 ", tempBuffer.getStringWithoutLengthAscii(offset, 43));
        offset += 43;
        final int expectedStrippedChannelLength = MAX_LABEL_LENGTH - offset;
        assertTrue(expectedStrippedChannelLength < strippedChannel.length());
        assertEquals(
            strippedChannel.substring(0, expectedStrippedChannelLength),
            tempBuffer.getStringWithoutLengthAscii(offset, expectedStrippedChannelLength));
    }

    @Test
    void allocateShouldAlignLabelByFourBytes()
    {
        final long archiveId = 888;
        final long recordingId = 1;
        final int sessionId = 30;
        final int streamId = 222;
        final String strippedChannel = "channel";
        final String sourceIdentity = "source";
        final UnsafeBuffer tempBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);
        final Counter counter = mock(Counter.class);
        final Aeron aeron = mock(Aeron.class);
        when(aeron.addCounter(
            eq(RECORDING_POSITION_TYPE_ID),
            eq(tempBuffer),
            eq(0),
            eq(30),
            eq(tempBuffer),
            eq(32),
            eq(29)))
            .thenReturn(counter);

        final Counter result = RecordingPos.allocate(
            aeron,
            tempBuffer,
            archiveId,
            recordingId,
            sessionId,
            streamId,
            strippedChannel,
            sourceIdentity);

        assertSame(counter, result);
        int offset = 0;
        assertEquals(archiveId, tempBuffer.getLong(offset));
        offset += SIZE_OF_LONG;
        assertEquals(recordingId, tempBuffer.getLong(offset));
        offset += SIZE_OF_LONG;
        assertEquals(sessionId, tempBuffer.getInt(offset));
        offset += SIZE_OF_INT;
        assertEquals(sourceIdentity.length(), tempBuffer.getInt(offset));
        offset += SIZE_OF_INT;
        assertEquals(sourceIdentity, tempBuffer.getStringWithoutLengthAscii(offset, sourceIdentity.length()));

        offset = 32;
        assertEquals("rec-pos: 888 1 30 222 ", tempBuffer.getStringWithoutLengthAscii(offset, 22));
        offset += 22;
        assertEquals(strippedChannel, tempBuffer.getStringWithoutLengthAscii(offset, strippedChannel.length()));
    }
}
