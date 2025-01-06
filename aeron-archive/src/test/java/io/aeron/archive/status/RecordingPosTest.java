/*
 * Copyright 2014-2025 Real Logic Limited.
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
import io.aeron.archive.ArchiveCounters;
import org.agrona.BitUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.status.RecordingPos.*;
import static io.aeron.test.Tests.generateStringWithSuffix;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
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
        assertEquals(recordingId, tempBuffer.getLong(offset));
        offset += SIZE_OF_LONG;
        assertEquals(sessionId, tempBuffer.getInt(offset));
        offset += SIZE_OF_INT;
        final int expectedSourceIdentityLength = MAX_KEY_LENGTH - offset - SIZE_OF_INT - SIZE_OF_LONG;
        assertEquals(expectedSourceIdentityLength, tempBuffer.getInt(offset));
        assertTrue(expectedSourceIdentityLength < sourceIdentity.length());
        offset += SIZE_OF_INT;
        assertEquals(
            sourceIdentity.substring(0, expectedSourceIdentityLength),
            tempBuffer.getStringWithoutLengthAscii(offset, expectedSourceIdentityLength));
        offset += expectedSourceIdentityLength;
        assertEquals(archiveId, tempBuffer.getLong(offset));
        offset += SIZE_OF_LONG;

        offset = BitUtil.align(offset, SIZE_OF_INT);
        final String expectedPrefix = "rec-pos: 54311 42 -13 ";
        assertEquals(expectedPrefix, tempBuffer.getStringWithoutLengthAscii(offset, expectedPrefix.length()));
        offset += expectedPrefix.length();
        final int expectedStrippedChannelLength =
            MAX_LABEL_LENGTH - expectedPrefix.length() - ArchiveCounters.lengthOfArchiveIdLabel(archiveId);
        assertTrue(expectedStrippedChannelLength < strippedChannel.length());
        assertEquals(
            strippedChannel.substring(0, expectedStrippedChannelLength),
            tempBuffer.getStringWithoutLengthAscii(offset, expectedStrippedChannelLength));
        offset += expectedStrippedChannelLength;
        assertEquals(
            " - archiveId=-9223372036854775808",
            tempBuffer.getStringWithoutLengthAscii(offset, ArchiveCounters.lengthOfArchiveIdLabel(archiveId)));
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
            eq(41)))
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
        assertEquals(recordingId, tempBuffer.getLong(offset));
        offset += SIZE_OF_LONG;
        assertEquals(sessionId, tempBuffer.getInt(offset));
        offset += SIZE_OF_INT;
        assertEquals(sourceIdentity.length(), tempBuffer.getInt(offset));
        offset += SIZE_OF_INT;
        assertEquals(sourceIdentity, tempBuffer.getStringWithoutLengthAscii(offset, sourceIdentity.length()));
        offset += sourceIdentity.length();
        assertEquals(archiveId, tempBuffer.getLong(offset));
        offset += SIZE_OF_LONG;

        offset = BitUtil.align(offset, SIZE_OF_INT);
        final String expectedLabelPrefix = "rec-pos: 1 30 222 ";
        assertEquals(expectedLabelPrefix, tempBuffer.getStringWithoutLengthAscii(offset, expectedLabelPrefix.length()));
        offset += expectedLabelPrefix.length();
        assertEquals(strippedChannel, tempBuffer.getStringWithoutLengthAscii(offset, strippedChannel.length()));
        offset += strippedChannel.length();
        assertEquals(
            " - archiveId=888",
            tempBuffer.getStringWithoutLengthAscii(offset, ArchiveCounters.lengthOfArchiveIdLabel(archiveId)));
    }

    @Test
    void shouldFindByRecordingIdAndArchiveId()
    {
        final long recordingId = 42;
        final long archiveId = 19;
        final int sourceIdentityLength = 10;
        final CountersReader countersReader = mock(CountersReader.class);
        when(countersReader.maxCounterId()).thenReturn(5);
        when(countersReader.getCounterState(anyInt())).thenReturn(RECORD_ALLOCATED);
        when(countersReader.getCounterTypeId(0)).thenReturn(0);
        when(countersReader.getCounterTypeId(2)).thenReturn(0);
        when(countersReader.getCounterTypeId(1)).thenReturn(RECORDING_POSITION_TYPE_ID);
        when(countersReader.getCounterTypeId(3)).thenReturn(RECORDING_POSITION_TYPE_ID);
        final AtomicBuffer metaBuffer = mock(AtomicBuffer.class);
        when(countersReader.metaDataBuffer()).thenReturn(metaBuffer);
        when(metaBuffer.getLong(METADATA_LENGTH + KEY_OFFSET + RECORDING_ID_OFFSET)).thenReturn(recordingId);
        final int keyOffset = 3 * METADATA_LENGTH + KEY_OFFSET;
        when(metaBuffer.getLong(keyOffset + RECORDING_ID_OFFSET)).thenReturn(recordingId);
        when(metaBuffer.getInt(keyOffset + SOURCE_IDENTITY_LENGTH_OFFSET)).thenReturn(sourceIdentityLength);
        when(metaBuffer.getLong(keyOffset + SOURCE_IDENTITY_OFFSET + sourceIdentityLength)).thenReturn(archiveId);

        assertEquals(3, RecordingPos.findCounterIdByRecording(countersReader, recordingId, archiveId));

        assertEquals(
            NULL_RECORDING_ID,
            RecordingPos.findCounterIdByRecording(countersReader, recordingId, Long.MIN_VALUE));

        assertEquals(1, RecordingPos.findCounterIdByRecording(countersReader, recordingId, NULL_VALUE));
    }

    @Test
    void shouldFindBySessionIdAndArchiveId()
    {
        final int sessionId = 888;
        final long archiveId = 19;
        final int sourceIdentityLength = 3;
        final CountersReader countersReader = mock(CountersReader.class);
        when(countersReader.maxCounterId()).thenReturn(5);
        when(countersReader.getCounterState(anyInt())).thenReturn(RECORD_ALLOCATED);
        when(countersReader.getCounterTypeId(anyInt())).thenReturn(RECORDING_POSITION_TYPE_ID);
        final AtomicBuffer metaBuffer = mock(AtomicBuffer.class);
        when(countersReader.metaDataBuffer()).thenReturn(metaBuffer);
        when(metaBuffer.getInt(METADATA_LENGTH + KEY_OFFSET + SESSION_ID_OFFSET)).thenReturn(sessionId);
        final int keyOffset = 2 * METADATA_LENGTH + KEY_OFFSET;
        when(metaBuffer.getInt(keyOffset + SESSION_ID_OFFSET)).thenReturn(sessionId);
        when(metaBuffer.getInt(keyOffset + SOURCE_IDENTITY_LENGTH_OFFSET)).thenReturn(sourceIdentityLength);
        when(metaBuffer.getLong(keyOffset + SOURCE_IDENTITY_OFFSET + sourceIdentityLength)).thenReturn(archiveId);

        assertEquals(2, RecordingPos.findCounterIdBySession(countersReader, sessionId, archiveId));

        assertEquals(
            NULL_RECORDING_ID,
            RecordingPos.findCounterIdBySession(countersReader, sessionId, Long.MIN_VALUE));

        assertEquals(1, RecordingPos.findCounterIdBySession(countersReader, sessionId, NULL_VALUE));
    }
}
