/*
 * Copyright 2017 Real Logic Ltd.
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
import io.aeron.archive.codecs.SourceLocation;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * The position a recording has reached when being archived.
 */
public class RecordingPos
{
    /**
     * Type id of a recording position counter.
     */
    public static final int RECORDING_POSITION_TYPE_ID = 100;

    /**
     * Represents a null recording id when not found.
     */
    public static final long NULL_RECORDING_ID = -1L;

    /**
     * Represents a null counter id when not found.
     */
    public static final int NULL_COUNTER_ID = -1;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "rec-pos";

    public static final int RECORDING_ID_OFFSET = 0;
    public static final int CONTROL_SESSION_ID_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;
    public static final int CORRELATION_ID_OFFSET = CONTROL_SESSION_ID_OFFSET + SIZE_OF_LONG;
    public static final int SESSION_ID_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    public static final int STREAM_ID_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    public static final int KEY_LENGTH = STREAM_ID_OFFSET + SIZE_OF_INT;

    public static Counter allocate(
        final Aeron aeron,
        final UnsafeBuffer tempBuffer,
        final long recordingId,
        final long controlSessionId,
        final long correlationId,
        final int sessionId,
        final int streamId,
        final String strippedChannel)
    {
        final String label = NAME + ": " + recordingId + " " + sessionId + " " + streamId + " " + strippedChannel;
        final String trimmedLabel = label.length() > MAX_LABEL_LENGTH ? label.substring(0, MAX_LABEL_LENGTH) : label;

        tempBuffer.putLong(RECORDING_ID_OFFSET, recordingId);
        tempBuffer.putLong(CONTROL_SESSION_ID_OFFSET, controlSessionId);
        tempBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);
        tempBuffer.putInt(SESSION_ID_OFFSET, sessionId);
        tempBuffer.putInt(STREAM_ID_OFFSET, streamId);

        tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH, trimmedLabel);

        return aeron.addCounter(
            RECORDING_POSITION_TYPE_ID,
            tempBuffer,
            0,
            KEY_LENGTH,
            tempBuffer,
            KEY_LENGTH,
            trimmedLabel.length());
    }

    /**
     * Find the active recording id for a stream based on the recording request.
     *
     * @param countersReader   to search within.
     * @param controlSessionId to which the recording belongs.
     * @param correlationId    returned from the start recording request.
     * @param sessionId        for the active stream from a publication.
     * @return the recordingId if found otherwise {@link #NULL_RECORDING_ID}.
     * @see io.aeron.archive.client.AeronArchive#startRecording(String, int, SourceLocation)
     */
    public static long findActiveRecordingId(
        final CountersReader countersReader,
        final long controlSessionId,
        final long correlationId,
        final int sessionId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        for (int i = 0, size = countersReader.maxCounterId(); i < size; i++)
        {
            if (countersReader.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                    buffer.getLong(recordOffset + KEY_OFFSET + CONTROL_SESSION_ID_OFFSET) == controlSessionId &&
                    buffer.getLong(recordOffset + KEY_OFFSET + CORRELATION_ID_OFFSET) == correlationId &&
                    buffer.getInt(recordOffset + KEY_OFFSET + SESSION_ID_OFFSET) == sessionId)
                {
                    return buffer.getLong(recordOffset + KEY_OFFSET + RECORDING_ID_OFFSET);
                }
            }
        }

        return NULL_RECORDING_ID;
    }

    /**
     * Find the active counter id for a stream based on the recording id.
     *
     * @param countersReader to search within.
     * @param recordingId    for the active recording.
     * @return the counter id if found otherwise {@link #NULL_COUNTER_ID}.
     */
    public static int findActiveRecordingPositionCounterId(
        final CountersReader countersReader,
        final long recordingId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        for (int i = 0, size = countersReader.maxCounterId(); i < size; i++)
        {
            if (countersReader.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                    buffer.getLong(recordOffset + KEY_OFFSET + RECORDING_ID_OFFSET) == recordingId)
                {
                    return i;
                }
            }
        }

        return NULL_COUNTER_ID;
    }
}
