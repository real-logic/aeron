/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.Image;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * The position a recording has reached when being archived.
 * <p>
 * Key has the following layout:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                        Recording ID                           |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Session ID                            |
 *  +---------------------------------------------------------------+
 *  |                Source Identity for the Image                  |
 *  |                                                              ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
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
    public static final long NULL_RECORDING_ID = Aeron.NULL_VALUE;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "rec-pos";

    public static final int RECORDING_ID_OFFSET = 0;
    public static final int SESSION_ID_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;
    public static final int SOURCE_IDENTITY_LENGTH_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    public static final int SOURCE_IDENTITY_OFFSET = SOURCE_IDENTITY_LENGTH_OFFSET + SIZE_OF_INT;

    public static Counter allocate(
        final Aeron aeron,
        final UnsafeBuffer tempBuffer,
        final long recordingId,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String sourceIdentity)
    {
        tempBuffer.putLong(RECORDING_ID_OFFSET, recordingId);
        tempBuffer.putInt(SESSION_ID_OFFSET, sessionId);

        final int sourceIdentityLength = Math.min(sourceIdentity.length(), MAX_KEY_LENGTH - SOURCE_IDENTITY_OFFSET);
        tempBuffer.putStringAscii(SOURCE_IDENTITY_LENGTH_OFFSET, sourceIdentity);
        final int keyLength = SOURCE_IDENTITY_OFFSET + sourceIdentityLength;

        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength, NAME + ": ");
        labelLength += tempBuffer.putLongAscii(keyLength + labelLength, recordingId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(keyLength + labelLength, sessionId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");
        labelLength += tempBuffer.putIntAscii(keyLength + labelLength, streamId);
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, " ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            keyLength + labelLength, strippedChannel, 0, MAX_LABEL_LENGTH - labelLength);

        return aeron.addCounter(
            RECORDING_POSITION_TYPE_ID, tempBuffer, 0, keyLength, tempBuffer, keyLength, labelLength);
    }

    /**
     * Find the active counter id for a stream based on the recording id.
     *
     * @param countersReader to search within.
     * @param recordingId    for the active recording.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     */
    public static int findCounterIdByRecording(final CountersReader countersReader, final long recordingId)
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

    /**
     * Find the active counter id for a stream based on the session id.
     *
     * @param countersReader to search within.
     * @param sessionId      for the active recording.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     */
    public static int findCounterIdBySession(final CountersReader countersReader, final int sessionId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        for (int i = 0, size = countersReader.maxCounterId(); i < size; i++)
        {
            if (countersReader.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                    buffer.getInt(recordOffset + KEY_OFFSET + SESSION_ID_OFFSET) == sessionId)
                {
                    return i;
                }
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Get the recording id for a given counter id.
     *
     * @param countersReader to search within.
     * @param counterId      for the active recording.
     * @return the counter id if found otherwise {@link #NULL_RECORDING_ID}.
     */
    public static long getRecordingId(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + RECORDING_ID_OFFSET);
            }
        }

        return NULL_RECORDING_ID;
    }

    /**
     * Get the {@link Image#sourceIdentity()} for the recording.
     *
     * @param countersReader to search within.
     * @param counterId      for the active recording.
     * @return {@link Image#sourceIdentity()} for the recording or null if not found.
     */
    public static String getSourceIdentity(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID)
            {
                return buffer.getStringAscii(recordOffset + KEY_OFFSET + SOURCE_IDENTITY_LENGTH_OFFSET);
            }
        }

        return null;
    }

    /**
     * Is the recording counter still active.
     *
     * @param countersReader to search within.
     * @param counterId      to search for.
     * @param recordingId    to confirm it is still the same value.
     * @return true if the counter is still active otherwise false.
     */
    public static boolean isActive(final CountersReader countersReader, final int counterId, final long recordingId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            return
                buffer.getInt(recordOffset + TYPE_ID_OFFSET) == RECORDING_POSITION_TYPE_ID &&
                buffer.getLong(recordOffset + KEY_OFFSET + RECORDING_ID_OFFSET) == recordingId;
        }

        return false;
    }
}
