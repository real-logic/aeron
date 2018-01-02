/*
 * Copyright 2016 Real Logic Ltd.
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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.KEY_OFFSET;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;
import static org.agrona.concurrent.status.CountersReader.TYPE_ID_OFFSET;

/**
 * Counter representing the consensus position on a stream for the current term.
 * <p>
 * Key layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                  Recording ID for the term                    |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Log Position at beginning of term                |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |              Message Index at beginning of term               |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Session ID                            |
 *  +---------------------------------------------------------------+
 *  |                       Recovery Step                           |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class ConsensusPos
{
    /**
     * Type id of a consensus position counter.
     */
    public static final int CONSENSUS_POSITION_TYPE_ID = 202;

    /**
     * Represents a null counter id when not found.
     */
    public static final int NULL_COUNTER_ID = -1;

    /**
     * Represents a null value if the counter is not found.
     */
    public static final int NULL_VALUE = -1;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "con-pos: ";

    public static final int RECORDING_ID_OFFSET = 0;
    public static final int LOG_POSITION_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;
    public static final int MESSAGE_INDEX_OFFSET = LOG_POSITION_OFFSET + SIZE_OF_LONG;
    public static final int SESSION_ID_OFFSET = MESSAGE_INDEX_OFFSET + SIZE_OF_LONG;
    public static final int REPLAY_STEP_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    public static final int KEY_LENGTH = REPLAY_STEP_OFFSET + SIZE_OF_INT;

    /**
     * Allocate a counter to represent the consensus position on stream for the current leadership term.
     *
     * @param aeron        to allocate the counter.
     * @param tempBuffer   to use for building the key and label without allocation.
     * @param recordingId  for the current term.
     * @param logPosition  of the log at the beginning of the term.
     * @param messageIndex of the log at the beginning of the term.
     * @param sessionId    of the stream for the current term.
     * @param replayStep   during the recovery process or replaying term logs.
     * @return the {@link Counter} for the consensus position.
     */
    public static Counter allocate(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final long recordingId,
        final long logPosition,
        final long messageIndex,
        final int sessionId,
        final int replayStep)
    {
        tempBuffer.putLong(RECORDING_ID_OFFSET, recordingId);
        tempBuffer.putLong(LOG_POSITION_OFFSET, logPosition);
        tempBuffer.putLong(MESSAGE_INDEX_OFFSET, messageIndex);
        tempBuffer.putInt(SESSION_ID_OFFSET, sessionId);
        tempBuffer.putInt(REPLAY_STEP_OFFSET, replayStep);

        int labelOffset = 0;
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, NAME);
        labelOffset += tempBuffer.putIntAscii(KEY_LENGTH + labelOffset, sessionId);
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, " ");
        labelOffset += tempBuffer.putIntAscii(KEY_LENGTH + labelOffset, replayStep);

        return aeron.addCounter(
            CONSENSUS_POSITION_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, KEY_LENGTH, labelOffset);
    }

    /**
     * Find the active counter id for a stream based on the session id.
     *
     * @param countersReader to search within.
     * @param sessionId      for the active log.
     * @return the counter id if found otherwise {@link #NULL_COUNTER_ID}.
     */
    public static int findCounterIdBySession(final CountersReader countersReader, final int sessionId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        for (int i = 0, size = countersReader.maxCounterId(); i < size; i++)
        {
            if (countersReader.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONSENSUS_POSITION_TYPE_ID &&
                    buffer.getInt(recordOffset + KEY_OFFSET + SESSION_ID_OFFSET) == sessionId)
                {
                    return i;
                }
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Find the active counter id for a stream based on the replay step during recovery.
     *
     * @param countersReader to search within.
     * @param replayStep     for the active log.
     * @return the counter id if found otherwise {@link #NULL_COUNTER_ID}.
     */
    public static int findCounterIdByReplayStep(final CountersReader countersReader, final int replayStep)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        for (int i = 0, size = countersReader.maxCounterId(); i < size; i++)
        {
            if (countersReader.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONSENSUS_POSITION_TYPE_ID &&
                    buffer.getInt(recordOffset + KEY_OFFSET + REPLAY_STEP_OFFSET) == replayStep)
                {
                    return i;
                }
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Get the recording id for the current term.
     *
     * @param countersReader to search within.
     * @param counterId      for the active consensus position.
     * @return the recording id if found otherwise {@link #NULL_VALUE}.
     */
    public static long getRecordingId(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONSENSUS_POSITION_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + RECORDING_ID_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the beginning log position for a term for a given active counter.
     *
     * @param countersReader to search within.
     * @param counterId      for the active consensus position.
     * @return the beginning log position if found otherwise {@link #NULL_VALUE}.
     */
    public static long getBeginningLogPosition(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONSENSUS_POSITION_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + LOG_POSITION_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the beginning message index for the term for a given active counter.
     *
     * @param countersReader to search within.
     * @param counterId      for the active consensus position.
     * @return the beginning message index if found otherwise {@link #NULL_VALUE}.
     */
    public static long getBeginningMessageIndex(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONSENSUS_POSITION_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + MESSAGE_INDEX_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the replay step index for a given counter.
     *
     * @param countersReader to search within.
     * @param counterId      for the active consensus position.
     * @return the replay step value if found otherwise {@link #NULL_VALUE}.
     */
    public static int getReplayStep(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONSENSUS_POSITION_TYPE_ID)
            {
                return buffer.getInt(recordOffset + KEY_OFFSET + REPLAY_STEP_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the session id for a active counter. Since a session id can have any value there is no possible
     * null value so an exception will be thrown if the counter is not found.
     *
     * @param countersReader to search within.
     * @param counterId      for the active consensus position.
     * @return the session id if found other which throw {@link IllegalStateException}
     * @throws IllegalStateException if counter is not found.
     */
    public static int getSessionId(final CountersReader countersReader, final int counterId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        if (countersReader.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == CONSENSUS_POSITION_TYPE_ID)
            {
                return buffer.getInt(recordOffset + KEY_OFFSET + SESSION_ID_OFFSET);
            }
        }

        throw new IllegalStateException("No active counter for id: " + counterId);
    }
}
