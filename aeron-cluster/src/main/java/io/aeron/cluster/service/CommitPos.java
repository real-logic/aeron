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
 * Counter representing the commit position that can consumed by a state machine on a stream for the current
 * leadership term.
 * <p>
 * Key layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |             Recording ID for the leadership term              |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |            Log Position at base of leadership term            |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                     Leadership Term ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                      Log Session ID                           |
 *  +---------------------------------------------------------------+
 *  |                       Recovery Step                           |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class CommitPos
{
    /**
     * Type id of a commit position counter.
     */
    public static final int COMMIT_POSITION_TYPE_ID = 203;

    /**
     * Represents a null value if the counter is not found.
     */
    public static final int NULL_VALUE = -1;

    /**
     * Human readable name for the counter.
     */
    public static final String NAME = "commit-pos: leadershipTermId=";

    public static final int RECORDING_ID_OFFSET = 0;
    public static final int LOG_POSITION_OFFSET = RECORDING_ID_OFFSET + SIZE_OF_LONG;
    public static final int LEADERSHIP_TERM_ID_OFFSET = LOG_POSITION_OFFSET + SIZE_OF_LONG;
    public static final int SESSION_ID_OFFSET = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;
    public static final int REPLAY_STEP_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    public static final int KEY_LENGTH = REPLAY_STEP_OFFSET + SIZE_OF_INT;

    /**
     * Allocate a counter to represent the commit position on stream for the current leadership term.
     *
     * @param aeron            to allocate the counter.
     * @param tempBuffer       to use for building the key and label without allocation.
     * @param recordingId      for the current term.
     * @param logPosition      of the log at the beginning of the leadership term.
     * @param leadershipTermId of the log at the beginning of the leadership term.
     * @param sessionId        of the active log for the current leadership term.
     * @param replayStep       during the recovery process or replaying term logs.
     * @return the {@link Counter} for the commit position.
     */
    public static Counter allocate(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final long recordingId,
        final long logPosition,
        final long leadershipTermId,
        final int sessionId,
        final int replayStep)
    {
        tempBuffer.putLong(RECORDING_ID_OFFSET, recordingId);
        tempBuffer.putLong(LOG_POSITION_OFFSET, logPosition);
        tempBuffer.putLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);
        tempBuffer.putInt(SESSION_ID_OFFSET, sessionId);
        tempBuffer.putInt(REPLAY_STEP_OFFSET, replayStep);

        int labelOffset = 0;
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, NAME);
        labelOffset += tempBuffer.putLongAscii(KEY_LENGTH + labelOffset, leadershipTermId);
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, " logSessionId=");
        labelOffset += tempBuffer.putIntAscii(KEY_LENGTH + labelOffset, sessionId);
        labelOffset += tempBuffer.putStringWithoutLengthAscii(KEY_LENGTH + labelOffset, " replayStep=");
        labelOffset += tempBuffer.putIntAscii(KEY_LENGTH + labelOffset, replayStep);

        return aeron.addCounter(
            COMMIT_POSITION_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, KEY_LENGTH, labelOffset);
    }

    /**
     * Find the active counter id for a stream based on the session id.
     *
     * @param counters  to search within.
     * @param sessionId for the active log.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     */
    public static int findCounterIdBySession(final CountersReader counters, final int sessionId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            if (counters.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID &&
                    buffer.getInt(recordOffset + KEY_OFFSET + SESSION_ID_OFFSET) == sessionId)
                {
                    return i;
                }
            }
        }

        return CountersReader.NULL_COUNTER_ID;
    }

    /**
     * Find the active counter id for a stream based on the replay step during recovery.
     *
     * @param counters   to search within.
     * @param replayStep for the active log.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     */
    public static int findCounterIdByReplayStep(final CountersReader counters, final int replayStep)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, size = counters.maxCounterId(); i < size; i++)
        {
            if (counters.getCounterState(i) == RECORD_ALLOCATED)
            {
                final int recordOffset = CountersReader.metaDataOffset(i);

                if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID &&
                    buffer.getInt(recordOffset + KEY_OFFSET + REPLAY_STEP_OFFSET) == replayStep)
                {
                    return i;
                }
            }
        }

        return CountersReader.NULL_COUNTER_ID;
    }

    /**
     * Get the recording id for the current leadership term.
     *
     * @param counters  to search within.
     * @param counterId for the active commit position.
     * @return the recording id if found otherwise {@link #NULL_VALUE}.
     */
    public static long getRecordingId(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + RECORDING_ID_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the accumulated log position as a base for this leadership term.
     *
     * @param counters  to search within.
     * @param counterId for the active commit position.
     * @return the base log position if found otherwise {@link #NULL_VALUE}.
     */
    public static long getBaseLogPosition(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + LOG_POSITION_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the leadership term id for the given commit position.
     *
     * @param counters  to search within.
     * @param counterId for the active commit position.
     * @return the leadership term id if found otherwise {@link #NULL_VALUE}.
     */
    public static long getLeadershipTermId(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
            {
                return buffer.getLong(recordOffset + KEY_OFFSET + LEADERSHIP_TERM_ID_OFFSET);
            }
        }

        return NULL_VALUE;
    }

    /**
     * Get the replay step index for a given counter.
     *
     * @param counters  to search within.
     * @param counterId for the active commit position.
     * @return the replay step value if found otherwise {@link #NULL_VALUE}.
     */
    public static int getReplayStep(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
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
     * @param counters  to search within.
     * @param counterId for the active commit position.
     * @return the session id if found other which throw {@link IllegalStateException}
     * @throws IllegalStateException if counter is not found.
     */
    public static int getLogSessionId(final CountersReader counters, final int counterId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            if (buffer.getInt(recordOffset + TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID)
            {
                return buffer.getInt(recordOffset + KEY_OFFSET + SESSION_ID_OFFSET);
            }
        }

        throw new IllegalStateException("No active counter for id: " + counterId);
    }

    /**
     * Is the counter still active and log still recording?
     *
     * @param counters    to search within.
     * @param counterId   to search for.
     * @param recordingId to match against.
     * @return true if the counter is still active otherwise false.
     */
    public static boolean isActive(final CountersReader counters, final int counterId, final long recordingId)
    {
        final DirectBuffer buffer = counters.metaDataBuffer();

        if (counters.getCounterState(counterId) == RECORD_ALLOCATED)
        {
            final int recordOffset = CountersReader.metaDataOffset(counterId);

            return buffer.getInt(recordOffset + TYPE_ID_OFFSET) == COMMIT_POSITION_TYPE_ID &&
                buffer.getLong(recordOffset + KEY_OFFSET + RECORDING_ID_OFFSET) == recordingId;
        }

        return false;
    }
}
