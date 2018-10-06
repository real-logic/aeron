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
package io.aeron.cluster.service;

import io.aeron.Aeron;
import io.aeron.Counter;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Counter representing the commit position that can consumed by a state machine on a stream for the a leadership term.
 * <p>
 * Key layout as follows:
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                     Leadership Term ID                        |
 *  |                                                               |
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
     * Human readable name for the counter.
     */
    public static final String NAME = "commit-pos: leadershipTermId=";

    public static final int LEADERSHIP_TERM_ID_OFFSET = 0;
    public static final int KEY_LENGTH = LEADERSHIP_TERM_ID_OFFSET + SIZE_OF_LONG;

    /**
     * Allocate a counter to represent the commit position on stream for the current leadership term.
     *
     * @param aeron                to allocate the counter.
     * @param tempBuffer           to use for building the key and label without allocation.
     * @param leadershipTermId     of the log at the beginning of the leadership term.
     * @return the {@link Counter} for the commit position.
     */
    public static Counter allocate(
        final Aeron aeron, final MutableDirectBuffer tempBuffer, final long leadershipTermId)
    {
        tempBuffer.putLong(LEADERSHIP_TERM_ID_OFFSET, leadershipTermId);

        final int labelOffset = BitUtil.align(KEY_LENGTH, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, NAME);
        labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, leadershipTermId);

        return aeron.addCounter(
            COMMIT_POSITION_TYPE_ID, tempBuffer, 0, KEY_LENGTH, tempBuffer, labelOffset, labelLength);
    }
}
