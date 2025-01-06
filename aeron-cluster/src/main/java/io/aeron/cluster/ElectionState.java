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
package io.aeron.cluster;

import io.aeron.cluster.client.ClusterException;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Election states for a {@link ConsensusModule} which get represented by a {@link #code()} stored in a
 * {@link io.aeron.Counter} of the type {@link ConsensusModule.Configuration#ELECTION_STATE_TYPE_ID}.
 */
public enum ElectionState
{
    /**
     * Consolidate local state and prepare for new leadership.
     */
    INIT(0),

    /**
     * Canvass members for current state and to assess if a successful leadership attempt can be mounted.
     */
    CANVASS(1),

    /**
     * Nominate member for new leadership by requesting votes.
     */
    NOMINATE(2),

    /**
     * Await ballot outcome from members on candidacy for leadership.
     */
    CANDIDATE_BALLOT(3),

    /**
     * Await ballot outcome after voting for a candidate.
     */
    FOLLOWER_BALLOT(4),

    /**
     * Wait for followers to replicate any missing log entries to track commit position.
     */
    LEADER_LOG_REPLICATION(5),

    /**
     * Replay local log in preparation for new leadership term.
     */
    LEADER_REPLAY(6),

    /**
     * Initialise state for new leadership term.
     */
    LEADER_INIT(7),

    /**
     * Publish new leadership term and commit position, while awaiting followers ready.
     */
    LEADER_READY(8),

    /**
     * Replicate missing log entries from the leader.
     */
    FOLLOWER_LOG_REPLICATION(9),

    /**
     * Replay local log in preparation for following new leader.
     */
    FOLLOWER_REPLAY(10),

    /**
     * Initialise catch-up in preparation of receiving a replay from the leader to catch up in current term.
     */
    FOLLOWER_CATCHUP_INIT(11),

    /**
     * Await joining a replay from leader to catch-up.
     */
    FOLLOWER_CATCHUP_AWAIT(12),

    /**
     * Catch-up to leader until live log can be added and merged.
     */
    FOLLOWER_CATCHUP(13),

    /**
     * Initialise follower in preparation for joining the live log.
     */
    FOLLOWER_LOG_INIT(14),

    /**
     * Await joining the live log from the leader.
     */
    FOLLOWER_LOG_AWAIT(15),

    /**
     * Publish append position to leader to signify ready for new term.
     */
    FOLLOWER_READY(16),

    /**
     * Election is closed after new leader is established.
     */
    CLOSED(17);

    static final ElectionState[] STATES = values();

    private final int code;

    ElectionState(final int code)
    {
        if (code != ordinal())
        {
            throw new IllegalArgumentException(name() + " - code must equal ordinal value: code=" + code);
        }

        this.code = code;
    }

    /**
     * Code stored in a {@link io.aeron.Counter} to represent the election state.
     *
     * @return code stored in a {@link io.aeron.Counter} to represent the election state.
     */
    public int code()
    {
        return code;
    }

    /**
     * Get the enum value for a given code stored in a counter.
     *
     * @param code representing election state.
     * @return the enum value for a given code stored in a counter.
     */
    public static ElectionState get(final long code)
    {
        if (code < 0 || code > (STATES.length - 1))
        {
            throw new ClusterException("invalid election state counter code: " + code);
        }

        return STATES[(int)code];
    }

    /**
     * Get the {@link ElectionState} value based on the value stored in an {@link AtomicCounter}.
     *
     * @param counter to read the value for matching against {@link #code()}.
     * @return the {@link ElectionState} value based on the value stored in an {@link AtomicCounter}.
     */
    public static ElectionState get(final AtomicCounter counter)
    {
        if (counter.isClosed())
        {
            return CLOSED;
        }

        return get(counter.get());
    }
}
