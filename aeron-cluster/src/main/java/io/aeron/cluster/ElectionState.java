package io.aeron.cluster;

import io.aeron.cluster.client.ClusterException;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Election states for a {@link ConsensusModule} which get represented by a code stored in a {@link io.aeron.Counter}
 * of the type {@link ConsensusModule.Configuration#ELECTION_STATE_TYPE_ID}.
 */
public enum ElectionState
{
    INIT(0),
    CANVASS(1),

    NOMINATE(2),
    CANDIDATE_BALLOT(3),
    FOLLOWER_BALLOT(4),

    LEADER_REPLAY(5),
    LEADER_TRANSITION(6),
    LEADER_READY(7),

    FOLLOWER_REPLAY(8),
    FOLLOWER_CATCHUP_TRANSITION(9),
    FOLLOWER_CATCHUP(10),
    FOLLOWER_TRANSITION(11),
    FOLLOWER_READY(12),

    CLOSED(13);

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
        return get(counter.get());
    }
}
