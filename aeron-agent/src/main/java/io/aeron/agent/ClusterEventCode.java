package io.aeron.agent;

/**
 * Cluster events and codecs for encoding/decoding events recorded to the {@link EventConfiguration#EVENT_RING_BUFFER}.
 */
public enum ClusterEventCode
{
    ELECTION_STATE_CHANGE(0, /*ClusterEventDissector::electionStateChange*/ null);

    static final int EVENT_CODE_TYPE = EventCodeType.DRIVER.getTypeCode();
    private static final int MAX_ID = 63;
    private static final EventCode[] EVENT_CODE_BY_ID = new EventCode[MAX_ID];

    private final long tagBit;
    private final int id;
    private final DissectFunction/*<ClusterEventCode>*/ dissector;

    static
    {
        for (final EventCode code : EventCode.values())
        {
            final int id = code.id();
            if (null != EVENT_CODE_BY_ID[id])
            {
                throw new IllegalArgumentException("id already in use: " + id);
            }

            EVENT_CODE_BY_ID[id] = code;
        }
    }

    ClusterEventCode(final int id, final DissectFunction/*<ClusterEventCode>*/ dissector)
    {
        this.id = id;
        this.tagBit = 1L << id;
        this.dissector = dissector;
    }

    public int id()
    {
        return id;
    }

}