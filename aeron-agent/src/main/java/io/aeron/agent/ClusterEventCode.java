/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

/**
 * Events that can be enabled for logging in the cluster module.
 */
public enum ClusterEventCode implements EventCode
{
    ELECTION_STATE_CHANGE(1, ClusterEventDissector::electionStateChange),
    NEW_LEADERSHIP_TERM(2, ClusterEventDissector::newLeadershipTerm),
    STATE_CHANGE(3, ClusterEventDissector::stateChange),
    ROLE_CHANGE(4, ClusterEventDissector::roleChange);

    static final int EVENT_CODE_TYPE = EventCodeType.CLUSTER.getTypeCode();
    private static final int MAX_ID = 63;
    private static final ClusterEventCode[] EVENT_CODE_BY_ID = new ClusterEventCode[MAX_ID];

    private final long tagBit;
    private final int id;
    private final DissectFunction<ClusterEventCode> dissector;

    static
    {
        for (final ClusterEventCode code : ClusterEventCode.values())
        {
            final int id = code.id();
            if (null != EVENT_CODE_BY_ID[id])
            {
                throw new IllegalArgumentException("id already in use: " + id);
            }

            EVENT_CODE_BY_ID[id] = code;
        }
    }

    ClusterEventCode(final int id, final DissectFunction<ClusterEventCode> dissector)
    {
        this.id = id;
        this.tagBit = 1L << id;
        this.dissector = dissector;
    }

    static ClusterEventCode get(final int eventCodeId)
    {
        return EVENT_CODE_BY_ID[eventCodeId];
    }

    /**
     * {@inheritDoc}
     */
    public int id()
    {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    public long tagBit()
    {
        return tagBit;
    }

    /**
     * {@inheritDoc}
     */
    public EventCodeType eventCodeType()
    {
        return EventCodeType.CLUSTER;
    }

    public void decode(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        dissector.dissect(this, buffer, offset, builder);
    }

    public static boolean isEnabled(final ClusterEventCode code, final long mask)
    {
        return (mask & code.tagBit()) == code.tagBit();
    }
}