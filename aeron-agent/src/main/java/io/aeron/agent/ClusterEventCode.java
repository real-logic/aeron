/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

import java.util.Arrays;

import static io.aeron.agent.ClusterEventDissector.dissectNewLeadershipTerm;

/**
 * Events that can be enabled for logging in the cluster module.
 */
public enum ClusterEventCode implements EventCode
{
    ELECTION_STATE_CHANGE(1, ClusterEventDissector::dissectStateChange),
    NEW_LEADERSHIP_TERM(2, (eventCode, buffer, offset, builder) -> dissectNewLeadershipTerm(buffer, offset, builder)),
    STATE_CHANGE(3, ClusterEventDissector::dissectStateChange),
    ROLE_CHANGE(4, ClusterEventDissector::dissectStateChange);

    static final int EVENT_CODE_TYPE = EventCodeType.CLUSTER.getTypeCode();
    private static final ClusterEventCode[] EVENT_CODE_BY_ID;

    private final int id;
    private final DissectFunction<ClusterEventCode> dissector;

    static
    {
        final ClusterEventCode[] codes = ClusterEventCode.values();
        final int maxId = Arrays.stream(codes).mapToInt(ClusterEventCode::id).max().orElse(0);
        EVENT_CODE_BY_ID = new ClusterEventCode[maxId + 1];

        for (final ClusterEventCode code : codes)
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

    public void decode(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        dissector.dissect(this, buffer, offset, builder);
    }
}
