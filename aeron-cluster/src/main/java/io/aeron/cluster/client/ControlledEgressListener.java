/*
 *  Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.cluster.client;

import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Interface for consuming messages coming from the cluster that also include administrative events in a controlled
 * fashion like {@link io.aeron.logbuffer.ControlledFragmentHandler}. Only session messages my be controlled in
 * consumption, other are consumed via {@link io.aeron.logbuffer.ControlledFragmentHandler.Action#COMMIT}.
 */
@FunctionalInterface
public interface ControlledEgressListener
{
    /**
     * Message event returned from the clustered service.
     *
     * @param clusterSessionId to which the message belongs.
     * @param timestampMs      at which the correlated ingress was sequenced in the cluster.
     * @param buffer           containing the message.
     * @param offset           at which the message begins.
     * @param length           of the message in bytes.
     * @param header           Aeron header associated with the message fragment.
     * @return what action should be taken regarding advancement of the stream.
     */
    ControlledFragmentHandler.Action onMessage(
        long clusterSessionId,
        long timestampMs,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    default void sessionEvent(
        long correlationId,
        long clusterSessionId,
        long leadershipTermId,
        int leaderMemberId,
        EventCode code,
        String detail)
    {
    }

    default void newLeader(long clusterSessionId, long leadershipTermId, int leaderMemberId, String memberEndpoints)
    {
    }
}
