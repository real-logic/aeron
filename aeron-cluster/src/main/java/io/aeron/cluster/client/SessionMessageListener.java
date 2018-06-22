/*
 * Copyright 2018 Real Logic Ltd.
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

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Interface for consuming messages on a given session from the cluster.
 */
@FunctionalInterface
public interface SessionMessageListener
{
    /**
     * Message event returned from the cluster.
     *
     * @param correlationId    to associate with the ingress message to which it is correlated.
     * @param clusterSessionId to which the message belongs.
     * @param timestamp        at which the correlated ingress was sequenced in the cluster.
     * @param buffer           containing the message.
     * @param offset           at which the message begins.
     * @param length           of the message in bytes.
     * @param header           Aeron header associated with the message fragment.
     */
    void onMessage(
        long correlationId,
        long clusterSessionId,
        long timestamp,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);
}
