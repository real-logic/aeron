/*
 *  Copyright 2017 Real Logic Ltd.
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

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

public interface ClusteredService
{
    void onStart(Cluster cluster);

    void onSessionOpen(ClientSession session, long timestampMs);

    void onSessionClose(ClientSession session, long timestampMs);

    void onSessionMessage(
        long clusterSessionId,
        long correlationId,
        long timestampMs,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    void onTimerEvent(long correlationId, long timestampMs);
}
