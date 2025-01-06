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

import io.aeron.cluster.codecs.CloseReason;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

interface ConsensusModuleSnapshotListener
{
    void onLoadBeginSnapshot(int appVersion, TimeUnit timeUnit, DirectBuffer buffer, int offset, int length);

    void onLoadConsensusModuleState(
        long nextSessionId,
        long nextServiceSessionId,
        long logServiceSessionId,
        int pendingMessageCapacity,
        DirectBuffer buffer,
        int offset,
        int length);

    void onLoadPendingMessage(long clusterSessionId, DirectBuffer buffer, int offset, int length);

    void onLoadClusterSession(
        long clusterSessionId,
        long correlationId,
        long openedLogPosition,
        long timeOfLastActivity,
        CloseReason closeReason,
        int responseStreamId,
        String responseChannel,
        DirectBuffer buffer,
        int offset,
        int length);

    void onLoadTimer(long correlationId, long deadline, DirectBuffer buffer, int offset, int length);

    void onLoadPendingMessageTracker(
        long nextServiceSessionId,
        long logServiceSessionId,
        int pendingMessageCapacity,
        int serviceId,
        DirectBuffer buffer,
        int offset,
        int length);

    void onLoadEndSnapshot(DirectBuffer buffer, int offset, int length);
}
