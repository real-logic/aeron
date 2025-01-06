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

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

class ConsensusModuleSnapshotPrinter implements ConsensusModuleSnapshotListener
{
    private final PrintStream out;

    ConsensusModuleSnapshotPrinter(final PrintStream out)
    {
        this.out = out;
    }

    public void onLoadBeginSnapshot(
        final int appVersion, final TimeUnit timeUnit, final DirectBuffer buffer, final int offset, final int length)
    {
        out.println("Snapshot:" +
            " appVersion=" + appVersion +
            " timeUnit=" + timeUnit);
    }

    public void onLoadEndSnapshot(final DirectBuffer buffer, final int offset, final int length)
    {
    }

    public void onLoadConsensusModuleState(
        final long nextSessionId,
        final long nextServiceSessionId,
        final long logServiceSessionId,
        final int pendingMessageCapacity,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        out.println("Consensus Module State:" +
            " nextSessionId=" + nextSessionId +
            " nextServiceSessionId=" + nextServiceSessionId +
            " logServiceSessionId=" + logServiceSessionId +
            " pendingMessageCapacity=" + pendingMessageCapacity);
    }

    public void onLoadPendingMessage(
        final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
    {
        out.println("Pending Message:" +
            " length=" + length +
            " clusterSessionId=" + clusterSessionId);
    }

    public void onLoadClusterSession(
        final long clusterSessionId,
        final long correlationId,
        final long openedLogPosition,
        final long timeOfLastActivity,
        final CloseReason closeReason,
        final int responseStreamId,
        final String responseChannel,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        out.println("Cluster Session:" +
            " clusterSessionId=" + clusterSessionId +
            " correlationId=" + correlationId +
            " openedLogPosition=" + openedLogPosition +
            " timeOfLastActivity=" + timeOfLastActivity +
            " closeReason=" + closeReason +
            " responseStreamId=" + responseStreamId +
            " responseChannel=" + responseChannel);
    }

    public void onLoadTimer(
        final long correlationId, final long deadline, final DirectBuffer buffer, final int offset, final int length)
    {
        out.println("Timer:" +
            " correlationId=" + correlationId +
            " deadline=" + deadline);
    }

    public void onLoadPendingMessageTracker(
        final long nextServiceSessionId,
        final long logServiceSessionId,
        final int pendingMessageCapacity,
        final int serviceId,
        final DirectBuffer buffer,
        final int offset,
        final int length)
    {
        out.println("Pending Message Tracker:" +
            " nextServiceSessionId=" + nextServiceSessionId +
            " logServiceSessionId=" + logServiceSessionId +
            " pendingMessageCapacity=" + pendingMessageCapacity +
            " serviceId=" + serviceId);
    }
}
