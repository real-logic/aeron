/*
 *  Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * Interface which a service must implement to be contained in the cluster.
 */
public interface ClusteredService
{
    /**
     * Start event for the service where the service can perform any initialisation required. This will be called
     * before any snapshot or logs are replayed.
     *
     * @param cluster with which the service can interact.
     */
    void onStart(Cluster cluster);

    /**
     * A session has been opened for a client to the cluster.
     *
     * @param session     for the client which have been opened.
     * @param timestampMs at which the session was opened.
     */
    void onSessionOpen(ClientSession session, long timestampMs);

    /**
     * A session has been closed for a client to the cluster.
     *
     * @param session     that has been closed.
     * @param timestampMs at which the session was closed.
     * @param closeReason the session was closed.
     */
    void onSessionClose(ClientSession session, long timestampMs, CloseReason closeReason);

    /**
     * A message has been received to be processed by a clustered service.
     *
     * @param session       for the client which sent the message.
     * @param correlationId to associate any response.
     * @param timestampMs   for when the message was received.
     * @param buffer        containing the message.
     * @param offset        in the buffer at which the message is encoded.
     * @param length        of the encoded message.
     * @param header        aeron header for the incoming message.
     */
    void onSessionMessage(
        ClientSession session,
        long correlationId,
        long timestampMs,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    /**
     * A scheduled timer has expired.
     *
     * @param correlationId for the expired timer.
     * @param timestampMs   at which the timer expired.
     */
    void onTimerEvent(long correlationId, long timestampMs);

    /**
     * The service should take a snapshot and store its state to the provided archive {@link Publication}.
     * <p>
     * <b>Note:</b> As this is a potentially long running operation the implementation should occasional call
     * {@link Thread#isInterrupted()} and if true then throw an {@link InterruptedException} or
     * {@link org.agrona.concurrent.AgentTerminationException}.
     *
     * @param snapshotPublication to which the state should be recorded.
     */
    void onTakeSnapshot(Publication snapshotPublication);

    /**
     * The service should load its state from a stored snapshot in the provided archived {@link Image}.
     * <p>
     * <b>Note:</b> As this is a potentially long running operation the implementation should occasional call
     * {@link Thread#isInterrupted()} and if true then throw an {@link InterruptedException} or
     * {@link org.agrona.concurrent.AgentTerminationException}.
     *
     * @param snapshotImage to which the service should store its state.
     */
    void onLoadSnapshot(Image snapshotImage);

    /**
     * Notify that the cluster node has changed role.
     *
     * @param newRole that the node has assumed.
     */
    void onRoleChange(Cluster.Role newRole);
}
