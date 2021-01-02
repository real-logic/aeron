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
package io.aeron;

import io.aeron.status.ChannelEndpointStatus;

/**
 * This class serves as a registry for all counter type IDs used by Aeron.
 * <p>
 * The following ranges are reserved:
 * <ul>
 *     <li>{@code 0 - 99}: for client/driver counters.</li>
 *     <li>{@code 100 - 199}: for archive counters.</li>
 *     <li>{@code 200 - 299}: for cluster counters.</li>
 * </ul>
 */
public final class AeronCounters
{
    // Client/driver counters

    /**
     * System wide counters for monitoring. These are separate from counters used for position tracking on streams.
     */
    public static final int DRIVER_SYSTEM_COUNTER_TYPE_ID = 0;

    /**
     * The limit as a position in bytes applied to publishers on a session-channel-stream tuple. Publishers will
     * experience back pressure when this position is passed as a means of flow control.
     */
    public static final int DRIVER_PUBLISHER_LIMIT_TYPE_ID = 1;

    /**
     * The position the Sender has reached for sending data to the media on a session-channel-stream tuple.
     */
    public static final int DRIVER_SENDER_POSITION_TYPE_ID = 2;

    /**
     * The highest position the Receiver has observed on a session-channel-stream tuple while rebuilding the stream.
     * It is possible the stream is not complete to this point if the stream has experienced loss.
     */
    public static final int DRIVER_RECEIVER_HWM_TYPE_ID = 3;
    /**
     * The position an individual Subscriber has reached on a session-channel-stream tuple. It is possible to have
     * multiple
     */
    public static final int DRIVER_SUBSCRIBER_POSITION_TYPE_ID = 4;

    /**
     * The highest position the Receiver has rebuilt up to on a session-channel-stream tuple while rebuilding the
     * stream.
     * The stream is complete up to this point.
     */
    public static final int DRIVER_RECEIVER_POS_TYPE_ID = 5;

    /**
     * The status of a send channel endpoint represented as a counter value.
     */
    public static final int DRIVER_SEND_CHANNEL_STATUS_TYPE_ID = 6;

    /**
     * The status of a receive channel endpoint represented as a counter value.
     */
    public static final int DRIVER_RECEIVE_CHANNEL_STATUS_TYPE_ID = 7;

    /**
     * The position the Sender can immediately send up-to on a session-channel-stream tuple.
     */
    public static final int DRIVER_SENDER_LIMIT_TYPE_ID = 9;

    /**
     * A counter per Image indicating presence of the congestion control.
     */
    public static final int DRIVER_PER_IMAGE_TYPE_ID = 10;

    /**
     * A counter for tracking the last heartbeat of an entity with a given registration id.
     */
    public static final int DRIVER_HEARTBEAT_TYPE_ID = 11;

    /**
     * The position in bytes a publication has reached appending to the log.
     * <p>
     * <b>Note:</b> This is a not a real-time value like the other and is updated one per second for monitoring
     * purposes.
     */
    public static final int DRIVER_PUBLISHER_POS_TYPE_ID = 12;

    /**
     * Count of back-pressure events (BPE)s a sender has experienced on a stream.
     */
    public static final int DRIVER_SENDER_BPE_TYPE_ID = 13;

    /**
     * Count of media driver neighbors for name resolution.
     */
    public static final int NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID = 15;

    /**
     * Count of entries in the name resolver cache.
     */
    public static final int NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID = 16;

    /**
     * Counter used to store the status of a bind address and port for the local end of a channel.
     * <p>
     * When the value is {@link ChannelEndpointStatus#ACTIVE} then the key value and label will be updated with the
     * socket address and port which is bound.
     */
    public static final int DRIVER_LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID = 14;

    // Archive counters
    /**
     * The position a recording has reached when being archived.
     */
    public static final int ARCHIVE_RECORDING_POSITION_TYPE_ID = 100;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
     */
    public static final int ARCHIVE_ERROR_COUNT_TYPE_ID = 101;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of concurrent control sessions.
     */
    public static final int ARCHIVE_CONTROL_SESSIONS_TYPE_ID = 102;

    // Cluster counters

    /**
     * Counter type id for the consensus module state.
     */
    public static final int CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID = 200;

    /**
     * Counter type id for the cluster node role.
     */
    public static final int CLUSTER_NODE_ROLE_TYPE_ID = 201;

    /**
     * Counter type id for the control toggle.
     */
    public static final int CLUSTER_CONTROL_TOGGLE_TYPE_ID = 202;

    /**
     * Counter type id of the commit position.
     */
    public static final int CLUSTER_COMMIT_POSITION_TYPE_ID = 203;

    /**
     * Counter representing the Recovery State for the cluster.
     */
    public static final int CLUSTER_RECOVERY_STATE_TYPE_ID = 204;

    /**
     * Counter type id for count of snapshots taken.
     */
    public static final int CLUSTER_SNAPSHOT_COUNTER_TYPE_ID = 205;

    /**
     * Type id for election state counter.
     */
    public static final int CLUSTER_ELECTION_STATE_TYPE_ID = 207;

    /**
     * The type id of the {@link Counter} used for the backup state.
     */
    public static final int CLUSTER_BACKUP_STATE_TYPE_ID = 208;

    /**
     * The type id of the {@link Counter} used for the live log position counter.
     */
    public static final int CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID = 209;

    /**
     * The type id of the {@link Counter} used for the next query deadline counter.
     */
    public static final int CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID = 210;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
     */
    public static final int CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID = 211;

    /**
     * Counter type id for the consensus module error count.
     */
    public static final int CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID = 212;

    /**
     * Counter type id for the number of cluster clients which have been timed out.
     */
    public static final int CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID = 213;

    /**
     * Counter type id for the number of invalid requests which the cluster has received.
     */
    public static final int CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID = 214;

    /**
     * Counter type id for the clustered service error count.
     */
    public static final int CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID = 215;

    private AeronCounters()
    {
    }
}
