/*
 * Copyright 2014 - 2016 Real Logic Ltd.
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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.driver.exceptions.ConfigurationException;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.BitUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ControllableIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.agrona.concurrent.status.StatusIndicator;

import java.util.concurrent.TimeUnit;

import static io.aeron.driver.ThreadingMode.DEDICATED;
import static java.lang.Integer.getInteger;
import static java.lang.Long.getLong;
import static java.lang.System.getProperty;

/**
 * Configuration options for the {@link MediaDriver}.
 */
public class Configuration
{
    /**
     * Property name for boolean value of term buffers should be created sparse.
     */
    public static final String TERM_BUFFER_SPARSE_FILE_PROP_NAME = "aeron.term.buffer.sparse.file";

    /**
     * Should term buffers be created as sparse files. Defaults to false.
     *
     * If a platform supports sparse files then log buffer creation is faster with pages being allocated as
     * needed. This can help for large numbers of channels/streams but can result in latency pauses.
     */
    public static final String TERM_BUFFER_SPARSE_FILE = getProperty(TERM_BUFFER_SPARSE_FILE_PROP_NAME);

    /**
     * Length (in bytes) of the log buffers for terms.
     */
    public static final String TERM_BUFFER_MAX_LENGTH_PROP_NAME = "aeron.term.buffer.max.length";

    /**
     * Default term max buffer length. The maximum possible term length is 1GB.
     */
    public static final int TERM_BUFFER_LENGTH_MAX_DEFAULT = 1024 * 1024 * 1024;

    /**
     * Length (in bytes) of the log buffers for publication terms.
     */
    public static final String TERM_BUFFER_LENGTH_PROP_NAME = "aeron.term.buffer.length";

    /**
     * Default term buffer length.
     */
    public static final int TERM_BUFFER_LENGTH_DEFAULT = 16 * 1024 * 1024;

    /**
     * Property name for term buffer length (in bytes) for IPC buffers.
     */
    public static final String IPC_TERM_BUFFER_LENGTH_PROP_NAME = "aeron.ipc.term.buffer.length";

    /**
     * Default IPC term buffer length.
     */
    public static final int TERM_BUFFER_IPC_LENGTH_DEFAULT = 64 * 1024 * 1024;

    /**
     * IPC Term buffer length in bytes.
     */
    public static final int IPC_TERM_BUFFER_LENGTH = getInteger(IPC_TERM_BUFFER_LENGTH_PROP_NAME, TERM_BUFFER_IPC_LENGTH_DEFAULT);

    /**
     * Property name low file storage warning threshold.
     */
    public static final String LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME = "aeron.low.file.store.warning.threshold";

    /**
     * Default value for low file storage warning threshold.
     */
    public static final long LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT = TERM_BUFFER_LENGTH_DEFAULT * 4L;

    /**
     * Default value for low file storage warning threshold.
     */
    public static final long LOW_FILE_STORE_WARNING_THRESHOLD =
        getLong(LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME, LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT);

    /**
     * Length (in bytes) of the conductor buffer for control commands from the clients to the media driver conductor.
     */
    public static final String CONDUCTOR_BUFFER_LENGTH_PROP_NAME = "aeron.conductor.buffer.length";

    /**
     * Default buffer length for conductor buffers between the client and the media driver conductor.
     */
    public static final int CONDUCTOR_BUFFER_LENGTH_DEFAULT = (1024 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;

    /**
     * Conductor buffer length in bytes.
     */
    public static final int CONDUCTOR_BUFFER_LENGTH = getInteger(
        CONDUCTOR_BUFFER_LENGTH_PROP_NAME, CONDUCTOR_BUFFER_LENGTH_DEFAULT);

    /**
     * Length (in bytes) of the broadcast buffers from the media driver to the clients.
     */
    public static final String TO_CLIENTS_BUFFER_LENGTH_PROP_NAME = "aeron.clients.buffer.length";

    /**
     * Default buffer length for broadcast buffers from the media driver and the clients.
     */
    public static final int TO_CLIENTS_BUFFER_LENGTH_DEFAULT = (1024 * 1024) + BroadcastBufferDescriptor.TRAILER_LENGTH;

    /**
     * Length for broadcast buffers from the media driver and the clients.
     */
    public static final int TO_CLIENTS_BUFFER_LENGTH = getInteger(
        TO_CLIENTS_BUFFER_LENGTH_PROP_NAME, TO_CLIENTS_BUFFER_LENGTH_DEFAULT);

    /**
     * Property name for length of the error buffer for the system counters.
     */
    public static final String COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME = "aeron.counters.buffer.length";

    /**
     * Default length of the memory mapped buffers for the system counters file.
     */
    public static final int COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

    /**
     * Length of the memory mapped buffers for the system counters file.
     */
    public static final int COUNTERS_VALUES_BUFFER_LENGTH = getInteger(
        COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME, COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT);

    public static final int COUNTERS_METADATA_BUFFER_LENGTH = COUNTERS_VALUES_BUFFER_LENGTH * 2;

    /**
     * Property name for length of the memory mapped buffer for the distinct error log.
     */
    public static final String ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.error.buffer.length";

    /**
     * Default buffer length for the error buffer for the media driver.
     */
    public static final int ERROR_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

    /**
     * Buffer length for the error buffer for the media driver.
     */
    public static final int ERROR_BUFFER_LENGTH = getInteger(ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT);

    /**
     * Property name for length of the initial window which must be sufficient for Bandwidth Delay Produce (BDP).
     */
    public static final String INITIAL_WINDOW_LENGTH_PROP_NAME = "aeron.rcv.initial.window.length";

    /**
     * Default initial window length for flow control sender to receiver purposes
     *
     * Length of Initial Window
     *
     * RTT (LAN) = 100 usec
     * Throughput = 10 Gbps
     *
     * Buffer = Throughput * RTT
     * Buffer = (10 * 1000 * 1000 * 1000 / 8) * 0.0001 = 125000
     * Round to 128KB
     */
    public static final int INITIAL_WINDOW_LENGTH_DEFAULT = 128 * 1024;

    /**
     * Property name for status message timeout in nanoseconds.
     */
    public static final String STATUS_MESSAGE_TIMEOUT_PROP_NAME = "aeron.rcv.status.message.timeout";

    /**
     * Max timeout between SMs.
     */
    public static final long STATUS_MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(200);

    /**
     * Property name for SO_RCVBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Produce (BDP).
     */
    public static final String SOCKET_RCVBUF_LENGTH_PROP_NAME = "aeron.socket.so_rcvbuf";

    /**
     * Default SO_RCVBUF length.
     */
    public static final int SOCKET_RCVBUF_LENGTH_DEFAULT = 128 * 1024;

    /**
     * SO_RCVBUF length, 0 means use OS default.
     */
    public static final int SOCKET_RCVBUF_LENGTH = getInteger(SOCKET_RCVBUF_LENGTH_PROP_NAME, SOCKET_RCVBUF_LENGTH_DEFAULT);

    /**
     * Property name for SO_SNDBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Produce (BDP).
     */
    public static final String SOCKET_SNDBUF_LENGTH_PROP_NAME = "aeron.socket.so_sndbuf";

    /**
     * Default SO_SNDBUF length.
     */
    public static final int SOCKET_SNDBUF_LENGTH_DEFAULT = 0;

    /**
     * SO_SNDBUF length, 0 means use OS default.
     */
    public static final int SOCKET_SNDBUF_LENGTH = getInteger(SOCKET_SNDBUF_LENGTH_PROP_NAME, SOCKET_SNDBUF_LENGTH_DEFAULT);

    /**
     * Property name for IP_MULTICAST_TTL setting on UDP sockets.
     */
    public static final String SOCKET_MULTICAST_TTL_PROP_NAME = "aeron.socket.multicast.ttl";

    /**
     * Multicast TTL value, 0 means use OS default.
     */
    public static final int SOCKET_MULTICAST_TTL_DEFAULT = 0;

    /**
     * Multicast TTL value.
     */
    public static final int SOCKET_MULTICAST_TTL = getInteger(SOCKET_MULTICAST_TTL_PROP_NAME, SOCKET_MULTICAST_TTL_DEFAULT);

    /**
     * Property name for linger timeout on {@link Publication}s.
     */
    public static final String PUBLICATION_LINGER_PROP_NAME = "aeron.publication.linger.timeout";

    /**
     * Default time for {@link Publication}s to linger before cleanup.
     */
    public static final long PUBLICATION_LINGER_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * Time for {@link Publication}s to linger before cleanup.
     */
    public static final long PUBLICATION_LINGER_NS = getLong(PUBLICATION_LINGER_PROP_NAME, PUBLICATION_LINGER_DEFAULT_NS);

    /**
     * Property name for {@link Aeron} client liveness timeout.
     */
    public static final String CLIENT_LIVENESS_TIMEOUT_PROP_NAME = "aeron.client.liveness.timeout";

    /**
     * Default timeout for client liveness in nanoseconds.
     */
    public static final long CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(5000);

    /**
     * Timeout for client liveness in nanoseconds.
     */
    public static final long CLIENT_LIVENESS_TIMEOUT_NS = getLong(
        CLIENT_LIVENESS_TIMEOUT_PROP_NAME, CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS);

    /**
     * Property name for {@link Image} liveness timeout.
     */
    public static final String IMAGE_LIVENESS_TIMEOUT_PROP_NAME = "aeron.image.liveness.timeout";

    /**
     * Default timeout for {@link Image} liveness in nanoseconds.
     */
    public static final long IMAGE_LIVENESS_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

    /**
     * Timeout for {@link Image} liveness in nanoseconds.
     */
    public static final long IMAGE_LIVENESS_TIMEOUT_NS = getLong(
        IMAGE_LIVENESS_TIMEOUT_PROP_NAME, IMAGE_LIVENESS_TIMEOUT_DEFAULT_NS);

    /**
     * Property name for window limit on {@link Publication} side.
     */
    public static final String PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME = "aeron.publication.term.window.length";

    /**
     * Publication term window length for flow control in bytes.
     */
    public static final int PUBLICATION_TERM_WINDOW_LENGTH = getInteger(PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME, 0);

    /**
     * Property name for window limit for IPC publications.
     */
    public static final String IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME = "aeron.ipc.publication.term.window.length";

    /**
     * IPC Publication term window length for flow control in bytes.
     */
    public static final int IPC_PUBLICATION_TERM_WINDOW_LENGTH = getInteger(
        IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME, 0);

    /**
     * Property name for {@link Publication} unblock timeout.
     */
    public static final String PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME = "aeron.publication.unblock.timeout";

    /**
     * Timeout for {@link Publication} unblock in nanoseconds.
     */
    public static final long PUBLICATION_UNBLOCK_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

    /**
     * Publication timeout for when to unblock a partially written message.
     */
    public static final long PUBLICATION_UNBLOCK_TIMEOUT_NS = getLong(
        PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME, PUBLICATION_UNBLOCK_TIMEOUT_DEFAULT_NS);

    private static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";

    static final long AGENT_IDLE_MAX_SPINS = 20;
    static final long AGENT_IDLE_MAX_YIELDS = 50;
    static final long AGENT_IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(1);
    static final long AGENT_IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(100);

    private static final String CONTROLLABLE_IDLE_STRATEGY = "org.agrona.concurrent.ControllableIdleStrategy";

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Sender} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String SENDER_IDLE_STRATEGY_PROP_NAME = "aeron.sender.idle.strategy";

    /**
     * {@link IdleStrategy} to be employed by {@link Sender} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String SENDER_IDLE_STRATEGY = getProperty(SENDER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link DriverConductor} for {@link ThreadingMode#DEDICATED}
     * and {@link ThreadingMode#SHARED_NETWORK}.
     */
    public static final String CONDUCTOR_IDLE_STRATEGY_PROP_NAME = "aeron.conductor.idle.strategy";

    /**
     * {@link IdleStrategy} to be employed by {@link DriverConductor} for {@link ThreadingMode#DEDICATED}
     * and {@link ThreadingMode#SHARED_NETWORK}.
     */
    public static final String CONDUCTOR_IDLE_STRATEGY = getProperty(CONDUCTOR_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Receiver} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String RECEIVER_IDLE_STRATEGY_PROP_NAME = "aeron.receiver.idle.strategy";

    /**
     * {@link IdleStrategy} to be employed by {@link Receiver} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String RECEIVER_IDLE_STRATEGY = getProperty(RECEIVER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Sender} and {@link Receiver} for
     * {@link ThreadingMode#SHARED_NETWORK}.
     */
    public static final String SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME = "aeron.sharednetwork.idle.strategy";

    /**
     * {@link IdleStrategy} to be employed by {@link Sender} and {@link Receiver} for
     * {@link ThreadingMode#SHARED_NETWORK}.
     */
    public static final String SHARED_NETWORK_IDLE_STRATEGY = getProperty(
        SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Sender}, {@link Receiver}, and {@link DriverConductor}
     * for {@link ThreadingMode#SHARED}.
     */
    public static final String SHARED_IDLE_STRATEGY_PROP_NAME = "aeron.shared.idle.strategy";

    /**
     * {@link IdleStrategy} to be employed by {@link Sender}, {@link Receiver}, and {@link DriverConductor}
     * for {@link ThreadingMode#SHARED}.
     */
    public static final String SHARED_IDLE_STRATEGY = getProperty(SHARED_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * Property name for {@link FlowControl} to be employed for unicast channels.
     */
    public static final String UNICAST_FLOW_CONTROL_STRATEGY_PROP_NAME = "aeron.unicast.flow.control.strategy";

    /**
     * {@link FlowControl} to be employed for unicast channels.
     */
    public static final String UNICAST_FLOW_CONTROL_STRATEGY = getProperty(
        UNICAST_FLOW_CONTROL_STRATEGY_PROP_NAME, "io.aeron.driver.UnicastFlowControl");

    /**
     * Property name for {@link FlowControl} to be employed for multicast channels.
     */
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY_PROP_NAME = "aeron.multicast.flow.control.strategy";

    /**
     * {@link FlowControl} to be employed for multicast channels.
     */
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY = getProperty(
        MULTICAST_FLOW_CONTROL_STRATEGY_PROP_NAME, "io.aeron.driver.MaxMulticastFlowControl");

    /**
     * Property name for {@link FlowControlSupplier} to be employed for unicast channels.
     */
    public static final String UNICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME = "aeron.unicast.FlowControl.supplier";

    /**
     * {@link FlowControlSupplier} to be employed for unicast channels.
     */
    public static final String UNICAST_FLOW_CONTROL_STRATEGY_SUPPLIER = getProperty(
        UNICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME, "io.aeron.driver.DefaultUnicastFlowControlSupplier");

    /**
     * Property name for {@link FlowControlSupplier} to be employed for unicast channels.
     */
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME = "aeron.multicast.FlowControl.supplier";

    /**
     * {@link FlowControlSupplier} to be employed for multicast channels.
     */
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER = getProperty(
        MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME, "io.aeron.driver.DefaultMulticastFlowControlSupplier");

    /**
     * Length of the maximum transmission unit of the media driver's protocol
     */
    public static final String MTU_LENGTH_PROP_NAME = "aeron.mtu.length";

    /**
     * Default length is greater than typical Ethernet MTU so will fragment to save on system calls.
     */
    public static final int MTU_LENGTH_DEFAULT = 4096;

    /**
     * Length of the MTU to use for sending messages.
     */
    public static final int MTU_LENGTH = getInteger(MTU_LENGTH_PROP_NAME, MTU_LENGTH_DEFAULT);

    /**
     * Maximum UDP datagram payload size for IPv4. Jumbo datagrams from IPv6 are not supported.
     */
    public static final int MAX_UDP_PAYLOAD_LENGTH = 65507;

    /**
     * {@link ThreadingMode} to be used by the Aeron {@link MediaDriver}
     */
    public static final String THREADING_MODE_PROP_NAME = "aeron.threading.mode";
    static final ThreadingMode THREADING_MODE_DEFAULT = ThreadingMode.valueOf(
        getProperty(THREADING_MODE_PROP_NAME, DEDICATED.name()));

    /**
     * How often to check liveness and cleanup
     */
    public static final long HEARTBEAT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Property name for {@link SendChannelEndpointSupplier}.
     */
    public static final String SEND_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME = "aeron.SendChannelEndpoint.supplier";

    /**
     *{@link SendChannelEndpointSupplier} to provide endpoint extension behaviour.
     */
    public static final String SEND_CHANNEL_ENDPOINT_SUPPLIER = getProperty(
        SEND_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME, "io.aeron.driver.DefaultSendChannelEndpointSupplier");

    /**
     * Property name for {@link ReceiveChannelEndpointSupplier}.
     */
    public static final String RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME = "aeron.ReceiveChannelEndpoint.supplier";

    /**
     * {@link ReceiveChannelEndpointSupplier} to provide endpoint extension behaviour.
     */
    public static final String RECEIVE_CHANNEL_ENDPOINT_SUPPLIER = getProperty(
        RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME, "io.aeron.driver.DefaultReceiveChannelEndpointSupplier");

    /**
     * Capacity for the command queues used between driver agents.
     */
    public static final int CMD_QUEUE_CAPACITY = 1024;

    /**
     * Timeout on cleaning up pending SETUP state on subscriber.
     */
    public static final long PENDING_SETUPS_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(1000);

    /**
     * Timeout between SETUP frames for publications during initial setup phase.
     */
    public static final long PUBLICATION_SETUP_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(100);

    /**
     * Timeout between heartbeats for publications.
     */
    public static final long PUBLICATION_HEARTBEAT_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(100);

    /**
     * Default group size estimate for NAK delay randomization.
     */
    public static final int NAK_GROUPSIZE_DEFAULT = 10;

    /**
     * Default group RTT estimate for NAK delay randomization in ms.
     */
    public static final int NAK_GRTT_DEFAULT = 10;

    /**
     * Default max backoff for NAK delay randomization in ms.
     */
    public static final long NAK_MAX_BACKOFF_DEFAULT = TimeUnit.MILLISECONDS.toNanos(60);

    /**
     * Multicast NAK delay is immediate initial with delayed subsequent delay.
     */
    public static final OptimalMulticastDelayGenerator NAK_MULTICAST_DELAY_GENERATOR = new OptimalMulticastDelayGenerator(
        NAK_MAX_BACKOFF_DEFAULT, NAK_GROUPSIZE_DEFAULT, NAK_GRTT_DEFAULT);

    /**
     * Default Unicast NAK delay in nanoseconds.
     */
    public static final long NAK_UNICAST_DELAY_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(60);

    /**
     * Unicast NAK delay is immediate initial with delayed subsequent delay.
     */
    public static final StaticDelayGenerator NAK_UNICAST_DELAY_GENERATOR = new StaticDelayGenerator(
        NAK_UNICAST_DELAY_DEFAULT_NS, true);

    /**
     * Default delay for retransmission of data for unicast.
     */
    public static final long RETRANSMIT_UNICAST_DELAY_DEFAULT_NS = TimeUnit.NANOSECONDS.toNanos(0);

    /**
     * Source uses same for unicast and multicast. For ticks.
     */
    public static final FeedbackDelayGenerator RETRANSMIT_UNICAST_DELAY_GENERATOR = () -> RETRANSMIT_UNICAST_DELAY_DEFAULT_NS;

    /**
     * Default delay for linger for unicast.
     */
    public static final long RETRANSMIT_UNICAST_LINGER_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(60);

    /**
     * Delay for linger for unicast.
     */
    public static final FeedbackDelayGenerator RETRANSMIT_UNICAST_LINGER_GENERATOR = () -> RETRANSMIT_UNICAST_LINGER_DEFAULT_NS;

    /**
     * Default max number of active retransmissions per connected stream.
     */
    public static final int MAX_RETRANSMITS_DEFAULT = 16;

    /**
     * Validate the the term buffer length is a power of two.
     *
     * @param length of the term buffer
     */
    public static void validateTermBufferLength(final int length)
    {
        if (!BitUtil.isPowerOfTwo(length))
        {
            throw new IllegalStateException("Term buffer length must be a positive power of 2: " + length);
        }
    }

    /**
     * How far ahead the publisher can get from the sender position.
     *
     * @param termBufferLength to be used when {@link #PUBLICATION_TERM_WINDOW_LENGTH} is not set.
     * @return the length to be used for the publication window.
     */
    public static int publicationTermWindowLength(final int termBufferLength)
    {
        int publicationTermWindowLength = termBufferLength / 2;

        if (0 != PUBLICATION_TERM_WINDOW_LENGTH)
        {
            publicationTermWindowLength = Math.min(PUBLICATION_TERM_WINDOW_LENGTH, publicationTermWindowLength);
        }

        return publicationTermWindowLength;
    }

    /**
     * How far ahead the publisher can get from the sender position for IPC only.
     *
     * @param termBufferLength to be used when {@link #IPC_PUBLICATION_TERM_WINDOW_LENGTH} is not set.
     * @return the length to be used for the publication window.
     */
    public static int ipcPublicationTermWindowLength(final int termBufferLength)
    {
        int publicationTermWindowLength = termBufferLength;

        if (0 != IPC_PUBLICATION_TERM_WINDOW_LENGTH)
        {
            publicationTermWindowLength = Math.min(IPC_PUBLICATION_TERM_WINDOW_LENGTH, publicationTermWindowLength);
        }

        return publicationTermWindowLength;
    }

    /**
     * How large the term buffer should be for IPC only.
     *
     * @param termBufferLength to be used when {@link #IPC_TERM_BUFFER_LENGTH} is not set.
     * @return the length to be used for the term buffer in bytes
     */
    public static int ipcTermBufferLength(final int termBufferLength)
    {
        return 0 != IPC_TERM_BUFFER_LENGTH ? IPC_TERM_BUFFER_LENGTH : termBufferLength;
    }

    /**
     * Validate that the initial window length is suitably greater than MTU.
     *
     * @param initialWindowLength to be validated.
     * @param mtuLength           against which to validate.
     */
    public static void validateInitialWindowLength(final int initialWindowLength, final int mtuLength)
    {
        if (mtuLength > initialWindowLength)
        {
            throw new IllegalStateException("Initial window length must be >= to MTU length: " + mtuLength);
        }
    }

    /**
     * Get the {@link IdleStrategy} that should be applied to {@link org.agrona.concurrent.Agent}s.
     *
     * @param strategyName       to be created.
     * @param controllableStatus status indicator for what the strategy should do.
     * @return the newly created IdleStrategy.
     */
    public static IdleStrategy agentIdleStrategy(final String strategyName, final StatusIndicator controllableStatus)
    {
        IdleStrategy idleStrategy = null;

        switch (strategyName)
        {
            case DEFAULT_IDLE_STRATEGY:
                idleStrategy = new BackoffIdleStrategy(
                    AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS, AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS);
                break;

            case CONTROLLABLE_IDLE_STRATEGY:
                idleStrategy = new ControllableIdleStrategy(controllableStatus);
                controllableStatus.setOrdered(ControllableIdleStrategy.PARK);
                break;

            default:
                try
                {
                    idleStrategy = (IdleStrategy)Class.forName(strategyName).newInstance();
                }
                catch (final Exception ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
                break;
        }

        return idleStrategy;
    }

    static IdleStrategy senderIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(SENDER_IDLE_STRATEGY, controllableStatus);
    }

    static IdleStrategy conductorIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(CONDUCTOR_IDLE_STRATEGY, controllableStatus);
    }

    static IdleStrategy receiverIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(RECEIVER_IDLE_STRATEGY, controllableStatus);
    }

    static IdleStrategy sharedNetworkIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(SHARED_NETWORK_IDLE_STRATEGY, controllableStatus);
    }

    static IdleStrategy sharedIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(SHARED_IDLE_STRATEGY, controllableStatus);
    }

    static int termBufferLength()
    {
        return getInteger(TERM_BUFFER_LENGTH_PROP_NAME, TERM_BUFFER_LENGTH_DEFAULT);
    }

    static int maxTermBufferLength()
    {
        return getInteger(TERM_BUFFER_MAX_LENGTH_PROP_NAME, TERM_BUFFER_LENGTH_MAX_DEFAULT);
    }

    static int initialWindowLength()
    {
        return getInteger(INITIAL_WINDOW_LENGTH_PROP_NAME, INITIAL_WINDOW_LENGTH_DEFAULT);
    }

    static long statusMessageTimeout()
    {
        return getLong(STATUS_MESSAGE_TIMEOUT_PROP_NAME, STATUS_MESSAGE_TIMEOUT_DEFAULT_NS);
    }

    /**
     * Get the supplier of {@link SendChannelEndpoint}s which can be used for
     * debugging, monitoring, or modifying the behaviour when sending to the media channel.
     *
     * @return the {@link SendChannelEndpointSupplier}.
     */
    static SendChannelEndpointSupplier sendChannelEndpointSupplier()
    {
        SendChannelEndpointSupplier supplier = null;
        try
        {
            supplier = (SendChannelEndpointSupplier)Class.forName(SEND_CHANNEL_ENDPOINT_SUPPLIER).newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link ReceiveChannelEndpoint}s which can be used for
     * debugging, monitoring, or modifying the behaviour when receiving from the media channel.
     *
     * @return the {@link SendChannelEndpointSupplier}.
     */
    static ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier()
    {
        ReceiveChannelEndpointSupplier supplier = null;
        try
        {
            supplier = (ReceiveChannelEndpointSupplier)Class.forName(RECEIVE_CHANNEL_ENDPOINT_SUPPLIER).newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link FlowControl}s which can be used for changing behavior of flow control for unicast
     * publications.
     *
     * @return the {@link FlowControlSupplier}.
     */
    static FlowControlSupplier unicastFlowControlSupplier()
    {
        FlowControlSupplier supplier = null;
        try
        {
            supplier = (FlowControlSupplier)Class.forName(UNICAST_FLOW_CONTROL_STRATEGY_SUPPLIER).newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link FlowControl}s which can be used for changing behavior of flow control for unicast
     * publications.
     *
     * @return the {@link FlowControlSupplier}.
     */
    static FlowControlSupplier multicastFlowControlSupplier()
    {
        FlowControlSupplier supplier = null;
        try
        {
            supplier = (FlowControlSupplier)Class.forName(MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER).newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Validate the the MTU is an appropriate length. MTU lengths must be a multiple of {@link FrameDescriptor#FRAME_ALIGNMENT}.
     *
     * @param mtuLength to be validated.
     * @throws ConfigurationException if the MTU length is not valid.
     */
    public static void validateMtuLength(final int mtuLength)
    {
        if (mtuLength < 0 || mtuLength > MAX_UDP_PAYLOAD_LENGTH)
        {
            throw new ConfigurationException(
                "mtuLength must be a > 0 and <= " + MAX_UDP_PAYLOAD_LENGTH + ": mtuLength=" + mtuLength);
        }

        if ((mtuLength % FrameDescriptor.FRAME_ALIGNMENT) != 0)
        {
            throw new ConfigurationException(String.format(
                "mtuLength must be a multiple of %d: mtuLength=%d",
                FrameDescriptor.FRAME_ALIGNMENT, mtuLength));
        }
    }
}
