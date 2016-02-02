/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.driver.event.EventLogger;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBufferDescriptor;

import java.util.concurrent.TimeUnit;

import static java.lang.Integer.getInteger;
import static java.lang.Long.getLong;
import static java.lang.System.getProperty;
import static uk.co.real_logic.aeron.driver.ThreadingMode.DEDICATED;

/**
 * Configuration options for the media driver.
 */
public class Configuration
{
    private static final String DEFAULT_IDLE_STRATEGY = "uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy";

    /**
     * Byte buffer length (in bytes) for reads
     */
    public static final String RECEIVE_BUFFER_LENGTH_PROP_NAME = "aeron.rcv.buffer.length";

    /**
     * Length (in bytes) of the log buffers for publication terms
     */
    public static final String TERM_BUFFER_LENGTH_PROP_NAME = "aeron.term.buffer.length";

    /**
     * Length (in bytes) of the log buffers for terms for incoming images
     */
    public static final String TERM_BUFFER_MAX_LENGTH_PROP_NAME = "aeron.term.buffer.max.length";

    /**
     * Length (in bytes) of the conductor buffers between the media driver and the client
     */
    public static final String CONDUCTOR_BUFFER_LENGTH_PROP_NAME = "aeron.conductor.buffer.length";

    /**
     * Length (in bytes) of the broadcast buffers from the media driver to the clients
     */
    public static final String TO_CLIENTS_BUFFER_LENGTH_PROP_NAME = "aeron.clients.buffer.length";

    /**
     * Property name for length of the memory mapped buffers for the counters file
     */
    public static final String COUNTER_VALUES_BUFFER_LENGTH_PROP_NAME = "aeron.dir.counters.length";

    /**
     * Property name for length of the initial window  which must be sufficient for Bandwidth Delay Produce (BDP).
     */
    public static final String INITIAL_WINDOW_LENGTH_PROP_NAME = "aeron.rcv.initial.window.length";

    /**
     * Property name for status message timeout in nanoseconds
     */
    public static final String STATUS_MESSAGE_TIMEOUT_PROP_NAME = "aeron.rcv.status.message.timeout";

    /**
     * Property name for SO_RCVBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Produce (BDP).
     */
    public static final String SOCKET_RCVBUF_LENGTH_PROP_NAME = "aeron.socket.so_rcvbuf";

    /**
     * Property name for SO_SNDBUF setting on UDP sockets  which must be sufficient for Bandwidth Delay Produce (BDP).
     */
    public static final String SOCKET_SNDBUF_LENGTH_PROP_NAME = "aeron.socket.so_sndbuf";

    /**
     * Property name for linger timeout for publications
     */
    public static final String PUBLICATION_LINGER_PROP_NAME = "aeron.publication.linger.timeout";

    /**
     * Property name for window limit on publication side
     */
    public static final String PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME = "aeron.publication.term.window.length";
    public static final int PUBLICATION_TERM_WINDOW_LENGTH = getInteger(PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME, 0);

    /**
     * Property name for client liveness timeout
     */
    public static final String CLIENT_LIVENESS_TIMEOUT_PROP_NAME = "aeron.client.liveness.timeout";

    /**
     * Property name for image liveness timeout
     */
    public static final String IMAGE_LIVENESS_TIMEOUT_PROP_NAME = "aeron.image.liveness.timeout";

    /**
     * Property name for publication unblock timeout
     */
    public static final String PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME = "aeron.publication.unblock.timeout";

    /**
     * Property name for data loss rate
     */
    public static final String DATA_LOSS_RATE_PROP_NAME = "aeron.debug.data.loss.rate";

    /**
     * Property name for data loss seed
     */
    public static final String DATA_LOSS_SEED_PROP_NAME = "aeron.debug.data.loss.seed";

    /**
     * Property name for control loss rate
     */
    public static final String CONTROL_LOSS_RATE_PROP_NAME = "aeron.debug.control.loss.rate";

    /**
     * Property name for control loss seed
     */
    public static final String CONTROL_LOSS_SEED_PROP_NAME = "aeron.debug.control.loss.seed";

    /**
     * Default term buffer length.
     */
    public static final int TERM_BUFFER_IPC_LENGTH_DEFAULT = 64 * 1024 * 1024;

    /**
     * Property name for term buffer length (in bytes) for IPC logbuffers
     */
    public static final String IPC_TERM_BUFFER_LENGTH_PROP_NAME = "aeron.ipc.term.buffer.length";
    public static final int IPC_TERM_BUFFER_LENGTH = getInteger(IPC_TERM_BUFFER_LENGTH_PROP_NAME, TERM_BUFFER_IPC_LENGTH_DEFAULT);

    /**
     * Property name for window limit for IPC publications
     */
    public static final String IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME = "aeron.ipc.publication.term.window.length";
    public static final int IPC_PUBLICATION_TERM_WINDOW_LENGTH = getInteger(
        IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME, 0);

    /**
     * Default byte buffer length for reads
     */
    public static final int RECEIVE_BYTE_BUFFER_LENGTH_DEFAULT = 4096;
    public static final int RECEIVE_BYTE_BUFFER_LENGTH = getInteger(
        RECEIVE_BUFFER_LENGTH_PROP_NAME, RECEIVE_BYTE_BUFFER_LENGTH_DEFAULT);

    /**
     * Default term buffer length.
     */
    public static final int TERM_BUFFER_LENGTH_DEFAULT = 16 * 1024 * 1024;

    /**
     * Default term max buffer length.
     */
    public static final int TERM_BUFFER_LENGTH_MAX_DEFAULT = 16 * 1024 * 1024;

    /**
     * Default buffer length for conductor buffers between the media driver and the client
     */
    public static final int CONDUCTOR_BUFFER_LENGTH_DEFAULT = 1024 * 1024 + RingBufferDescriptor.TRAILER_LENGTH;
    public static final int CONDUCTOR_BUFFER_LENGTH = getInteger(
        CONDUCTOR_BUFFER_LENGTH_PROP_NAME, CONDUCTOR_BUFFER_LENGTH_DEFAULT);

    /**
     * Default buffer length for broadcast buffers from the media driver to the clients
     */
    public static final int TO_CLIENTS_BUFFER_LENGTH_DEFAULT = 1024 * 1024 + BroadcastBufferDescriptor.TRAILER_LENGTH;
    public static final int TO_CLIENTS_BUFFER_LENGTH = getInteger(
        TO_CLIENTS_BUFFER_LENGTH_PROP_NAME, TO_CLIENTS_BUFFER_LENGTH_DEFAULT);

    /**
     * Length of the memory mapped buffers for the counters file
     */
    public static final int COUNTER_VALUES_BUFFER_LENGTH_DEFAULT = 1024 * 1024;
    public static final int COUNTER_VALUES_BUFFER_LENGTH = getInteger(
        COUNTER_VALUES_BUFFER_LENGTH_PROP_NAME, COUNTER_VALUES_BUFFER_LENGTH_DEFAULT);

    public static final int COUNTER_LABELS_BUFFER_LENGTH = COUNTER_VALUES_BUFFER_LENGTH;

    /**
     * Default group size estimate for NAK delay randomization
     */
    public static final int NAK_GROUPSIZE_DEFAULT = 10;

    /**
     * Default group RTT estimate for NAK delay randomization in msec
     */
    public static final int NAK_GRTT_DEFAULT = 10;

    /**
     * Default max backoff for NAK delay randomization in msec
     */
    public static final long NAK_MAX_BACKOFF_DEFAULT = TimeUnit.MILLISECONDS.toNanos(60);
    /**
     * Multicast NAK delay is immediate initial with delayed subsequent delay
     */
    public static final OptimalMulticastDelayGenerator NAK_MULTICAST_DELAY_GENERATOR = new OptimalMulticastDelayGenerator(
        NAK_MAX_BACKOFF_DEFAULT, NAK_GROUPSIZE_DEFAULT, NAK_GRTT_DEFAULT);

    /**
     * Default Unicast NAK delay in nanoseconds
     */
    public static final long NAK_UNICAST_DELAY_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(60);
    /**
     * Unicast NAK delay is immediate initial with delayed subsequent delay
     */
    public static final StaticDelayGenerator NAK_UNICAST_DELAY_GENERATOR = new StaticDelayGenerator(
        NAK_UNICAST_DELAY_DEFAULT_NS, true);

    /**
     * Default delay for retransmission of data for unicast
     */
    public static final long RETRANSMIT_UNICAST_DELAY_DEFAULT_NS = TimeUnit.NANOSECONDS.toNanos(0);
    /**
     * Source uses same for unicast and multicast. For ticks.
     */
    public static final FeedbackDelayGenerator RETRANSMIT_UNICAST_DELAY_GENERATOR = () -> RETRANSMIT_UNICAST_DELAY_DEFAULT_NS;

    /**
     * Default delay for linger for unicast
     */
    public static final long RETRANSMIT_UNICAST_LINGER_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(60);
    public static final FeedbackDelayGenerator RETRANSMIT_UNICAST_LINGER_GENERATOR = () -> RETRANSMIT_UNICAST_LINGER_DEFAULT_NS;

    /**
     * Default max number of active retransmissions per Term
     */
    public static final int MAX_RETRANSMITS_DEFAULT = 16;

    /**
     * Default initial window length for flow control sender to receiver purposes
     * <p>
     * Length of Initial Window
     * <p>
     * RTT (LAN) = 100 usec
     * Throughput = 10 Gbps
     * <p>
     * Buffer = Throughput * RTT
     * Buffer = (10*1000*1000*1000/8) * 0.0001 = 125000
     * Round to 128KB
     */
    public static final int INITIAL_WINDOW_LENGTH_DEFAULT = 128 * 1024;

    /**
     * Max timeout between SMs.
     */
    public static final long STATUS_MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(200);

    /**
     * 0 means use OS default.
     */
    public static final int SOCKET_RCVBUF_LENGTH_DEFAULT = 128 * 1024;
    public static final int SOCKET_RCVBUF_LENGTH = getInteger(SOCKET_RCVBUF_LENGTH_PROP_NAME, SOCKET_RCVBUF_LENGTH_DEFAULT);

    /**
     * 0 means use OS default.
     */
    public static final int SOCKET_SNDBUF_LENGTH_DEFAULT = 0;
    public static final int SOCKET_SNDBUF_LENGTH = getInteger(SOCKET_SNDBUF_LENGTH_PROP_NAME, SOCKET_SNDBUF_LENGTH_DEFAULT);

    /**
     * Time for publications to linger before cleanup
     */
    public static final long PUBLICATION_LINGER_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);
    public static final long PUBLICATION_LINGER_NS = getLong(PUBLICATION_LINGER_PROP_NAME, PUBLICATION_LINGER_DEFAULT_NS);

    /**
     * Timeout for client liveness in nanoseconds
     */
    public static final long CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(5000);
    public static final long CLIENT_LIVENESS_TIMEOUT_NS = getLong(
        CLIENT_LIVENESS_TIMEOUT_PROP_NAME, CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS);
    /**
     * Timeout for connection liveness in nanoseconds
     */
    public static final long IMAGE_LIVENESS_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);
    public static final long IMAGE_LIVENESS_TIMEOUT_NS = getLong(
        IMAGE_LIVENESS_TIMEOUT_PROP_NAME, IMAGE_LIVENESS_TIMEOUT_DEFAULT_NS);

    /**
     * Timeout for publication unblock in nanoseconds
     */
    public static final long PUBLICATION_UNBLOCK_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);
    public static final long PUBLICATION_UNBLOCK_TIMEOUT_NS = getLong(
        PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME, PUBLICATION_UNBLOCK_TIMEOUT_DEFAULT_NS);

    public static final long AGENT_IDLE_MAX_SPINS = 20;
    public static final long AGENT_IDLE_MAX_YIELDS = 50;
    public static final long AGENT_IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(1);
    public static final long AGENT_IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(100);

    /**
     * {@link IdleStrategy} to be employed by {@link Sender} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String SENDER_IDLE_STRATEGY_PROP_NAME = "aeron.sender.idle.strategy";
    public static final String SENDER_IDLE_STRATEGY = getProperty(
        SENDER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * {@link IdleStrategy} to be employed by {@link DriverConductor} for {@link ThreadingMode#DEDICATED}
     * and {@link ThreadingMode#SHARED_NETWORK}.
     */
    public static final String CONDUCTOR_IDLE_STRATEGY_PROP_NAME = "aeron.conductor.idle.strategy";
    public static final String CONDUCTOR_IDLE_STRATEGY = getProperty(
        CONDUCTOR_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * {@link IdleStrategy} to be employed by {@link Receiver} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String RECEIVER_IDLE_STRATEGY_PROP_NAME = "aeron.receiver.idle.strategy";
    public static final String RECEIVER_IDLE_STRATEGY = getProperty(
        RECEIVER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * {@link IdleStrategy} to be employed by {@link Sender} and {@link Receiver} for
     * {@link ThreadingMode#SHARED_NETWORK}.
     */
    public static final String SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME = "aeron.sharednetwork.idle.strategy";
    public static final String SHARED_NETWORK_IDLE_STRATEGY = getProperty(
        SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * {@link IdleStrategy} to be employed by {@link Sender}, {@link Receiver}, and {@link DriverConductor}
     * for {@link ThreadingMode#SHARED}.
     */
    public static final String SHARED_IDLE_STRATEGY_PROP_NAME = "aeron.shared.idle.strategy";
    public static final String SHARED_IDLE_STRATEGY = getProperty(
        SHARED_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /** Capacity for the command queues used between driver agents. */
    public static final int CMD_QUEUE_CAPACITY = 1024;

    /** Timeout on cleaning up pending SETUP state on subscriber */
    public static final long PENDING_SETUPS_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(1000);

    /** Timeout between SETUP frames for publications during initial setup phase */
    public static final long PUBLICATION_SETUP_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(100);

    /** Timeout between heartbeats for publications */
    public static final long PUBLICATION_HEARTBEAT_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(100);

    /**
     * {@link FlowControl} to be employed for unicast channels.
     */
    public static final String UNICAST_FLOW_CONTROL_STRATEGY_PROP_NAME =
        "aeron.unicast.flow.control.strategy";
    public static final String UNICAST_FLOW_CONTROL_STRATEGY = getProperty(
        UNICAST_FLOW_CONTROL_STRATEGY_PROP_NAME, "uk.co.real_logic.aeron.driver.UnicastFlowControl");

    /**
     * {@link FlowControl} to be employed for multicast channels.
     */
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY_PROP_NAME =
        "aeron.multicast.flow.control.strategy";
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY = getProperty(
        MULTICAST_FLOW_CONTROL_STRATEGY_PROP_NAME, "uk.co.real_logic.aeron.driver.MaxMulticastFlowControl");

    /** Length of the maximum transport unit of the media driver's protocol */
    public static final String MTU_LENGTH_PROP_NAME = "aeron.mtu.length";
    public static final int MTU_LENGTH_DEFAULT = 4096;
    public static final int MTU_LENGTH = getInteger(MTU_LENGTH_PROP_NAME, MTU_LENGTH_DEFAULT);

    public static final String THREADING_MODE_PROP_NAME = "aeron.threading.mode";
    public static final String THREADING_MODE_DEFAULT = DEDICATED.name();

    /**
     * how often to check liveness and cleanup
     */
    public static final long HEARTBEAT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    public static final String SEND_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME = "aeron.SendChannelEndpoint.supplier";

    public static final String SEND_CHANNEL_ENDPOINT_SUPPLIER_DEFAULT =
        EventLogger.IS_FRAME_LOGGING_ENABLED ?
            "uk.co.real_logic.aeron.driver.DebugSendChannelEndpointSupplier" :
            "uk.co.real_logic.aeron.driver.DefaultSendChannelEndpointSupplier";

    public static final String SEND_CHANNEL_ENDPOINT_SUPPLIER = getProperty(
        SEND_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME, SEND_CHANNEL_ENDPOINT_SUPPLIER_DEFAULT);

    public static final String RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME = "aeron.ReceiveChannelEndpoint.supplier";

    public static final String RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_DEFAULT =
        EventLogger.IS_FRAME_LOGGING_ENABLED ?
            "uk.co.real_logic.aeron.driver.DebugReceiveChannelEndpointSupplier" :
            "uk.co.real_logic.aeron.driver.DefaultReceiveChannelEndpointSupplier";

    public static final String RECEIVE_CHANNEL_ENDPOINT_SUPPLIER = getProperty(
        RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME, RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_DEFAULT);

    /**
     * How far ahead the publisher can get from the sender position.
     *
     * @param termBufferLength to be used when {@link #PUBLICATION_TERM_WINDOW_LENGTH} is not set.
     * @return the length to be used for the publication window.
     */
    public static int publicationTermWindowLength(final int termBufferLength)
    {
        return 0 != PUBLICATION_TERM_WINDOW_LENGTH ? PUBLICATION_TERM_WINDOW_LENGTH : termBufferLength / 2;
    }

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
     * Validate that the initial window length is suitably greater than MTU.
     *
     * @param initialWindowLength to be validated.
     * @param mtuLength against which to validate.
     */
    public static void validateInitialWindowLength(final int initialWindowLength, final int mtuLength)
    {
        if (mtuLength > initialWindowLength)
        {
            throw new IllegalStateException("Initial window length must be >= to MTU length: " + mtuLength);
        }
    }

    public static IdleStrategy agentIdleStrategy(final String name)
    {
        IdleStrategy idleStrategy = null;

        if (name.equals(DEFAULT_IDLE_STRATEGY))
        {
            idleStrategy = new BackoffIdleStrategy(
                AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS, AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS);
        }
        else
        {
            try
            {
                idleStrategy = (IdleStrategy)Class.forName(name).newInstance();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        return idleStrategy;
    }

    public static IdleStrategy senderIdleStrategy()
    {
        return agentIdleStrategy(SENDER_IDLE_STRATEGY);
    }

    public static IdleStrategy conductorIdleStrategy()
    {
        return agentIdleStrategy(CONDUCTOR_IDLE_STRATEGY);
    }

    public static IdleStrategy receiverIdleStrategy()
    {
        return agentIdleStrategy(RECEIVER_IDLE_STRATEGY);
    }

    public static IdleStrategy sharedNetworkIdleStrategy()
    {
        return agentIdleStrategy(SHARED_NETWORK_IDLE_STRATEGY);
    }

    public static IdleStrategy sharedIdleStrategy()
    {
        return agentIdleStrategy(SHARED_IDLE_STRATEGY);
    }

    public static FlowControl unicastFlowControlStrategy()
    {
        FlowControl flowControl = null;
        try
        {
            flowControl = (FlowControl)Class.forName(UNICAST_FLOW_CONTROL_STRATEGY).newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return flowControl;
    }

    public static FlowControl multicastFlowControlStrategy()
    {
        FlowControl flowControl = null;
        try
        {
            flowControl = (FlowControl)Class.forName(MULTICAST_FLOW_CONTROL_STRATEGY).newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return flowControl;
    }

    public static int termBufferLength()
    {
        return getInteger(TERM_BUFFER_LENGTH_PROP_NAME, TERM_BUFFER_LENGTH_DEFAULT);
    }

    public static int maxTermBufferLength()
    {
        return getInteger(TERM_BUFFER_MAX_LENGTH_PROP_NAME, TERM_BUFFER_LENGTH_MAX_DEFAULT);
    }

    public static int initialWindowLength()
    {
        return getInteger(INITIAL_WINDOW_LENGTH_PROP_NAME, INITIAL_WINDOW_LENGTH_DEFAULT);
    }

    public static long statusMessageTimeout()
    {
        return getLong(STATUS_MESSAGE_TIMEOUT_PROP_NAME, STATUS_MESSAGE_TIMEOUT_DEFAULT_NS);
    }

    public static long dataLossSeed()
    {
        return getLong(DATA_LOSS_SEED_PROP_NAME, -1);
    }

    public static long controlLossSeed()
    {
        return getLong(CONTROL_LOSS_SEED_PROP_NAME, -1);
    }

    public static double dataLossRate()
    {
        return Double.parseDouble(getProperty(DATA_LOSS_RATE_PROP_NAME, "0.0"));
    }

    public static double controlLossRate()
    {
        return Double.parseDouble(getProperty(CONTROL_LOSS_RATE_PROP_NAME, "0.0"));
    }

    public static LossGenerator createLossGenerator(final double lossRate, final long lossSeed)
    {
        if (0 == lossRate)
        {
            return (address, buffer, length) -> false;
        }

        return new RandomLossGenerator(lossRate, lossSeed);
    }

    public static ThreadingMode threadingMode()
    {
        return ThreadingMode.valueOf(getProperty(THREADING_MODE_PROP_NAME, THREADING_MODE_DEFAULT));
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
     * How far ahead the publisher can get from the sender position for IPC only.
     *
     * @param termBufferLength to be used when {@link #IPC_PUBLICATION_TERM_WINDOW_LENGTH} is not set.
     * @return the length to be used for the publication window.
     */
    public static int ipcPublicationTermWindowLength(final int termBufferLength)
    {
        return 0 != IPC_PUBLICATION_TERM_WINDOW_LENGTH ? IPC_PUBLICATION_TERM_WINDOW_LENGTH : termBufferLength / 2;
    }

    /**
     * Get the supplier of {@link uk.co.real_logic.aeron.driver.media.SendChannelEndpoint}s which can be used for
     * debugging, monitoring, or modifying the behaviour when sending to the media channel.
     *
     * @return the {@link SendChannelEndpointSupplier}.
     */
    public static SendChannelEndpointSupplier sendChannelEndpointSupplier()
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
     * Get the supplier of {@link uk.co.real_logic.aeron.driver.media.ReceiveChannelEndpoint}s which can be used for
     * debugging, monitoring, or modifying the behaviour when receiving from the media channel.
     *
     * @return the {@link SendChannelEndpointSupplier}.
     */
    public static ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier()
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
}
