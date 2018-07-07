/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.ControllableIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.StatusIndicator;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.ThreadingMode.DEDICATED;
import static io.aeron.logbuffer.LogBufferDescriptor.PAGE_MAX_SIZE;
import static io.aeron.logbuffer.LogBufferDescriptor.PAGE_MIN_SIZE;
import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;
import static org.agrona.BitUtil.fromHex;
import static org.agrona.SystemUtil.*;

/**
 * Configuration options for the {@link MediaDriver}.
 */
public class Configuration
{
    /**
     * Warn if the Aeron directory exists.
     */
    public static final String DIR_WARN_IF_EXISTS_PROP_NAME = "aeron.dir.warn.if.exists";

    /**
     * Warn if the Aeron directory exists.
     */
    public static final boolean DIR_WARN_IF_EXISTS =
        "true".equalsIgnoreCase(getProperty(DIR_WARN_IF_EXISTS_PROP_NAME, "true"));

    /**
     * Should driver attempt to an immediate forced delete of {@link CommonContext#AERON_DIR_PROP_NAME} on start
     * if it exists.
     */
    public static final String DIR_DELETE_ON_START_PROP_NAME = "aeron.dir.delete.on.start";

    /**
     * Should driver attempt to an immediate forced delete of {@link CommonContext#AERON_DIR_PROP_NAME} on start
     * if it exists.
     */
    public static final boolean DIR_DELETE_ON_START =
        "true".equalsIgnoreCase(getProperty(DIR_DELETE_ON_START_PROP_NAME, "false"));

    /**
     * Should high resolution timer be used on Windows.
     */
    public static final String USE_WINDOWS_HIGH_RES_TIMER_PROP_NAME = "aeron.use.windows.high.res.timer";

    /**
     * Should high resolution timer be used on Windows.
     */
    public static final boolean USE_WINDOWS_HIGH_RES_TIMER =
        "true".equalsIgnoreCase(getProperty(USE_WINDOWS_HIGH_RES_TIMER_PROP_NAME, "false"));

    /**
     * Property name for boolean value of term buffers should be created sparse.
     */
    public static final String TERM_BUFFER_SPARSE_FILE_PROP_NAME = "aeron.term.buffer.sparse.file";

    /**
     * Should term buffers be created as sparse files. Defaults to false.
     * <p>
     * If a platform supports sparse files then log buffer creation is faster with pages being allocated as
     * needed. This can help for large numbers of channels/streams but can result in latency pauses.
     */
    public static final boolean TERM_BUFFER_SPARSE_FILE =
        "true".equalsIgnoreCase(getProperty(TERM_BUFFER_SPARSE_FILE_PROP_NAME, "false"));

    /**
     * Property name for page size to align all files to.
     */
    public static final String FILE_PAGE_SIZE_PROP_NAME = "aeron.file.page.size";

    /**
     * Default page size for alignment of all files.
     */
    public static final int FILE_PAGE_SIZE_DEFAULT = 4 * 1024;

    /**
     * Page size for alignment of all files.
     */
    public static final int FILE_PAGE_SIZE = getSizeAsInt(FILE_PAGE_SIZE_PROP_NAME, FILE_PAGE_SIZE_DEFAULT);

    /**
     * Property name for boolean value for if storage checks should be performed when allocating files.
     */
    public static final String PERFORM_STORAGE_CHECKS_PROP_NAME = "aeron.perform.storage.checks";

    /**
     * Should storage checks should be performed when allocating files.
     */
    public static final boolean PERFORM_STORAGE_CHECKS =
        "true".equalsIgnoreCase(getProperty(PERFORM_STORAGE_CHECKS_PROP_NAME, "true"));

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
    public static final int IPC_TERM_BUFFER_LENGTH = getSizeAsInt(
        IPC_TERM_BUFFER_LENGTH_PROP_NAME, TERM_BUFFER_IPC_LENGTH_DEFAULT);

    /**
     * Property name low file storage warning threshold.
     */
    public static final String LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME = "aeron.low.file.store.warning.threshold";

    /**
     * Default value for low file storage warning threshold.
     */
    public static final long LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT = TERM_BUFFER_LENGTH_DEFAULT * 10L;

    /**
     * Default value for low file storage warning threshold.
     */
    public static final long LOW_FILE_STORE_WARNING_THRESHOLD = getSizeAsLong(
        LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME, LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT);

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
    public static final int CONDUCTOR_BUFFER_LENGTH = getSizeAsInt(
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
    public static final int TO_CLIENTS_BUFFER_LENGTH = getSizeAsInt(
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
    public static final int COUNTERS_VALUES_BUFFER_LENGTH = getSizeAsInt(
        COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME, COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT);

    public static final int COUNTERS_METADATA_BUFFER_LENGTH =
        COUNTERS_VALUES_BUFFER_LENGTH * (CountersReader.METADATA_LENGTH / CountersReader.COUNTER_LENGTH);

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
    public static final int ERROR_BUFFER_LENGTH = getSizeAsInt(
        ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT);
    /**
     * Property name for length of the memory mapped buffer for the loss report buffer.
     */
    public static final String LOSS_REPORT_BUFFER_LENGTH_PROP_NAME = "aeron.loss.report.buffer.length";

    /**
     * Default buffer length for the loss report buffer.
     */
    public static final int LOSS_REPORT_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

    /**
     * Buffer length for the loss report buffer for the media driver.
     */
    public static final int LOSS_REPORT_BUFFER_LENGTH = getSizeAsInt(
        LOSS_REPORT_BUFFER_LENGTH_PROP_NAME, LOSS_REPORT_BUFFER_LENGTH_DEFAULT);

    /**
     * Property name for length of the initial window which must be sufficient for Bandwidth Delay Produce (BDP).
     */
    public static final String INITIAL_WINDOW_LENGTH_PROP_NAME = "aeron.rcv.initial.window.length";

    /**
     * Default initial window length for flow control sender to receiver purposes.
     * <p>
     * Length of Initial Window:
     * <p>
     * RTT (LAN) = 100 usec
     * Throughput = 10 Gbps
     * <p>
     * Buffer = Throughput * RTT
     * Buffer = (10 * 1000 * 1000 * 1000 / 8) * 0.0001 = 125000
     * Round to 128KB
     */
    public static final int INITIAL_WINDOW_LENGTH_DEFAULT = 128 * 1024;

    /**
     * Property name for status message timeout in nanoseconds after which one will be sent.
     */
    public static final String STATUS_MESSAGE_TIMEOUT_PROP_NAME = "aeron.rcv.status.message.timeout";

    /**
     * Max timeout between SMs.
     */
    public static final long STATUS_MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(200);

    /**
     * Property name for ratio of sending data to polling status messages in the Sender.
     */
    public static final String SEND_TO_STATUS_POLL_RATIO_PROP_NAME = "aeron.send.to.status.poll.ratio";

    /**
     * The ratio for sending data to polling status messages in the Sender.
     */
    public static final int SEND_TO_STATUS_POLL_RATIO_DEFAULT = 6;

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
    public static final int SOCKET_RCVBUF_LENGTH = getSizeAsInt(
        SOCKET_RCVBUF_LENGTH_PROP_NAME, SOCKET_RCVBUF_LENGTH_DEFAULT);

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
    public static final int SOCKET_SNDBUF_LENGTH = getSizeAsInt(
        SOCKET_SNDBUF_LENGTH_PROP_NAME, SOCKET_SNDBUF_LENGTH_DEFAULT);

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
    public static final int SOCKET_MULTICAST_TTL = getInteger(
        SOCKET_MULTICAST_TTL_PROP_NAME, SOCKET_MULTICAST_TTL_DEFAULT);

    /**
     * Property name for linger timeout after draining on {@link Publication}s.
     */
    public static final String PUBLICATION_LINGER_PROP_NAME = "aeron.publication.linger.timeout";

    /**
     * Default time for {@link Publication}s to linger before cleanup in nanoseconds.
     */
    public static final long PUBLICATION_LINGER_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * Time for {@link Publication}s to linger before cleanup in nanoseconds. This is the time a publication will
     * wait around after draining to the network so that tail loss can be recovered.
     */
    public static final long PUBLICATION_LINGER_NS = getDurationInNanos(
        PUBLICATION_LINGER_PROP_NAME, PUBLICATION_LINGER_DEFAULT_NS);

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
    public static final long CLIENT_LIVENESS_TIMEOUT_NS = getDurationInNanos(
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
    public static final long IMAGE_LIVENESS_TIMEOUT_NS = getDurationInNanos(
        IMAGE_LIVENESS_TIMEOUT_PROP_NAME, IMAGE_LIVENESS_TIMEOUT_DEFAULT_NS);

    /**
     * Property name for window limit on {@link Publication} side.
     */
    public static final String PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME = "aeron.publication.term.window.length";

    /**
     * Publication term window length for flow control in bytes.
     */
    public static final int PUBLICATION_TERM_WINDOW_LENGTH = getSizeAsInt(PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME, 0);

    /**
     * Property name for window limit for IPC publications.
     */
    public static final String IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME =
        "aeron.ipc.publication.term.window.length";

    /**
     * IPC Publication term window length for flow control in bytes.
     */
    public static final int IPC_PUBLICATION_TERM_WINDOW_LENGTH = getSizeAsInt(
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
    public static final long PUBLICATION_UNBLOCK_TIMEOUT_NS = getDurationInNanos(
        PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME, PUBLICATION_UNBLOCK_TIMEOUT_DEFAULT_NS);

    /**
     * Property name for {@link Publication} connection timeout.
     */
    public static final String PUBLICATION_CONNECTION_TIMEOUT_PROP_NAME = "aeron.publication.connection.timeout";

    /**
     * Timeout for {@link Publication} connection timeout in nanoseconds
     */
    public static final long PUBLICATION_CONNECTION_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * Publication timeout for when to indicate no connection from lack of status messages.
     */
    public static final long PUBLICATION_CONNECTION_TIMEOUT_NS = getDurationInNanos(
        PUBLICATION_CONNECTION_TIMEOUT_PROP_NAME, PUBLICATION_CONNECTION_TIMEOUT_DEFAULT_NS);

    /**
     * Property name for if spy subscriptions simulate a connection.
     */
    public static final String SPIES_SIMULATE_CONNECTION_PROP_NAME = "aeron.spies.simulate.connection";

    /**
     * Should a spy subscription simulate a connection to a network publication.
     */
    public static final boolean SPIES_SIMULATE_CONNECTION =
        "true".equalsIgnoreCase(getProperty(SPIES_SIMULATE_CONNECTION_PROP_NAME, "false"));

    private static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";

    /**
     * Spin on no activity before backing off to yielding.
     */
    public static final long IDLE_MAX_SPINS = 10;

    /**
     * Yield the thread so others can run before backing off to parking.
     */
    public static final long IDLE_MAX_YIELDS = 20;

    /**
     * Park for the minimum period of time which is typically 50-55 microseconds on 64-bit non-virtualised Linux.
     * You will typically get 50-55 microseconds plus the number of nanoseconds requested if a core is available.
     * On Windows expect to wait for at least 16ms or 1ms if the high-res timers are enabled.
     */
    public static final long IDLE_MIN_PARK_NS = 1000;

    /**
     * Maximum back-off park time which doubles on each interval stepping up from the min park idle.
     */
    public static final long IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(1000);

    private static final String CONTROLLABLE_IDLE_STRATEGY = "org.agrona.concurrent.ControllableIdleStrategy";

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Sender} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String SENDER_IDLE_STRATEGY_PROP_NAME = "aeron.sender.idle.strategy";

    /**
     * {@link IdleStrategy} to be employed by {@link Sender} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String SENDER_IDLE_STRATEGY = getProperty(
        SENDER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link DriverConductor} for
     * {@link ThreadingMode#DEDICATED} and {@link ThreadingMode#SHARED_NETWORK}.
     */
    public static final String CONDUCTOR_IDLE_STRATEGY_PROP_NAME = "aeron.conductor.idle.strategy";

    /**
     * {@link IdleStrategy} to be employed by {@link DriverConductor} for {@link ThreadingMode#DEDICATED}
     * and {@link ThreadingMode#SHARED_NETWORK}.
     */
    public static final String CONDUCTOR_IDLE_STRATEGY = getProperty(
        CONDUCTOR_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Receiver} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String RECEIVER_IDLE_STRATEGY_PROP_NAME = "aeron.receiver.idle.strategy";

    /**
     * {@link IdleStrategy} to be employed by {@link Receiver} for {@link ThreadingMode#DEDICATED}.
     */
    public static final String RECEIVER_IDLE_STRATEGY = getProperty(
        RECEIVER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

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
     * Property name for {@link IdleStrategy} to be employed by {@link Sender}, {@link Receiver},
     * and {@link DriverConductor} for {@link ThreadingMode#SHARED}.
     */
    public static final String SHARED_IDLE_STRATEGY_PROP_NAME = "aeron.shared.idle.strategy";

    /**
     * {@link IdleStrategy} to be employed by {@link Sender}, {@link Receiver}, and {@link DriverConductor}
     * for {@link ThreadingMode#SHARED}.
     */
    public static final String SHARED_IDLE_STRATEGY = getProperty(
        SHARED_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);

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
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME =
        "aeron.multicast.FlowControl.supplier";

    /**
     * {@link FlowControlSupplier} to be employed for multicast channels.
     */
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER = getProperty(
        MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME, "io.aeron.driver.DefaultMulticastFlowControlSupplier");

    /**
     * Maximum UDP datagram payload size for IPv4. Jumbo datagrams from IPv6 are not supported.
     * <p>
     * Max length is 65,507 bytes as 65,535 minus 8 byte UDP header then minus 20 byte IP header.
     * Then round down to nearest multiple of {@link FrameDescriptor#FRAME_ALIGNMENT} to give 65,504.
     */
    public static final int MAX_UDP_PAYLOAD_LENGTH = 65504;

    /**
     * Length of the maximum transmission unit of the media driver's protocol. If this is greater
     * than the network MTU for UDP then the packet will be fragmented and can amplify the impact of loss.
     */
    public static final String MTU_LENGTH_PROP_NAME = "aeron.mtu.length";

    /**
     * The default is conservative to avoid fragmentation on IPv4 or IPv6 over Ethernet with PPPoE header,
     * or for clouds such as Google, Oracle, and AWS.
     * <p>
     * On networks that suffer little congestion then a larger value can be used to reduce syscall costs.
     */
    public static final int MTU_LENGTH_DEFAULT = 1408;

    /**
     * Length of the MTU to use for sending messages.
     */
    public static final int MTU_LENGTH = getSizeAsInt(MTU_LENGTH_PROP_NAME, MTU_LENGTH_DEFAULT);

    /**
     * Length of the maximum transmission unit of the media driver's protocol for IPC.
     */
    public static final String IPC_MTU_LENGTH_PROP_NAME = "aeron.ipc.mtu.length";

    /**
     * Length of the MTU to use for sending messages via IPC
     */
    public static final int IPC_MTU_LENGTH = getSizeAsInt(IPC_MTU_LENGTH_PROP_NAME, MTU_LENGTH_DEFAULT);

    /**
     * {@link ThreadingMode} to be used by the Aeron {@link MediaDriver}.
     */
    public static final String THREADING_MODE_PROP_NAME = "aeron.threading.mode";

    /**
     * {@link ThreadingMode} default used by the media driver unless overridden in context.
     */
    public static final ThreadingMode THREADING_MODE_DEFAULT = ThreadingMode.valueOf(
        getProperty(THREADING_MODE_PROP_NAME, DEDICATED.name()));

    /**
     * Interval in between checks for timers and timeouts.
     */
    public static final String TIMER_INTERVAL_PROP_NAME = "aeron.timer.interval";

    /**
     * Default interval in between checks for timers and timeouts.
     */
    public static final long DEFAULT_TIMER_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * How often to check liveness and cleanup timers in nanoseconds.
     */
    public static final long TIMER_INTERVAL_NS = getDurationInNanos(
        TIMER_INTERVAL_PROP_NAME, DEFAULT_TIMER_INTERVAL_NS);

    /**
     *  Timeout between a counter being freed and being reused.
     */
    public static final String COUNTER_FREE_TO_REUSE_TIMEOUT_PROP_NAME = "aeron.counters.free.to.reuse.timeout";

    /**
     *  Timeout between a counter being freed and being reused
     */
    public static final long DEFAULT_COUNTER_FREE_TO_REUSE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Property name for {@link SendChannelEndpointSupplier}.
     */
    public static final String SEND_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME = "aeron.SendChannelEndpoint.supplier";

    /**
     * {@link SendChannelEndpointSupplier} to provide endpoint extension behaviour.
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
     * Property name for Application Specific Feedback added to Status Messages by the driver for flow control.
     */
    public static final String SM_APPLICATION_SPECIFIC_FEEDBACK_PROP_NAME =
        "aeron.flow.control.sm.applicationSpecificFeedback";

    /**
     * Value to use for all Status Message Application Specific Feedback values from the driver for flow control.
     */
    public static final byte[] SM_APPLICATION_SPECIFIC_FEEDBACK = fromHex(getProperty(
        SM_APPLICATION_SPECIFIC_FEEDBACK_PROP_NAME, ""));

    /**
     * Property name for {@link CongestionControlSupplier} to be employed for receivers.
     */
    public static final String CONGESTION_CONTROL_STRATEGY_SUPPLIER_PROP_NAME = "aeron.CongestionControl.supplier";

    /**
     * {@link CongestionControlSupplier} to be employed for receivers.
     */
    public static final String CONGESTION_CONTROL_STRATEGY_SUPPLIER = getProperty(
        CONGESTION_CONTROL_STRATEGY_SUPPLIER_PROP_NAME, "io.aeron.driver.DefaultCongestionControlSupplier");

    /**
     * Property name for low end of the publication reserved session id range which will not be automatically assigned.
     */
    public static final String PUBLICATION_RESERVED_SESSION_ID_LOW_PROP_NAME =
        "aeron.publication.reserved.session.id.low";

    /**
     * Low end of the publication reserved session id range which will not be automatically assigned.
     */
    public static final int PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT = -1;

    /**
     * Low end of the publication reserved session id range which will not be automatically assigned.
     */
    public static final int PUBLICATION_RESERVED_SESSION_ID_LOW = getInteger(
        PUBLICATION_RESERVED_SESSION_ID_LOW_PROP_NAME, PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT);

    /**
     * Property name for high end of the publication reserved session id range which will not be automatically assigned.
     */
    public static final String PUBLICATION_RESERVED_SESSION_ID_HIGH_PROP_NAME =
        "aeron.publication.reserved.session.id.high";

    /**
     * High end of the publication reserved session id range which will not be automatically assigned.
     */
    public static final int PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT = 1000;

    /**
     * High end of the publication reserved session id range which will not be automatically assigned.
     */
    public static final int PUBLICATION_RESERVED_SESSION_ID_HIGH = getInteger(
        PUBLICATION_RESERVED_SESSION_ID_HIGH_PROP_NAME, PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT);

    /**
     * Limit for the number of commands drained in one operation.
     */
    public static final int COMMAND_DRAIN_LIMIT = 10;

    /**
     * Capacity for the command queues used between driver agents.
     */
    public static final int CMD_QUEUE_CAPACITY = 256;

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
     * Default group size estimate for NAK delay randomisation.
     */
    public static final int NAK_GROUPSIZE_DEFAULT = 10;

    /**
     * Default group RTT estimate for NAK delay randomization in milliseconds.
     */
    public static final int NAK_GRTT_DEFAULT = 10;

    /**
     * Default max backoff for NAK delay randomisation in milliseconds.
     */
    public static final long NAK_MAX_BACKOFF_DEFAULT = TimeUnit.MILLISECONDS.toNanos(60);

    /**
     * Multicast NAK delay is immediate initial with delayed subsequent delay.
     */
    public static final OptimalMulticastDelayGenerator NAK_MULTICAST_DELAY_GENERATOR =
        new OptimalMulticastDelayGenerator(NAK_MAX_BACKOFF_DEFAULT, NAK_GROUPSIZE_DEFAULT, NAK_GRTT_DEFAULT);

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
     * Default delay before retransmission of data for unicast in nanoseconds.
     */
    public static final long RETRANSMIT_UNICAST_DELAY_DEFAULT_NS = TimeUnit.NANOSECONDS.toNanos(0);

    /**
     * Source uses same for unicast and multicast. For ticks.
     */
    public static final FeedbackDelayGenerator RETRANSMIT_UNICAST_DELAY_GENERATOR =
        () -> RETRANSMIT_UNICAST_DELAY_DEFAULT_NS;

    /**
     * Default delay for linger for unicast in nanoseconds.
     */
    public static final long RETRANSMIT_UNICAST_LINGER_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(60);

    /**
     * Delay for linger for unicast.
     */
    public static final FeedbackDelayGenerator RETRANSMIT_UNICAST_LINGER_GENERATOR =
        () -> RETRANSMIT_UNICAST_LINGER_DEFAULT_NS;

    /**
     * Default max number of active retransmissions per connected stream.
     */
    public static final int MAX_RETRANSMITS_DEFAULT = 16;

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
     * How far ahead the publisher can get from the minimum subscriber position for IPC only.
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
     * Get the {@link IdleStrategy} that should be applied to {@link org.agrona.concurrent.Agent}s.
     *
     * @param strategyName       of the class to be created.
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
                    IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);
                break;

            case CONTROLLABLE_IDLE_STRATEGY:
                idleStrategy = new ControllableIdleStrategy(controllableStatus);
                controllableStatus.setOrdered(ControllableIdleStrategy.PARK);
                break;

            default:
                try
                {
                    idleStrategy = (IdleStrategy)Class.forName(strategyName).getConstructor().newInstance();
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
        return getSizeAsInt(TERM_BUFFER_LENGTH_PROP_NAME, TERM_BUFFER_LENGTH_DEFAULT);
    }

    static int initialWindowLength()
    {
        return getSizeAsInt(INITIAL_WINDOW_LENGTH_PROP_NAME, INITIAL_WINDOW_LENGTH_DEFAULT);
    }

    static long statusMessageTimeout()
    {
        return getDurationInNanos(STATUS_MESSAGE_TIMEOUT_PROP_NAME, STATUS_MESSAGE_TIMEOUT_DEFAULT_NS);
    }

    static int sendToStatusMessagePollRatio()
    {
        return getInteger(SEND_TO_STATUS_POLL_RATIO_PROP_NAME, SEND_TO_STATUS_POLL_RATIO_DEFAULT);
    }

    static long counterFreeToReuseTimeout()
    {
        return getDurationInNanos(COUNTER_FREE_TO_REUSE_TIMEOUT_PROP_NAME, DEFAULT_COUNTER_FREE_TO_REUSE_TIMEOUT_NS);
    }

    /**
     * Get the supplier of {@link SendChannelEndpoint}s which can be used for
     * debugging, monitoring, or modifying the behaviour when sending to the channel.
     *
     * @return the {@link SendChannelEndpointSupplier}.
     */
    static SendChannelEndpointSupplier sendChannelEndpointSupplier()
    {
        SendChannelEndpointSupplier supplier = null;
        try
        {
            supplier = (SendChannelEndpointSupplier)Class.forName(SEND_CHANNEL_ENDPOINT_SUPPLIER)
                .getConstructor()
                .newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link ReceiveChannelEndpoint}s which can be used for
     * debugging, monitoring, or modifying the behaviour when receiving from the channel.
     *
     * @return the {@link SendChannelEndpointSupplier}.
     */
    static ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier()
    {
        ReceiveChannelEndpointSupplier supplier = null;
        try
        {
            supplier = (ReceiveChannelEndpointSupplier)Class.forName(RECEIVE_CHANNEL_ENDPOINT_SUPPLIER)
                .getConstructor()
                .newInstance();
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
            supplier = (FlowControlSupplier)Class.forName(UNICAST_FLOW_CONTROL_STRATEGY_SUPPLIER)
                .getConstructor()
                .newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link FlowControl}s which can be used for changing behavior of flow control for multicast
     * publications.
     *
     * @return the {@link FlowControlSupplier}.
     */
    static FlowControlSupplier multicastFlowControlSupplier()
    {
        FlowControlSupplier supplier = null;
        try
        {
            supplier = (FlowControlSupplier)Class.forName(MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER)
                .getConstructor()
                .newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link CongestionControl} implementations which can be used for receivers.
     *
     * @return the {@link CongestionControlSupplier}
     */
    static CongestionControlSupplier congestionControlSupplier()
    {
        CongestionControlSupplier supplier = null;
        try
        {
            supplier = (CongestionControlSupplier)Class.forName(CONGESTION_CONTROL_STRATEGY_SUPPLIER)
                .getConstructor()
                .newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Validate that the initial window length is greater than MTU.
     *
     * @param initialWindowLength to be validated.
     * @param mtuLength           against which to validate.
     */
    static void validateInitialWindowLength(final int initialWindowLength, final int mtuLength)
    {
        if (mtuLength > initialWindowLength)
        {
            throw new IllegalStateException("Initial window length must be >= to MTU length: " + mtuLength);
        }
    }

    /**
     * Validate that the MTU is an appropriate length. MTU lengths must be a multiple of
     * {@link FrameDescriptor#FRAME_ALIGNMENT}.
     *
     * @param mtuLength to be validated.
     * @throws ConfigurationException if the MTU length is not valid.
     */
    static void validateMtuLength(final int mtuLength)
    {
        if (mtuLength < DataHeaderFlyweight.HEADER_LENGTH || mtuLength > MAX_UDP_PAYLOAD_LENGTH)
        {
            throw new ConfigurationException(
                "mtuLength must be a >= HEADER_LENGTH and <= MAX_UDP_PAYLOAD_LENGTH: " + mtuLength);
        }

        if ((mtuLength & (FrameDescriptor.FRAME_ALIGNMENT - 1)) != 0)
        {
            throw new ConfigurationException("mtuLength must be a multiple of FRAME_ALIGNMENT: " + mtuLength);
        }
    }

    /**
     * Validate the publication linger timeout is an appropriate value.
     *
     * @param timeoutNs to be validated.
     * @param driverLingerTimeoutNs set for the driver operation.
     */
    static void validatePublicationLingerTimeoutNs(final long timeoutNs, final long driverLingerTimeoutNs)
    {
        if (timeoutNs < driverLingerTimeoutNs)
        {
            throw new ConfigurationException(
                "linger must be greater than or equal to driver linger timeout: " + timeoutNs);
        }
    }

    /**
     * Validate that the socket buffer lengths are sufficient for the media driver configuration.
     *
     * @param ctx to be validated.
     */
    static void validateSocketBufferLengths(final MediaDriver.Context ctx)
    {
        try (DatagramChannel probe = DatagramChannel.open())
        {
            final int defaultSoSndBuf = probe.getOption(StandardSocketOptions.SO_SNDBUF);

            probe.setOption(StandardSocketOptions.SO_SNDBUF, Integer.MAX_VALUE);
            final int maxSoSndBuf = probe.getOption(StandardSocketOptions.SO_SNDBUF);

            if (maxSoSndBuf < SOCKET_SNDBUF_LENGTH)
            {
                System.err.format(
                    "WARNING: Could not get desired SO_SNDBUF, adjust OS to allow %s: attempted=%d, actual=%d%n",
                    SOCKET_SNDBUF_LENGTH_PROP_NAME,
                    SOCKET_SNDBUF_LENGTH,
                    maxSoSndBuf);
            }

            probe.setOption(StandardSocketOptions.SO_RCVBUF, Integer.MAX_VALUE);
            final int maxSoRcvBuf = probe.getOption(StandardSocketOptions.SO_RCVBUF);

            if (maxSoRcvBuf < SOCKET_RCVBUF_LENGTH)
            {
                System.err.format(
                    "WARNING: Could not get desired SO_RCVBUF, adjust OS to allow %s: attempted=%d, actual=%d%n",
                    SOCKET_RCVBUF_LENGTH_PROP_NAME,
                    SOCKET_RCVBUF_LENGTH,
                    maxSoRcvBuf);
            }

            final int soSndBuf = 0 == SOCKET_SNDBUF_LENGTH ? defaultSoSndBuf : SOCKET_SNDBUF_LENGTH;

            if (ctx.mtuLength() > soSndBuf)
            {
                throw new ConfigurationException(String.format(
                    "MTU greater than socket SO_SNDBUF, adjust %s to match MTU: mtuLength=%d, SO_SNDBUF=%d",
                    SOCKET_SNDBUF_LENGTH_PROP_NAME,
                    ctx.mtuLength(),
                    soSndBuf));
            }

            if (ctx.initialWindowLength() > maxSoRcvBuf)
            {
                throw new ConfigurationException("Window length greater than socket SO_RCVBUF, increase '" +
                    Configuration.INITIAL_WINDOW_LENGTH_PROP_NAME +
                    "' to match window: windowLength=" + ctx.initialWindowLength() + ", SO_RCVBUF=" + maxSoRcvBuf);
            }
        }
        catch (final IOException ex)
        {
            throw new RuntimeException("probe socket: " + ex.toString(), ex);
        }
    }

    /**
     * Validate that page size is valid and alignment is valid.
     *
     * @param pageSize to be checked.
     * @throws ConfigurationException if the size is not as expected.
     */
    static void validatePageSize(final int pageSize)
    {
        if (pageSize < PAGE_MIN_SIZE)
        {
            throw new ConfigurationException(
                "Page size less than min size of " + PAGE_MIN_SIZE + ": " + pageSize);
        }

        if (pageSize > PAGE_MAX_SIZE)
        {
            throw new ConfigurationException(
                "Page size greater than max size of " + PAGE_MAX_SIZE + ": " + pageSize);
        }

        if (!BitUtil.isPowerOfTwo(pageSize))
        {
            throw new ConfigurationException("Page size not a power of 2: " + pageSize);
        }
    }
}
