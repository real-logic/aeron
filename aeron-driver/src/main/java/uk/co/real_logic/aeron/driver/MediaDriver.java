/*
 * Copyright 2014 Real Logic Ltd.
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

import uk.co.real_logic.aeron.common.*;
import uk.co.real_logic.aeron.common.concurrent.AtomicArray;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastBufferDescriptor;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBufferDescriptor;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.event.EventReader;
import uk.co.real_logic.aeron.common.concurrent.CountersManager;
import uk.co.real_logic.aeron.driver.buffer.TermBuffersFactory;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.Integer.getInteger;
import static uk.co.real_logic.aeron.common.IoUtil.deleteIfExists;
import static uk.co.real_logic.aeron.common.IoUtil.mapNewFile;

/**
 * Main class for JVM-based media driver
 * <p>
 * <p>
 * Usage:
 * <code>
 * $ java -jar aeron-driver.jar
 * $ java -Doption=value -jar aeron-driver.jar
 * </code>
 * Properties
 * <ul>
 * <li><code>aeron.rcv.buffer.size</code>: Use int value as size of buffer for receiving from network.</li>
 * <li><code>aeron.command.buffer.size</code>: Use int value as size of the command buffers between threads.</li>
 * <li><code>aeron.conductor.buffer.size</code>: Use int value as size of the conductor buffers between the media
 * driver and the client.</li>
 * </ul>
 */
public class MediaDriver implements AutoCloseable
{
    /**
     * Byte buffer size (in bytes) for reads
     */
    public static final String READ_BUFFER_SZ_PROP_NAME = "aeron.rcv.buffer.size";

    /**
     * Size (in bytes) of the log buffers for terms
     */
    public static final String TERM_BUFFER_SZ_PROP_NAME = "aeron.term.buffer.size";

    /**
     * Size (in bytes) of the command buffers between threads
     */
    public static final String COMMAND_BUFFER_SZ_PROP_NAME = "aeron.command.buffer.size";

    /**
     * Size (in bytes) of the conductor buffers between the media driver and the client
     */
    public static final String CONDUCTOR_BUFFER_SZ_PROP_NAME = "aeron.conductor.buffer.size";

    /**
     * Size (in bytes) of the broadcast buffers from the media driver to the clients
     */
    public static final String TO_CLIENTS_BUFFER_SZ_PROP_NAME = "aeron.clients.buffer.size";

    /**
     * Name of the default multicast interface
     */
    public static final String MULTICAST_DEFAULT_INTERFACE_PROP_NAME = "aeron.multicast.default.interface";

    /**
     * Property name for size of the memory mapped buffers for the counters file
     */
    public static final String COUNTER_BUFFERS_SZ_PROP_NAME = "aeron.dir.counters.size";

    /**
     * Property name for size of the initial window
     */
    public static final String INITIAL_WINDOW_SIZE_PROP_NAME = "aeron.rcv.initial.window.size";

    /**
     * Default byte buffer size for reads
     */
    public static final int READ_BYTE_BUFFER_SZ_DEFAULT = 4096;

    /**
     * Default term buffer size.
     */
    public static final int TERM_BUFFER_SZ_DEFAULT = 16 * 1024 * 1024;

    /**
     * Default buffer size for command buffers between threads
     */
    public static final int COMMAND_BUFFER_SZ_DEFAULT = 1024 * 1024;

    /**
     * Default buffer size for conductor buffers between the media driver and the client
     */
    public static final int CONDUCTOR_BUFFER_SZ_DEFAULT = 1024 * 1024 + RingBufferDescriptor.TRAILER_LENGTH;

    /**
     * Default buffer size for broadcast buffers from the media driver to the clients
     */
    public static final int TO_CLIENTS_BUFFER_SZ_DEFAULT = 1024 * 1024 + BroadcastBufferDescriptor.TRAILER_LENGTH;

    /**
     * Size of the memory mapped buffers for the counters file
     */
    public static final int COUNTERS_BUFFER_SZ_DEFAULT = 1024 * 1024;

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
    public static final int NAK_MAX_BACKOFF_DEFAULT = 60;

    /**
     * Default Unicast NAK delay in nanoseconds
     */
    public static final long NAK_UNICAST_DELAY_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(60);

    /**
     * Default group size estimate for retransmit delay randomization
     */
    public static final int RETRANS_GROUPSIZE_DEFAULT = 10;
    /**
     * Default group RTT estimate for retransmit delay randomization in msec
     */
    public static final int RETRANS_GRTT_DEFAULT = 10;
    /**
     * Default max backoff for retransmit delay randomization in msec
     */
    public static final int RETRANS_MAX_BACKOFF_DEFAULT = 60;
    /**
     * Default delay for retransmission of data for unicast
     */
    public static final long RETRANS_UNICAST_DELAY_DEFAULT_NS = TimeUnit.NANOSECONDS.toNanos(0);
    /**
     * Default delay for linger for unicast
     */
    public static final long RETRANS_UNICAST_LINGER_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(60);

    /**
     * Default max number of active retransmissions per Term
     */
    public static final int MAX_RETRANSMITS_DEFAULT = 16;

    /**
     * Default initial window size for flow control sender to receiver purposes
     * <p>
     * Sizing of Initial Window
     * <p>
     * RTT (LAN) = 100 usec
     * Throughput = 10 Gbps
     * <p>
     * Buffer = Throughput * RTT
     * Buffer = (10*1000*1000*1000/8) * 0.0001 = 125000
     * Round to 128KB
     */
    public static final int INITIAL_WINDOW_SIZE_DEFAULT = 128 * 1024;

    /**
     * Estimated RTT in nanoseconds.
     */
    public static final long ESTIMATED_RTT_NS = TimeUnit.MICROSECONDS.toNanos(100);

    public static final int READ_BYTE_BUFFER_SZ = getInteger(READ_BUFFER_SZ_PROP_NAME, READ_BYTE_BUFFER_SZ_DEFAULT);
    public static final int COMMAND_BUFFER_SZ = getInteger(COMMAND_BUFFER_SZ_PROP_NAME, COMMAND_BUFFER_SZ_DEFAULT);
    public static final int CONDUCTOR_BUFFER_SZ = getInteger(CONDUCTOR_BUFFER_SZ_PROP_NAME, CONDUCTOR_BUFFER_SZ_DEFAULT);
    public static final int TO_CLIENTS_BUFFER_SZ = getInteger(TO_CLIENTS_BUFFER_SZ_PROP_NAME, TO_CLIENTS_BUFFER_SZ_DEFAULT);
    public static final int COUNTER_BUFFERS_SZ = getInteger(COUNTER_BUFFERS_SZ_PROP_NAME, COUNTERS_BUFFER_SZ_DEFAULT);

    /**
     * ticksPerWheel for TimerWheel in conductor thread
     */
    public static final int CONDUCTOR_TICKS_PER_WHEEL = 1024;

    /**
     * tickDuration (in MICROSECONDS) for TimerWheel in conductor thread
     */
    public static final int CONDUCTOR_TICK_DURATION_US = 10 * 1000;

    public static final long AGENT_IDLE_MAX_SPINS = 100;
    public static final long AGENT_IDLE_MAX_YIELDS = 100;
    public static final long AGENT_IDLE_MIN_PARK_NS = TimeUnit.NANOSECONDS.toNanos(10);
    public static final long AGENT_IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(100);

    private final File adminDirFile;
    private final File dataDirFile;
    private final File countersDirFile;

    private final Receiver receiver;
    private final Sender sender;
    private final DriverConductor conductor;
    private final EventReader eventReader;
    private final Context ctx;

    private ExecutorService executor;

    private Thread conductorThread;
    private Thread senderThread;
    private Thread receiverThread;
    private Thread eventReaderThread;

    private Future conductorFuture;
    private Future senderFuture;
    private Future receiverFuture;
    private Future eventReaderFuture;

    /**
     * Start Media Driver as a stand-alone process.
     *
     * @param args command line arguments
     * @throws Exception
     */
    public static void main(final String[] args) throws Exception
    {
        try (final MediaDriver mediaDriver = new MediaDriver())
        {
            mediaDriver.invokeDaemonized();

            while (true)
            {
                Thread.sleep(1000);
            }
        }
    }

    /**
     * Initiatlize a media driver with default parameters.
     *
     * @throws Exception
     */
    public MediaDriver() throws Exception
    {
        this(new Context());
    }

    /**
     * Initialize a media driver with the given parameters.
     *
     * @param context for the media driver parameters
     * @throws Exception
     */
    public MediaDriver(final Context context) throws Exception
    {
        this.ctx = context;

        this.adminDirFile = new File(ctx.adminDirName());
        this.dataDirFile = new File(ctx.dataDirName());
        this.countersDirFile = new File(ctx.countersDirName());

        ensureDirectoriesExist();

        ctx.unicastSenderFlowControl(UnicastSenderControlStrategy::new)
           .multicastSenderFlowControl(UnicastSenderControlStrategy::new)
           .publications(new AtomicArray<>())
           .conductorTimerWheel(new TimerWheel(CONDUCTOR_TICK_DURATION_US,
               TimeUnit.MICROSECONDS,
               CONDUCTOR_TICKS_PER_WHEEL))
           .conductorCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
           .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
           .conductorIdleStrategy(new BackoffIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
               AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS))
           .senderIdleStrategy(new BackoffIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
               AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS))
           .receiverIdleStrategy(new BackoffIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
               AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS))
           .conclude();

        this.receiver = new Receiver(ctx);
        this.sender = new Sender(ctx);
        this.conductor = new DriverConductor(ctx);

        final EventReader.Context readerCtx =
            new EventReader.Context()
                .backoffStrategy(new BackoffIdleStrategy(AGENT_IDLE_MAX_SPINS, AGENT_IDLE_MAX_YIELDS,
                    AGENT_IDLE_MIN_PARK_NS, AGENT_IDLE_MAX_PARK_NS))
                .warnIfEventsFileExists(ctx.warnIfDirectoriesExist)
                .deleteOnExit(ctx.dirsDeleteOnExit())
                .eventHandler(ctx.eventConsumer);

        this.eventReader = new EventReader(readerCtx);
    }

    /**
     * Spin up all {@link Agent}s as Daemon threads.
     */
    public void invokeDaemonized()
    {
        conductorThread = new Thread(conductor);
        invokeDaemonized(conductorThread, "driver-conductor");

        senderThread = new Thread(sender);
        invokeDaemonized(senderThread, "driver-sender");

        receiverThread = new Thread(receiver);
        invokeDaemonized(receiverThread, "driver-receiver");

        eventReaderThread = new Thread(eventReader);
        invokeDaemonized(eventReaderThread, "event-reader");
    }

    /**
     * Spin up specific thread as a Daemon thread.
     *
     * @param agentThread thread to Daemonize
     * @param name        to associate with thread
     */
    public void invokeDaemonized(final Thread agentThread, final String name)
    {
        agentThread.setName(name);
        agentThread.setDaemon(true);
        agentThread.start();
    }

    /**
     * Invoke and start all {@link uk.co.real_logic.aeron.common.Agent}s internal to the media driver using
     * a fixed size thread pool internal to the media driver.
     */
    public void invokeEmbedded()
    {
        executor = Executors.newFixedThreadPool(4);

        conductorFuture = executor.submit(conductor);
        senderFuture = executor.submit(sender);
        receiverFuture = executor.submit(receiver);
        eventReaderFuture = executor.submit(eventReader);
    }

    /**
     * Stop running {@link uk.co.real_logic.aeron.common.Agent}s. Waiting for each to finish.
     *
     * @throws Exception
     */
    public void shutdown() throws Exception
    {
        shutdown(senderThread, sender);
        shutdown(receiverThread, receiver);
        shutdown(conductorThread, conductor);
        shutdown(eventReaderThread, eventReader);

        if (null != executor)
        {
            shutdownExecutorThread(senderFuture, sender);
            shutdownExecutorThread(receiverFuture, receiver);
            shutdownExecutorThread(conductorFuture, conductor);
            shutdownExecutorThread(eventReaderFuture, eventReader);

            executor.shutdown();
        }
    }

    /**
     * Close and cleanup all resources for media driver
     */
    public void close()
    {
        try
        {
            receiver.close();
            receiver.nioSelector().selectNowWithoutProcessing();
            sender.close();
            conductor.close();
            conductor.nioSelector().selectNowWithoutProcessing();
            ctx.close();
            eventReader.close();
            deleteDirectories();
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private void ensureDirectoriesExist() throws Exception
    {
        final BiConsumer<String, String> callback =
            (path, name) ->
            {
                if (ctx.warnIfDirectoriesExist())
                {
                    System.err.println("WARNING: " + name + " directory already exists: " + path);
                }
            };

        IoUtil.ensureDirectoryExists(adminDirFile, "conductor", callback);
        IoUtil.ensureDirectoryExists(dataDirFile, "data", callback);
        IoUtil.ensureDirectoryExists(countersDirFile, "counters", callback);
    }

    private void deleteDirectories() throws Exception
    {
        if (ctx.dirsDeleteOnExit())
        {
            IoUtil.delete(adminDirFile, false);
            IoUtil.delete(dataDirFile, false);
            IoUtil.delete(countersDirFile, false);
        }
    }

    private void shutdown(final Thread thread, final Agent agent)
    {
        if (thread == null)
        {
            return;
        }

        agent.stop();
        thread.interrupt();

        do
        {
            try
            {
                thread.join(100);

                if (!thread.isAlive())
                {
                    break;
                }
            }
            catch (final InterruptedException ex)
            {
                System.err.println("Daemon Thread <" + thread.getName() + "> interrupted stop. Retrying...");
                thread.interrupt();
            }
        }
        while (true);
    }

    private void shutdownExecutorThread(final Future future, final Agent agent)
    {
        agent.stop();

        try
        {
            future.get(100, TimeUnit.MILLISECONDS);
        }
        catch (final TimeoutException ex)
        {
            future.cancel(true);
        }
        catch (final Exception ex)
        {
            ctx.driverLogger().logException(ex);
        }
    }

    public static class Context extends CommonContext
    {
        private TermBuffersFactory termBuffersFactory;
        private NioSelector receiverNioSelector;
        private NioSelector conductorNioSelector;
        private Supplier<SenderControlStrategy> unicastSenderFlowControl;
        private Supplier<SenderControlStrategy> multicastSenderFlowControl;
        private TimerWheel conductorTimerWheel;
        private OneToOneConcurrentArrayQueue<? super Object> conductorCommandQueue;
        private OneToOneConcurrentArrayQueue<? super Object> receiverCommandQueue;
        private ReceiverProxy receiverProxy;
        private DriverConductorProxy driverConductorProxy;
        private IdleStrategy conductorIdleStrategy;
        private IdleStrategy senderIdleStrategy;
        private IdleStrategy receiverIdleStrategy;
        private AtomicArray<DriverPublication> publications;
        private ClientProxy clientProxy;
        private RingBuffer fromClientCommands;

        private MappedByteBuffer toClientsBuffer;
        private MappedByteBuffer toDriverBuffer;
        private MappedByteBuffer counterLabelsByteBuffer;
        private MappedByteBuffer counterValuesByteBuffer;
        private CountersManager countersManager;

        private int termBufferSize;
        private int initialWindowSize;

        private boolean warnIfDirectoriesExist;
        private EventLogger conductorLogger;
        private EventLogger receiverLogger;
        private EventLogger driverLogger;
        private EventLogger senderLogger;
        private Consumer<String> eventConsumer;

        public Context()
        {
            termBufferSize(getInteger(TERM_BUFFER_SZ_PROP_NAME, TERM_BUFFER_SZ_DEFAULT));
            initialWindowSize(getInteger(INITIAL_WINDOW_SIZE_PROP_NAME, INITIAL_WINDOW_SIZE_DEFAULT));
            warnIfDirectoriesExist = true;
        }

        public Context conclude() throws IOException
        {
            super.conclude();

            receiverNioSelector(new NioSelector());
            conductorNioSelector(new NioSelector());

            validateTermBufferSize(termBufferSize());
            validateInitialWindowSize(initialWindowSize(), mtuLength());

            // clean out existing files. We've warned about them already.
            deleteIfExists(toClientsFile());
            deleteIfExists(toDriverFile());

            if (dirsDeleteOnExit())
            {
                toClientsFile().deleteOnExit();
                toDriverFile().deleteOnExit();
            }

            toClientsBuffer = mapNewFile(toClientsFile(), TO_CLIENTS_BUFFER_SZ);

            final BroadcastTransmitter transmitter = new BroadcastTransmitter(new AtomicBuffer(toClientsBuffer));
            clientProxy(new ClientProxy(transmitter, new EventLogger()));

            toDriverBuffer = mapNewFile(toDriverFile(), CONDUCTOR_BUFFER_SZ);

            fromClientCommands(new ManyToOneRingBuffer(new AtomicBuffer(toDriverBuffer)));

            receiverProxy(new ReceiverProxy(receiverCommandQueue()));
            driverConductorProxy(new DriverConductorProxy(conductorCommandQueue));

            termBuffersFactory(new TermBuffersFactory(dataDirName(), termBufferSize));

            if (countersManager() == null)
            {
                if (counterLabelsBuffer() == null)
                {
                    final File counterLabelsFile = new File(countersDirName(), LABELS_FILE);

                    deleteIfExists(counterLabelsFile);

                    if (dirsDeleteOnExit())
                    {
                        counterLabelsFile.deleteOnExit();
                    }

                    counterLabelsByteBuffer = mapNewFile(counterLabelsFile, COUNTER_BUFFERS_SZ);

                    counterLabelsBuffer(new AtomicBuffer(counterLabelsByteBuffer));
                }

                if (countersBuffer() == null)
                {
                    final File counterValuesFile = new File(countersDirName(), VALUES_FILE);

                    deleteIfExists(counterValuesFile);

                    if (dirsDeleteOnExit())
                    {
                        counterValuesFile.deleteOnExit();
                    }

                    counterValuesByteBuffer = mapNewFile(counterValuesFile, COUNTER_BUFFERS_SZ);

                    countersBuffer(new AtomicBuffer(counterValuesByteBuffer));
                }

                countersManager(new CountersManager(counterLabelsBuffer(), countersBuffer()));
            }

            conductorLogger(new EventLogger());
            driverLogger(new EventLogger());
            receiverLogger(new EventLogger());
            senderLogger(new EventLogger());
            eventConsumer(System.out::println);

            return this;
        }

        public Context conductorCommandQueue(
            final OneToOneConcurrentArrayQueue<? super Object> conductorCommandQueue)
        {
            this.conductorCommandQueue = conductorCommandQueue;
            return this;
        }

        public Context termBuffersFactory(final TermBuffersFactory termBuffersFactory)
        {
            this.termBuffersFactory = termBuffersFactory;
            return this;
        }

        public Context receiverNioSelector(final NioSelector nioSelector)
        {
            this.receiverNioSelector = nioSelector;
            return this;
        }

        public Context conductorNioSelector(final NioSelector nioSelector)
        {
            this.conductorNioSelector = nioSelector;
            return this;
        }

        public Context unicastSenderFlowControl(final Supplier<SenderControlStrategy> senderFlowControl)
        {
            this.unicastSenderFlowControl = senderFlowControl;
            return this;
        }

        public Context multicastSenderFlowControl(final Supplier<SenderControlStrategy> senderFlowControl)
        {
            this.multicastSenderFlowControl = senderFlowControl;
            return this;
        }

        public Context conductorTimerWheel(final TimerWheel wheel)
        {
            this.conductorTimerWheel = wheel;
            return this;
        }

        public Context receiverCommandQueue(
            final OneToOneConcurrentArrayQueue<? super Object> receiverCommandQueue)
        {
            this.receiverCommandQueue = receiverCommandQueue;
            return this;
        }

        public Context receiverProxy(final ReceiverProxy receiverProxy)
        {
            this.receiverProxy = receiverProxy;
            return this;
        }

        public Context driverConductorProxy(final DriverConductorProxy driverConductorProxy)
        {
            this.driverConductorProxy = driverConductorProxy;
            return this;
        }

        public Context conductorIdleStrategy(final IdleStrategy strategy)
        {
            this.conductorIdleStrategy = strategy;
            return this;
        }

        public Context senderIdleStrategy(final IdleStrategy strategy)
        {
            this.senderIdleStrategy = strategy;
            return this;
        }

        public Context receiverIdleStrategy(final IdleStrategy strategy)
        {
            this.receiverIdleStrategy = strategy;
            return this;
        }

        public Context publications(final AtomicArray<DriverPublication> publications)
        {
            this.publications = publications;
            return this;
        }

        public Context clientProxy(final ClientProxy clientProxy)
        {
            this.clientProxy = clientProxy;
            return this;
        }

        public Context fromClientCommands(final RingBuffer fromClientCommands)
        {
            this.fromClientCommands = fromClientCommands;
            return this;
        }

        public Context countersManager(final CountersManager countersManager)
        {
            this.countersManager = countersManager;
            return this;
        }

        public Context termBufferSize(final int termBufferSize)
        {
            this.termBufferSize = termBufferSize;
            return this;
        }

        public Context initialWindowSize(final int initialWindowSize)
        {
            this.initialWindowSize = initialWindowSize;
            return this;
        }

        public Context warnIfDirectoriesExist(final boolean value)
        {
            this.warnIfDirectoriesExist = value;
            return this;
        }

        public Context conductorLogger(final EventLogger value)
        {
            this.conductorLogger = value;
            return this;
        }

        public Context receiverLogger(final EventLogger value)
        {
            this.receiverLogger = value;
            return this;
        }

        public Context driverLogger(final EventLogger value)
        {
            this.driverLogger = value;
            return this;
        }

        public Context senderLogger(final EventLogger value)
        {
            this.senderLogger = value;
            return this;
        }

        public Context eventConsumer(final Consumer<String> value)
        {
            this.eventConsumer = value;
            return this;
        }

        public OneToOneConcurrentArrayQueue<? super Object> conductorCommandQueue()
        {
            return conductorCommandQueue;
        }

        public TermBuffersFactory termBuffersFactory()
        {
            return termBuffersFactory;
        }

        public NioSelector receiverNioSelector()
        {
            return receiverNioSelector;
        }

        public NioSelector conductorNioSelector()
        {
            return conductorNioSelector;
        }

        public Supplier<SenderControlStrategy> unicastSenderFlowControl()
        {
            return unicastSenderFlowControl;
        }

        public Supplier<SenderControlStrategy> multicastSenderFlowControl()
        {
            return multicastSenderFlowControl;
        }

        public TimerWheel conductorTimerWheel()
        {
            return conductorTimerWheel;
        }

        public OneToOneConcurrentArrayQueue<? super Object> receiverCommandQueue()
        {
            return receiverCommandQueue;
        }

        public ReceiverProxy receiverProxy()
        {
            return receiverProxy;
        }

        public DriverConductorProxy driverConductorProxy()
        {
            return driverConductorProxy;
        }

        public IdleStrategy conductorIdleStrategy()
        {
            return conductorIdleStrategy;
        }

        public IdleStrategy senderIdleStrategy()
        {
            return senderIdleStrategy;
        }

        public IdleStrategy receiverIdleStrategy()
        {
            return receiverIdleStrategy;
        }

        public AtomicArray<DriverPublication> publications()
        {
            return publications;
        }

        public ClientProxy clientProxy()
        {
            return clientProxy;
        }

        public RingBuffer fromClientCommands()
        {
            return fromClientCommands;
        }

        public CountersManager countersManager()
        {
            return countersManager;
        }

        public int termBufferSize()
        {
            return termBufferSize;
        }

        public int initialWindowSize()
        {
            return initialWindowSize;
        }

        public boolean warnIfDirectoriesExist()
        {
            return warnIfDirectoriesExist;
        }

        public void close()
        {
            if (null != toClientsBuffer)
            {
                IoUtil.unmap(toClientsBuffer);
            }

            if (null != toDriverBuffer)
            {
                IoUtil.unmap(toDriverBuffer);
            }

            if (null != counterLabelsByteBuffer)
            {
                IoUtil.unmap(counterLabelsByteBuffer);
            }

            if (null != counterValuesByteBuffer)
            {
                IoUtil.unmap(counterValuesByteBuffer);
            }

            super.close();
        }

        public static void validateTermBufferSize(final int size)
        {
            if (size < 2 || 1 != Integer.bitCount(size))
            {
                throw new IllegalStateException("Term buffer size must be a positive power of 2: " + size);
            }
        }

        public static void validateInitialWindowSize(final int initialWindowSize, final int mtuLength)
        {
            if (mtuLength > initialWindowSize)
            {
                throw new IllegalStateException("Initial window size must be >= to MTU length: " + mtuLength);
            }
        }

        public EventLogger conductorLogger()
        {
            return conductorLogger;
        }

        public EventLogger receiverLogger()
        {
            return receiverLogger;
        }

        public EventLogger senderLogger()
        {
            return senderLogger;
        }

        public EventLogger driverLogger()
        {
            return driverLogger;
        }
    }
}
