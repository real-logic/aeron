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
import uk.co.real_logic.aeron.common.concurrent.CountersManager;
import uk.co.real_logic.aeron.common.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.common.event.EventConfiguration;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.event.EventReader;
import uk.co.real_logic.aeron.driver.buffer.TermBuffersFactory;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.Integer.getInteger;
import static java.lang.Long.getLong;
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
     * Construct a media driver with default parameters.
     *
     * @throws Exception
     */
    public MediaDriver() throws Exception
    {
        this(new Context());
    }

    /**
     * Construct a media driver with the given context.
     *
     * @param context for the media driver parameters
     */
    public MediaDriver(final Context context) throws Exception
    {
        this.ctx = context;

        this.adminDirFile = new File(ctx.adminDirName());
        this.dataDirFile = new File(ctx.dataDirName());
        this.countersDirFile = new File(ctx.countersDirName());

        ensureDirectoriesAreRecreated();

        final EventReader.Context readerCtx =
            new EventReader.Context()
                .idleStrategy(Configuration.agentIdleStrategy())
                .deleteOnExit(ctx.dirsDeleteOnExit())
                .eventHandler(ctx.eventConsumer);

        this.eventReader = new EventReader(readerCtx);

        ctx.unicastSenderFlowControl(UnicastSenderControlStrategy::new)
           .multicastSenderFlowControl(UnicastSenderControlStrategy::new)
           .publications(new AtomicArray<>())
           .subscriptions(new AtomicArray<>())
           .conductorTimerWheel(new TimerWheel(Configuration.CONDUCTOR_TICK_DURATION_US,
                                               TimeUnit.MICROSECONDS,
                                               Configuration.CONDUCTOR_TICKS_PER_WHEEL))
           .conductorCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
           .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
           .senderCommandQueue(new OneToOneConcurrentArrayQueue<>(1024))
           .conclude();

        this.receiver = new Receiver(ctx);
        this.sender = new Sender(ctx);
        this.conductor = new DriverConductor(ctx);
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

    private void ensureDirectoriesAreRecreated() throws Exception
    {
        final BiConsumer<String, String> callback =
            (path, name) ->
            {
                if (ctx.warnIfDirectoriesExist())
                {
                    System.err.println("WARNING: " + name + " directory already exists: " + path);
                }
            };

        IoUtil.ensureDirectoryIsRecreated(adminDirFile, "conductor", callback);
        IoUtil.ensureDirectoryIsRecreated(dataDirFile, "data", callback);
        IoUtil.ensureDirectoryIsRecreated(countersDirFile, "counters", callback);
    }

    private void deleteDirectories() throws Exception
    {
        if (ctx.dirsDeleteOnExit())
        {
            if (null != adminDirFile)
            {
                IoUtil.delete(adminDirFile, false);
            }

            if (null != dataDirFile)
            {
                IoUtil.delete(dataDirFile, false);
            }

            if (null != countersDirFile)
            {
                IoUtil.delete(countersDirFile, false);
            }
        }
    }

    private void shutdown(final Thread thread, final Agent agent)
    {
        if (thread == null)
        {
            return;
        }

        agent.close();
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
        int timeouts = 0;

        do
        {
            try
            {
                agent.close();

                future.get(100, TimeUnit.MILLISECONDS);

                if (future.isDone())
                {
                    break;
                }
            }
            catch (final TimeoutException ex)
            {
                System.err.println("Executor thread timeout. Retrying...");

                if (++timeouts > 5)
                {
                    System.err.println("... cancelling thread.");
                    future.cancel(true);
                }
            }
            catch (final CancellationException ex)
            {
                break;
            }
            catch (final Exception ex)
            {
                ctx.eventLogger().logException(ex);
            }
        }
        while (true);
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
        private OneToOneConcurrentArrayQueue<? super Object> senderCommandQueue;
        private ReceiverProxy receiverProxy;
        private SenderProxy senderProxy;
        private DriverConductorProxy driverConductorProxy;
        private IdleStrategy conductorIdleStrategy;
        private IdleStrategy senderIdleStrategy;
        private IdleStrategy receiverIdleStrategy;
        private AtomicArray<DriverPublication> publications;
        private AtomicArray<DriverSubscription> subscriptions;
        private ClientProxy clientProxy;
        private RingBuffer fromClientCommands;

        private MappedByteBuffer toClientsBuffer;
        private MappedByteBuffer toDriverBuffer;
        private MappedByteBuffer counterLabelsByteBuffer;
        private MappedByteBuffer counterValuesByteBuffer;
        private CountersManager countersManager;

        private int termBufferSize;
        private int initialWindowSize;
        private long statusMessageTimeout;

        private boolean warnIfDirectoriesExist;
        private EventLogger eventLogger;
        private Consumer<String> eventConsumer;

        public Context()
        {
            termBufferSize(getInteger(Configuration.TERM_BUFFER_SZ_PROP_NAME, Configuration.TERM_BUFFER_SZ_DEFAULT));
            initialWindowSize(getInteger(Configuration.INITIAL_WINDOW_SIZE_PROP_NAME, Configuration.INITIAL_WINDOW_SIZE_DEFAULT));
            statusMessageTimeout(
                getLong(Configuration.STATUS_MESSAGE_TIMEOUT_PROP_NAME, Configuration.STATUS_MESSAGE_TIMEOUT_DEFAULT_NS));

            eventConsumer = System.out::println;
            warnIfDirectoriesExist = true;
        }

        public Context conclude() throws IOException
        {
            super.conclude();

            if (null == eventLogger)
            {
                eventLogger = new EventLogger(
                    new File(System.getProperty(EventConfiguration.LOCATION_PROPERTY_NAME, EventConfiguration.LOCATION_DEFAULT)),
                    EventConfiguration.getEnabledEventCodes());
            }

            receiverNioSelector(new NioSelector());
            conductorNioSelector(new NioSelector());

            Configuration.validateTermBufferSize(termBufferSize());
            Configuration.validateInitialWindowSize(initialWindowSize(), mtuLength());

            // clean out existing files. We've warned about them already.
            deleteIfExists(toClientsFile());
            deleteIfExists(toDriverFile());

            if (dirsDeleteOnExit())
            {
                toClientsFile().deleteOnExit();
                toDriverFile().deleteOnExit();
            }

            toClientsBuffer = mapNewFile(toClientsFile(), Configuration.TO_CLIENTS_BUFFER_SZ);

            final BroadcastTransmitter transmitter = new BroadcastTransmitter(new AtomicBuffer(toClientsBuffer));
            clientProxy(new ClientProxy(transmitter, eventLogger));

            toDriverBuffer = mapNewFile(toDriverFile(), Configuration.CONDUCTOR_BUFFER_SZ);

            fromClientCommands(new ManyToOneRingBuffer(new AtomicBuffer(toDriverBuffer)));

            receiverProxy(new ReceiverProxy(receiverCommandQueue()));
            senderProxy(new SenderProxy(senderCommandQueue()));
            driverConductorProxy(new DriverConductorProxy(conductorCommandQueue));

            termBuffersFactory(new TermBuffersFactory(dataDirName(), termBufferSize, eventLogger));

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

                    counterLabelsByteBuffer = mapNewFile(counterLabelsFile, Configuration.COUNTER_BUFFERS_SZ);

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

                    counterValuesByteBuffer = mapNewFile(counterValuesFile, Configuration.COUNTER_BUFFERS_SZ);

                    countersBuffer(new AtomicBuffer(counterValuesByteBuffer));
                }

                countersManager(new CountersManager(counterLabelsBuffer(), countersBuffer()));
            }

            if (null == conductorIdleStrategy)
            {
                conductorIdleStrategy(Configuration.agentIdleStrategy());
            }

            if (null == senderIdleStrategy)
            {
                senderIdleStrategy(Configuration.agentIdleStrategy());
            }

            if (null == receiverIdleStrategy)
            {
                receiverIdleStrategy(Configuration.agentIdleStrategy());
            }

            return this;
        }

        public Context conductorCommandQueue(final OneToOneConcurrentArrayQueue<? super Object> conductorCommandQueue)
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

        public Context receiverCommandQueue(final OneToOneConcurrentArrayQueue<? super Object> receiverCommandQueue)
        {
            this.receiverCommandQueue = receiverCommandQueue;
            return this;
        }

        public Context senderCommandQueue(final OneToOneConcurrentArrayQueue<? super Object> senderCommandQueue)
        {
            this.senderCommandQueue = senderCommandQueue;
            return this;
        }

        public Context receiverProxy(final ReceiverProxy receiverProxy)
        {
            this.receiverProxy = receiverProxy;
            return this;
        }

        public Context senderProxy(final SenderProxy senderProxy)
        {
            this.senderProxy = senderProxy;
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

        public Context subscriptions(final AtomicArray<DriverSubscription> subscriptions)
        {
            this.subscriptions = subscriptions;
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

        public Context statusMessageTimeout(final long statusMessageTimeout)
        {
            this.statusMessageTimeout = statusMessageTimeout;
            return this;
        }

        public Context warnIfDirectoriesExist(final boolean value)
        {
            this.warnIfDirectoriesExist = value;
            return this;
        }

        public Context eventConsumer(final Consumer<String> value)
        {
            this.eventConsumer = value;
            return this;
        }

        public Context eventLogger(final EventLogger value)
        {
            this.eventLogger = value;
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

        public OneToOneConcurrentArrayQueue<? super Object> senderCommandQueue()
        {
            return senderCommandQueue;
        }

        public ReceiverProxy receiverProxy()
        {
            return receiverProxy;
        }

        public SenderProxy senderProxy()
        {
            return senderProxy;
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

        public AtomicArray<DriverSubscription> subscriptions()
        {
            return subscriptions;
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

        public long statusMessageTimeout()
        {
            return statusMessageTimeout;
        }

        public boolean warnIfDirectoriesExist()
        {
            return warnIfDirectoriesExist;
        }

        public EventLogger eventLogger()
        {
            return eventLogger;
        }

        public Consumer<Exception> eventLoggerException()
        {
            return eventLogger::logException;
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

            if (null != eventLogger)
            {
                eventLogger.close();
            }

            super.close();
        }
    }
}
