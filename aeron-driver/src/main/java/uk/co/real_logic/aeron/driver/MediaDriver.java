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
import uk.co.real_logic.aeron.common.concurrent.*;
import uk.co.real_logic.aeron.common.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.aeron.common.concurrent.ringbuffer.RingBuffer;
import uk.co.real_logic.aeron.common.event.EventConfiguration;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.common.event.EventReader;
import uk.co.real_logic.aeron.driver.buffer.TermBuffersFactory;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
    private final File adminDirectory;
    private final File dataDirectory;
    private final File countersDirectory;

    private final Receiver receiver;
    private final Sender sender;
    private final DriverConductor conductor;
    private final EventReader eventReader;
    private final Context ctx;

    private Thread conductorThread;
    private Thread senderThread;
    private Thread receiverThread;
    private Thread eventReaderThread;

    /**
     * Start Media Driver as a stand-alone process.
     *
     * @param args command line arguments
     */
    public static void main(final String[] args) throws Exception
    {
        try (final MediaDriver ignored = MediaDriver.launch())
        {
            new CommandBarrier("Type 'quit' to terminate", System.in, System.out).await("quit");
        }
    }

    /**
     * Construct a media driver with the given ctx.
     *
     * @param ctx for the media driver parameters
     */
    private MediaDriver(final Context ctx)
    {
        this.ctx = ctx;

        adminDirectory = new File(ctx.adminDirName());
        dataDirectory = new File(ctx.dataDirName());
        countersDirectory = new File(ctx.countersDirName());

        ensureDirectoriesAreRecreated();

        ctx.unicastSenderFlowControl(UnicastSenderControlStrategy::new)
           .multicastSenderFlowControl(UnicastSenderControlStrategy::new)
           .publications(new AtomicArray<>())
           .subscriptions(new AtomicArray<>())
           .conductorTimerWheel(Configuration.newConductorTimerWheel())
           .conductorCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
           .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
           .senderCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
           .conclude();

        eventReader = new EventReader(
            new EventReader.Context()
                .idleStrategy(Configuration.eventReaderIdleStrategy())
                .deleteOnExit(ctx.dirsDeleteOnExit())
                .eventHandler(ctx.eventConsumer));

        receiver = new Receiver(ctx);
        sender = new Sender(ctx);
        conductor = new DriverConductor(ctx);
    }

    /**
     * Launch a MediaDriver embedded in the current process with default configuration.
     *
     * @return the newly started MediaDriver.
     */
    public static MediaDriver launch()
    {
        return launch(new Context());
    }

    /**
     * Launch a MediaDriver embedded in the current process and provided a configuration context.
     *
     * @param ctx containing the configuration options.
     * @return the newly created MediaDriver.
     */
    public static MediaDriver launch(final Context ctx)
    {
        return new MediaDriver(ctx).start();
    }

    /**
     * Shutdown the media driver by stopping all threads and freeing resources.
     */
    public void close()
    {
        try
        {
            shutdown(senderThread, sender);
            shutdown(receiverThread, receiver);
            shutdown(conductorThread, conductor);

            receiver.nioSelector().selectNowWithoutProcessing();
            conductor.nioSelector().selectNowWithoutProcessing();
            ctx.close();

            shutdown(eventReaderThread, eventReader);
            deleteDirectories();
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private MediaDriver start()
    {
        conductorThread = new Thread(conductor);
        startThread(conductorThread, "driver-conductor");

        senderThread = new Thread(sender);
        startThread(senderThread, "sender");

        receiverThread = new Thread(receiver);
        startThread(receiverThread, "receiver");

        eventReaderThread = new Thread(eventReader);
        startThread(eventReaderThread, "event-reader");

        return this;
    }

    private void startThread(final Thread agentThread, final String name)
    {
        agentThread.setName(name);
        agentThread.start();
    }

    private void ensureDirectoriesAreRecreated()
    {
        final BiConsumer<String, String> callback =
            (path, name) ->
            {
                if (ctx.warnIfDirectoriesExist())
                {
                    System.err.println("WARNING: " + name + " directory already exists: " + path);
                }
            };

        IoUtil.ensureDirectoryIsRecreated(adminDirectory, "conductor", callback);
        IoUtil.ensureDirectoryIsRecreated(dataDirectory, "data", callback);
        IoUtil.ensureDirectoryIsRecreated(countersDirectory, "counters", callback);
    }

    private void deleteDirectories() throws Exception
    {
        if (ctx.dirsDeleteOnExit())
        {
            IoUtil.delete(adminDirectory, false);
            IoUtil.delete(dataDirectory, false);
            IoUtil.delete(countersDirectory, false);
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
            termBufferSize(Configuration.termBufferSize());
            initialWindowSize(Configuration.initialWindowSize());
            statusMessageTimeout(Configuration.statusMessageTimeout());

            eventConsumer = System.out::println;
            warnIfDirectoriesExist = true;
        }

        public Context conclude()
        {
            try
            {
                super.conclude();

                if (null == eventLogger)
                {
                    eventLogger = new EventLogger(
                        EventConfiguration.bufferLocationFile(),
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
            }
            catch (final Exception ex)
            {
                throw new RuntimeException(ex);
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
