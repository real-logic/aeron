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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.Integer.getInteger;
import static uk.co.real_logic.aeron.common.IoUtil.deleteIfExists;
import static uk.co.real_logic.aeron.common.IoUtil.mapNewFile;
import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;
import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_PROP_NAME;

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
    private final Context ctx;

    /**
     * Start Media Driver as a stand-alone process.
     *
     * @param args command line arguments
     */
    public static void main(final String[] args) throws Exception
    {
        try (final MediaDriver ignored = MediaDriver.launch())
        {
            new SigIntBarrier().await();

            System.out.println("Shutdown Driver...");
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

        ctx.unicastSenderFlowControl(Configuration::unicastSenderFlowControlStrategy)
           .multicastSenderFlowControl(Configuration::multicastSenderFlowControlStrategy)
           .conductorTimerWheel(Configuration.newConductorTimerWheel())
           .conductorCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
           .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
           .senderCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
           .conclude();

        conductor = new DriverConductor(ctx);
        sender = new Sender(ctx);
        receiver = new Receiver(ctx);
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
            sender.close();
            receiver.close();
            conductor.close();

            freeSocketsForReuseOnWindows();
            ctx.close();

            deleteDirectories();
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private void freeSocketsForReuseOnWindows()
    {
        ctx.receiverNioSelector().selectNowWithoutProcessing();
        ctx.conductorNioSelector().selectNowWithoutProcessing();
    }

    private MediaDriver start()
    {
        startThread(new Thread(conductor), "aeron-driver-conductor");
        startThread(new Thread(sender), "aeron-sender");
        startThread(new Thread(receiver), "aeron-receiver");

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

    public static class Context extends CommonContext
    {
        private TermBuffersFactory termBuffersFactory;
        private NioSelector receiverNioSelector;
        private NioSelector conductorNioSelector;
        private Supplier<SenderFlowControl> unicastSenderFlowControl;
        private Supplier<SenderFlowControl> multicastSenderFlowControl;
        private TimerWheel conductorTimerWheel;
        private OneToOneConcurrentArrayQueue<Object> conductorCommandQueue;
        private OneToOneConcurrentArrayQueue<Object> receiverCommandQueue;
        private OneToOneConcurrentArrayQueue<Object> senderCommandQueue;
        private ReceiverProxy receiverProxy;
        private SenderProxy senderProxy;
        private DriverConductorProxy driverConductorProxy;
        private IdleStrategy conductorIdleStrategy;
        private IdleStrategy senderIdleStrategy;
        private IdleStrategy receiverIdleStrategy;
        private ClientProxy clientProxy;
        private RingBuffer toDriverCommands;
        private RingBuffer toEventReader;

        private MappedByteBuffer toClientsBuffer;
        private MappedByteBuffer toDriverBuffer;
        private MappedByteBuffer counterLabelsByteBuffer;
        private MappedByteBuffer counterValuesByteBuffer;
        private CountersManager countersManager;
        private SystemCounters systemCounters;

        private int publicationTermBufferSize;
        private int maxConnectionTermBufferSize;
        private int initialWindowSize;
        private int eventBufferSize;
        private long statusMessageTimeout;
        private long dataLossSeed;
        private long controlLossSeed;
        private double dataLossRate;
        private double controlLossRate;
        private int mtuLength;

        private boolean warnIfDirectoriesExist;
        private EventLogger eventLogger;
        private Consumer<String> eventConsumer;

        public Context()
        {
            termBufferSize(Configuration.termBufferSize());
            termBufferSizeMax(Configuration.termBufferSizeMax());
            initialWindowSize(Configuration.initialWindowSize());
            statusMessageTimeout(Configuration.statusMessageTimeout());
            dataLossRate(Configuration.dataLossRate());
            dataLossSeed(Configuration.dataLossSeed());
            controlLossRate(Configuration.controlLossRate());
            controlLossSeed(Configuration.controlLossSeed());

            eventConsumer = System.out::println;
            eventBufferSize = EventConfiguration.bufferSize();

            warnIfDirectoriesExist = true;
        }

        public Context conclude()
        {
            try
            {
                super.conclude();

                mtuLength(getInteger(MTU_LENGTH_PROP_NAME, MTU_LENGTH_DEFAULT));

                final ByteBuffer eventByteBuffer = ByteBuffer.allocate(eventBufferSize);

                if (null == eventLogger)
                {
                    eventLogger = new EventLogger(eventByteBuffer);
                }

                toEventReader(new ManyToOneRingBuffer(new AtomicBuffer(eventByteBuffer)));

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

                toDriverCommands(new ManyToOneRingBuffer(new AtomicBuffer(toDriverBuffer)));

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

                if (null == systemCounters)
                {
                    systemCounters = new SystemCounters(countersManager);
                }

                receiverProxy(new ReceiverProxy(receiverCommandQueue(), systemCounters.receiverProxyFails()));
                senderProxy(new SenderProxy(senderCommandQueue(), systemCounters.senderProxyFails()));
                driverConductorProxy(new DriverConductorProxy(conductorCommandQueue, systemCounters.conductorProxyFails()));

                termBuffersFactory(
                    new TermBuffersFactory(dataDirName(), publicationTermBufferSize, maxConnectionTermBufferSize, eventLogger));

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

        public Context conductorCommandQueue(final OneToOneConcurrentArrayQueue<Object> conductorCommandQueue)
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

        public Context unicastSenderFlowControl(final Supplier<SenderFlowControl> senderFlowControl)
        {
            this.unicastSenderFlowControl = senderFlowControl;
            return this;
        }

        public Context multicastSenderFlowControl(final Supplier<SenderFlowControl> senderFlowControl)
        {
            this.multicastSenderFlowControl = senderFlowControl;
            return this;
        }

        public Context conductorTimerWheel(final TimerWheel wheel)
        {
            this.conductorTimerWheel = wheel;
            return this;
        }

        public Context receiverCommandQueue(final OneToOneConcurrentArrayQueue<Object> receiverCommandQueue)
        {
            this.receiverCommandQueue = receiverCommandQueue;
            return this;
        }

        public Context senderCommandQueue(final OneToOneConcurrentArrayQueue<Object> senderCommandQueue)
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

        public Context clientProxy(final ClientProxy clientProxy)
        {
            this.clientProxy = clientProxy;
            return this;
        }

        public Context toDriverCommands(final RingBuffer toDriverCommands)
        {
            this.toDriverCommands = toDriverCommands;
            return this;
        }

        public Context countersManager(final CountersManager countersManager)
        {
            this.countersManager = countersManager;
            return this;
        }

        public Context termBufferSize(final int termBufferSize)
        {
            this.publicationTermBufferSize = termBufferSize;
            return this;
        }

        public Context termBufferSizeMax(final int termBufferSizeMax)
        {
            this.maxConnectionTermBufferSize = termBufferSizeMax;
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

        public Context toEventReader(final RingBuffer toEventReader)
        {
            this.toEventReader = toEventReader;
            return this;
        }

        public Context eventBufferSize(final int size)
        {
            this.eventBufferSize = size;
            return this;
        }

        public Context dataLossRate(final double lossRate)
        {
            this.dataLossRate = lossRate;
            return this;
        }

        public Context dataLossSeed(final long lossSeed)
        {
            this.dataLossSeed = lossSeed;
            return this;
        }

        public Context controlLossRate(final double lossRate)
        {
            this.controlLossRate = lossRate;
            return this;
        }

        public Context controlLossSeed(final long lossSeed)
        {
            this.controlLossSeed = lossSeed;
            return this;
        }

        public Context systemCounters(final SystemCounters systemCounters)
        {
            this.systemCounters = systemCounters;
            return this;
        }

        public OneToOneConcurrentArrayQueue<Object> conductorCommandQueue()
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

        public Supplier<SenderFlowControl> unicastSenderFlowControl()
        {
            return unicastSenderFlowControl;
        }

        public Supplier<SenderFlowControl> multicastSenderFlowControl()
        {
            return multicastSenderFlowControl;
        }

        public TimerWheel conductorTimerWheel()
        {
            return conductorTimerWheel;
        }

        public OneToOneConcurrentArrayQueue<Object> receiverCommandQueue()
        {
            return receiverCommandQueue;
        }

        public OneToOneConcurrentArrayQueue<Object> senderCommandQueue()
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

        public ClientProxy clientProxy()
        {
            return clientProxy;
        }

        public RingBuffer toDriverCommands()
        {
            return toDriverCommands;
        }

        public CountersManager countersManager()
        {
            return countersManager;
        }

        public int termBufferSize()
        {
            return publicationTermBufferSize;
        }

        public int termBufferSizeMax()
        {
            return maxConnectionTermBufferSize;
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

        public Consumer<Exception> exceptionConsumer()
        {
            return eventLogger::logException;
        }

        public double dataLossRate()
        {
            return dataLossRate;
        }

        public long dataLossSeed()
        {
            return dataLossSeed;
        }

        public double controlLossRate()
        {
            return controlLossRate;
        }

        public long controlLossSeed()
        {
            return controlLossSeed;
        }

        public int mtuLength()
        {
            return mtuLength;
        }

        public CommonContext mtuLength(final int mtuLength)
        {
            this.mtuLength = mtuLength;
            return this;
        }

        public SystemCounters systemCounters()
        {
            return systemCounters;
        }

        public Consumer<String> eventConsumer()
        {
            return eventConsumer;
        }

        public int eventBufferSize()
        {
            return eventBufferSize;
        }

        public RingBuffer toEventReader()
        {
            return toEventReader;
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

            if (null != systemCounters)
            {
                systemCounters.close();
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
    }
}
