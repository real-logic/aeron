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

import uk.co.real_logic.aeron.common.*;
import uk.co.real_logic.aeron.common.concurrent.SigIntBarrier;
import uk.co.real_logic.aeron.common.event.EventConfiguration;
import uk.co.real_logic.aeron.common.event.EventLogger;
import uk.co.real_logic.aeron.driver.buffer.RawLogFactory;
import uk.co.real_logic.aeron.driver.cmd.DriverConductorCmd;
import uk.co.real_logic.aeron.driver.cmd.ReceiverCmd;
import uk.co.real_logic.aeron.driver.cmd.SenderCmd;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.TimerWheel;
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.agrona.concurrent.broadcast.BroadcastTransmitter;
import uk.co.real_logic.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import uk.co.real_logic.agrona.concurrent.ringbuffer.RingBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.lang.Integer.getInteger;
import static uk.co.real_logic.aeron.driver.Configuration.*;
import static uk.co.real_logic.agrona.IoUtil.deleteIfExists;
import static uk.co.real_logic.agrona.IoUtil.mapNewFile;

/**
 * Main class for JVM-based media driver
 *
 * Usage:
 * <code>
 * $ java -jar aeron-driver.jar
 * $ java -Doption=value -jar aeron-driver.jar
 * </code>
 * Properties
 * <ul>
 * <li><code>aeron.rcv.buffer.length</code>: Use int value as length of buffer for receiving from network.</li>
 * <li><code>aeron.command.buffer.length</code>: Use int value as length of the command buffers between threads.</li>
 * <li><code>aeron.conductor.buffer.length</code>: Use int value as length of the conductor buffers between the media
 * driver and the client.</li>
 * </ul>
 */
public final class MediaDriver implements AutoCloseable
{
    private final File adminDirectory;
    private final File dataDirectory;
    private final List<AgentRunner> runners;
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

        ensureDirectoriesAreRecreated();

        ctx.unicastSenderFlowControl(Configuration::unicastSenderFlowControlStrategy)
           .multicastSenderFlowControl(Configuration::multicastSenderFlowControlStrategy)
           .conductorTimerWheel(Configuration.newConductorTimerWheel())
           .conductorCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
           .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
           .senderCommandQueue(new OneToOneConcurrentArrayQueue<>(Configuration.CMD_QUEUE_CAPACITY))
           .conclude();

        final AtomicCounter driverExceptions = ctx.systemCounters().driverExceptions();

        final Receiver receiver = new Receiver(ctx);
        final Sender sender = new Sender(ctx);
        final DriverConductor driverConductor = new DriverConductor(ctx);

        ctx.receiverProxy().receiver(receiver);
        ctx.senderProxy().sender(sender);
        ctx.driverConductorProxy().driverConductor(driverConductor);

        switch (ctx.threadingMode)
        {
            case SHARED:
                runners = Collections.singletonList(
                    new AgentRunner(ctx.sharedIdleStrategy, ctx.exceptionConsumer(), driverExceptions,
                        new CompositeAgent(sender, new CompositeAgent(receiver, driverConductor)))
                );
                break;

            case SHARED_NETWORK:
                runners = Arrays.asList(
                    new AgentRunner(ctx.sharedNetworkIdleStrategy, ctx.exceptionConsumer(), driverExceptions,
                                    new CompositeAgent(sender, receiver)),
                    new AgentRunner(ctx.conductorIdleStrategy, ctx.exceptionConsumer(), driverExceptions, driverConductor)
                );
                break;

            default:
            case DEDICATED:
                runners = Arrays.asList(
                    new AgentRunner(ctx.senderIdleStrategy, ctx.exceptionConsumer(), driverExceptions, sender),
                    new AgentRunner(ctx.receiverIdleStrategy, ctx.exceptionConsumer(), driverExceptions, receiver),
                    new AgentRunner(ctx.conductorIdleStrategy, ctx.exceptionConsumer(), driverExceptions, driverConductor)
                );
                break;
        }

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
            runners.forEach(AgentRunner::close);

            freeSocketsForReuseOnWindows();
            ctx.close();

            deleteDirectories();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void freeSocketsForReuseOnWindows()
    {
        ctx.receiverNioSelector().selectNowWithoutProcessing();
        ctx.conductorNioSelector().selectNowWithoutProcessing();
    }

    private MediaDriver start()
    {
        runners.forEach(
            (runner) ->
            {
                final Thread thread = new Thread(runner);
                thread.setName(runner.agent().roleName());
                thread.start();
            });

        return this;
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
    }

    private void deleteDirectories() throws Exception
    {
        if (ctx.dirsDeleteOnExit())
        {
            IoUtil.delete(adminDirectory, false);
            IoUtil.delete(dataDirectory, false);
        }
    }

    public static class Context extends CommonContext
    {
        private RawLogFactory rawLogFactory;
        private TransportPoller receiverTransportPoller;
        private TransportPoller conductorTransportPoller;
        private Supplier<SenderFlowControl> unicastSenderFlowControl;
        private Supplier<SenderFlowControl> multicastSenderFlowControl;
        private TimerWheel conductorTimerWheel;
        private OneToOneConcurrentArrayQueue<DriverConductorCmd> conductorCommandQueue;
        private OneToOneConcurrentArrayQueue<ReceiverCmd> receiverCommandQueue;
        private OneToOneConcurrentArrayQueue<SenderCmd> senderCommandQueue;
        private ReceiverProxy receiverProxy;
        private SenderProxy senderProxy;
        private DriverConductorProxy driverConductorProxy;
        private IdleStrategy conductorIdleStrategy;
        private IdleStrategy senderIdleStrategy;
        private IdleStrategy receiverIdleStrategy;
        private IdleStrategy sharedNetworkIdleStrategy;
        private IdleStrategy sharedIdleStrategy;
        private ClientProxy clientProxy;
        private RingBuffer toDriverCommands;
        private RingBuffer toEventReader;

        private MappedByteBuffer cncByteBuffer;
        private UnsafeBuffer cncMetaDataBuffer;

        private CountersManager countersManager;
        private SystemCounters systemCounters;

        private int publicationTermBufferLength;
        private int maxConnectionTermBufferLength;
        private int initialWindowLength;
        private int eventBufferLength;
        private long statusMessageTimeout;
        private long dataLossSeed;
        private long controlLossSeed;
        private double dataLossRate;
        private double controlLossRate;
        private int mtuLength;

        private boolean warnIfDirectoriesExist;
        private EventLogger eventLogger;
        private Consumer<String> eventConsumer;
        private ThreadingMode threadingMode;

        public Context()
        {
            termBufferLength(Configuration.termBufferLength());
            termBufferMaxLength(Configuration.termBufferLengthMax());
            initialWindowLength(Configuration.initialWindowLength());
            statusMessageTimeout(Configuration.statusMessageTimeout());
            dataLossRate(Configuration.dataLossRate());
            dataLossSeed(Configuration.dataLossSeed());
            controlLossRate(Configuration.controlLossRate());
            controlLossSeed(Configuration.controlLossSeed());

            eventConsumer = System.out::println;
            eventBufferLength = EventConfiguration.bufferLength();

            warnIfDirectoriesExist = true;
        }

        public Context conclude()
        {
            super.conclude();

            try
            {
                if (threadingMode == null)
                {
                    threadingMode = Configuration.threadingMode();
                }

                mtuLength(getInteger(MTU_LENGTH_PROP_NAME, MTU_LENGTH_DEFAULT));

                final ByteBuffer eventByteBuffer = ByteBuffer.allocate(eventBufferLength);

                if (null == eventLogger)
                {
                    eventLogger = new EventLogger(eventByteBuffer);
                }

                toEventReader(new ManyToOneRingBuffer(new UnsafeBuffer(eventByteBuffer)));

                receiverNioSelector(new TransportPoller());
                conductorNioSelector(new TransportPoller());

                Configuration.validateTermBufferLength(termBufferLength());
                Configuration.validateInitialWindowLength(initialWindowLength(), mtuLength());

                deleteIfExists(cncFile());

                if (dirsDeleteOnExit())
                {
                    cncFile().deleteOnExit();
                }

                cncByteBuffer = mapNewFile(
                    cncFile(),
                    CncFileDescriptor.computeCncFileLength(
                        CONDUCTOR_BUFFER_LENGTH + TO_CLIENTS_BUFFER_LENGTH +
                            COUNTER_BUFFERS_LENGTH + COUNTER_BUFFERS_LENGTH));

                cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
                CncFileDescriptor.fillMetaData(
                    cncMetaDataBuffer,
                    CONDUCTOR_BUFFER_LENGTH,
                    TO_CLIENTS_BUFFER_LENGTH,
                    COUNTER_BUFFERS_LENGTH,
                    COUNTER_BUFFERS_LENGTH);

                final BroadcastTransmitter transmitter =
                    new BroadcastTransmitter(CncFileDescriptor.createToClientsBuffer(cncByteBuffer, cncMetaDataBuffer));

                clientProxy(new ClientProxy(transmitter, eventLogger));

                toDriverCommands(
                    new ManyToOneRingBuffer(CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer)));

                concludeCounters();

                receiverProxy(new ReceiverProxy(threadingMode, receiverCommandQueue(), systemCounters.receiverProxyFails()));
                senderProxy(new SenderProxy(threadingMode, senderCommandQueue(), systemCounters.senderProxyFails()));
                driverConductorProxy(new DriverConductorProxy(
                    threadingMode, conductorCommandQueue, systemCounters.conductorProxyFails()));

                rawLogBuffersFactory(new RawLogFactory(
                    dataDirName(), publicationTermBufferLength, maxConnectionTermBufferLength, eventLogger));

                concludeIdleStrategies();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return this;
        }

        public Context conductorCommandQueue(final OneToOneConcurrentArrayQueue<DriverConductorCmd> conductorCommandQueue)
        {
            this.conductorCommandQueue = conductorCommandQueue;
            return this;
        }

        public Context rawLogBuffersFactory(final RawLogFactory rawLogFactory)
        {
            this.rawLogFactory = rawLogFactory;
            return this;
        }

        public Context receiverNioSelector(final TransportPoller transportPoller)
        {
            this.receiverTransportPoller = transportPoller;
            return this;
        }

        public Context conductorNioSelector(final TransportPoller transportPoller)
        {
            this.conductorTransportPoller = transportPoller;
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

        public Context conductorTimerWheel(final TimerWheel timerWheel)
        {
            this.conductorTimerWheel = timerWheel;
            return this;
        }

        public Context receiverCommandQueue(final OneToOneConcurrentArrayQueue<ReceiverCmd> receiverCommandQueue)
        {
            this.receiverCommandQueue = receiverCommandQueue;
            return this;
        }

        public Context senderCommandQueue(final OneToOneConcurrentArrayQueue<SenderCmd> senderCommandQueue)
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

        public Context sharedNetworkIdleStrategy(final IdleStrategy strategy)
        {
            this.sharedNetworkIdleStrategy = strategy;
            return this;
        }

        public Context sharedIdleStrategy(final IdleStrategy strategy)
        {
            this.sharedIdleStrategy = strategy;
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

        public Context termBufferLength(final int termBufferLength)
        {
            this.publicationTermBufferLength = termBufferLength;
            return this;
        }

        public Context termBufferMaxLength(final int termBufferMaxLength)
        {
            this.maxConnectionTermBufferLength = termBufferMaxLength;
            return this;
        }

        public Context initialWindowLength(final int initialWindowLength)
        {
            this.initialWindowLength = initialWindowLength;
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

        public Context eventBufferLength(final int length)
        {
            this.eventBufferLength = length;
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

        public Context threadingMode(final ThreadingMode threadingMode)
        {
            this.threadingMode = threadingMode;
            return this;
        }

        public OneToOneConcurrentArrayQueue<DriverConductorCmd> conductorCommandQueue()
        {
            return conductorCommandQueue;
        }

        public RawLogFactory rawLogBuffersFactory()
        {
            return rawLogFactory;
        }

        public TransportPoller receiverNioSelector()
        {
            return receiverTransportPoller;
        }

        public TransportPoller conductorNioSelector()
        {
            return conductorTransportPoller;
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

        public OneToOneConcurrentArrayQueue<ReceiverCmd> receiverCommandQueue()
        {
            return receiverCommandQueue;
        }

        public OneToOneConcurrentArrayQueue<SenderCmd> senderCommandQueue()
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

        public IdleStrategy sharedNetworkIdleStrategy()
        {
            return sharedNetworkIdleStrategy;
        }

        public IdleStrategy sharedIdleStrategy()
        {
            return sharedIdleStrategy;
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

        public int termBufferLength()
        {
            return publicationTermBufferLength;
        }

        public int termBufferMaxLength()
        {
            return maxConnectionTermBufferLength;
        }

        public int initialWindowLength()
        {
            return initialWindowLength;
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

        public Consumer<Throwable> exceptionConsumer()
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

        public int eventBufferLength()
        {
            return eventBufferLength;
        }

        public RingBuffer toEventReader()
        {
            return toEventReader;
        }

        public void close()
        {
            if (null != systemCounters)
            {
                systemCounters.close();
            }

            IoUtil.unmap(cncByteBuffer);

            super.close();
        }

        private void concludeCounters()
        {
            if (countersManager() == null)
            {
                if (counterLabelsBuffer() == null)
                {
                    counterLabelsBuffer(CncFileDescriptor.createCounterLabelsBuffer(cncByteBuffer, cncMetaDataBuffer));
                }

                if (countersBuffer() == null)
                {
                    countersBuffer(CncFileDescriptor.createCounterValuesBuffer(cncByteBuffer, cncMetaDataBuffer));
                }

                countersManager(new CountersManager(counterLabelsBuffer(), countersBuffer()));
            }

            if (null == systemCounters)
            {
                systemCounters = new SystemCounters(countersManager);
            }
        }

        private void concludeIdleStrategies()
        {
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

            if (null == sharedNetworkIdleStrategy)
            {
                sharedNetworkIdleStrategy(Configuration.agentIdleStrategy());
            }

            if (null == sharedIdleStrategy)
            {
                sharedIdleStrategy(Configuration.agentIdleStrategy());
            }
        }
    }
}
