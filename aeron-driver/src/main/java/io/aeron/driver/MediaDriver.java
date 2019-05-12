/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.*;
import io.aeron.driver.buffer.LogFactory;
import io.aeron.driver.exceptions.ActiveDriverException;
import io.aeron.driver.media.*;
import io.aeron.driver.buffer.FileStoreLogFactory;
import io.aeron.driver.reports.LossReport;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.LoggingErrorHandler;
import org.agrona.concurrent.ringbuffer.*;
import org.agrona.concurrent.status.*;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.CncFileDescriptor.*;
import static io.aeron.driver.Configuration.*;
import static io.aeron.driver.reports.LossReportUtil.mapLossReport;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.driver.status.SystemCounterDescriptor.CONTROLLABLE_IDLE_STRATEGY;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.BitUtil.align;
import static org.agrona.IoUtil.mapNewFile;
import static org.agrona.SystemUtil.loadPropertiesFiles;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;

/**
 * Main class for JVM-based media driver
 * <p>
 * Usage:
 * <code>
 * $ java -jar aeron-driver.jar
 * $ java -Doption=value -jar aeron-driver.jar
 * </code>
 *
 * @see Configuration
 */
public final class MediaDriver implements AutoCloseable
{
    private boolean wasHighResTimerEnabled;
    private final AgentRunner sharedRunner;
    private final AgentRunner sharedNetworkRunner;
    private final AgentRunner conductorRunner;
    private final AgentRunner receiverRunner;
    private final AgentRunner senderRunner;
    private final AgentInvoker sharedInvoker;
    private final Context ctx;

    /**
     * Start Media Driver as a stand-alone process.
     *
     * @param args command line arguments
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();
        final MediaDriver.Context ctx = new MediaDriver.Context();

        ctx.terminationHook(barrier::signal);

        try (MediaDriver ignore = MediaDriver.launch(ctx))
        {
            barrier.await();

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

        ctx.concludeAeronDirectory();

        ensureDirectoryIsRecreated(ctx);
        validateSocketBufferLengths(ctx);

        ctx.conclude();

        final DriverConductor conductor = new DriverConductor(ctx);
        final Receiver receiver = new Receiver(ctx);
        final Sender sender = new Sender(ctx);

        ctx.receiverProxy().receiver(receiver);
        ctx.senderProxy().sender(sender);
        ctx.driverConductorProxy().driverConductor(conductor);

        final AtomicCounter errorCounter = ctx.systemCounters().get(ERRORS);
        final ErrorHandler errorHandler = ctx.errorHandler();

        switch (ctx.threadingMode())
        {
            case INVOKER:
                sharedInvoker = new AgentInvoker(
                    errorHandler, errorCounter, new CompositeAgent(sender, receiver, conductor));
                sharedRunner = null;
                sharedNetworkRunner = null;
                conductorRunner = null;
                receiverRunner = null;
                senderRunner = null;
                break;

            case SHARED:
                sharedRunner = new AgentRunner(
                    ctx.sharedIdleStrategy(),
                    errorHandler,
                    errorCounter,
                    new CompositeAgent(sender, receiver, conductor));
                sharedNetworkRunner = null;
                conductorRunner = null;
                receiverRunner = null;
                senderRunner = null;
                sharedInvoker = null;
                break;

            case SHARED_NETWORK:
                sharedNetworkRunner = new AgentRunner(
                    ctx.sharedNetworkIdleStrategy(), errorHandler, errorCounter, new CompositeAgent(sender, receiver));
                conductorRunner = new AgentRunner(ctx.conductorIdleStrategy(), errorHandler, errorCounter, conductor);
                sharedRunner = null;
                receiverRunner = null;
                senderRunner = null;
                sharedInvoker = null;
                break;

            default:
            case DEDICATED:
                senderRunner = new AgentRunner(ctx.senderIdleStrategy(), errorHandler, errorCounter, sender);
                receiverRunner = new AgentRunner(ctx.receiverIdleStrategy(), errorHandler, errorCounter, receiver);
                conductorRunner = new AgentRunner(ctx.conductorIdleStrategy(), errorHandler, errorCounter, conductor);
                sharedNetworkRunner = null;
                sharedRunner = null;
                sharedInvoker = null;
                break;
        }
    }

    /**
     * Launch an isolated MediaDriver embedded in the current process with a generated aeronDirectoryName that can be
     * retrieved by calling aeronDirectoryName.
     * <p>
     * If the aeronDirectoryName is set as a system property to something different than
     * {@link CommonContext#AERON_DIR_PROP_DEFAULT} then this set value will be used.
     *
     * @return the newly started MediaDriver.
     */
    public static MediaDriver launchEmbedded()
    {
        return launchEmbedded(new Context());
    }

    /**
     * Launch an isolated MediaDriver embedded in the current process with a provided configuration ctx and a generated
     * aeronDirectoryName (overwrites configured {@link Context#aeronDirectoryName()}) that can be retrieved by calling
     * aeronDirectoryName.
     * <p>
     * If the aeronDirectoryName is set as a system property, or via context, to something different than
     * {@link CommonContext#AERON_DIR_PROP_DEFAULT} then this set value will be used.
     *
     * @param ctx containing the configuration options.
     * @return the newly started MediaDriver.
     */
    public static MediaDriver launchEmbedded(final Context ctx)
    {
        if (CommonContext.AERON_DIR_PROP_DEFAULT.equals(ctx.aeronDirectoryName()))
        {
            ctx.aeronDirectoryName(CommonContext.generateRandomDirName());
        }

        return launch(ctx);
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
     * Launch a MediaDriver embedded in the current process and provided a configuration ctx.
     *
     * @param ctx containing the configuration options.
     * @return the newly created MediaDriver.
     */
    public static MediaDriver launch(final Context ctx)
    {
        return new MediaDriver(ctx).start();
    }

    /**
     * Get the {@link MediaDriver.Context} that is used by this {@link MediaDriver}.
     *
     * @return the {@link MediaDriver.Context} that is used by this {@link MediaDriver}.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * Get the {@link AgentInvoker} for the shared agents when running without threads.
     *
     * @return the {@link AgentInvoker} for the shared agents when running without threads.
     */
    public AgentInvoker sharedAgentInvoker()
    {
        return sharedInvoker;
    }

    /**
     * Shutdown the media driver by stopping all threads and freeing resources.
     */
    public void close()
    {
        CloseHelper.close(sharedRunner);
        CloseHelper.close(sharedNetworkRunner);
        CloseHelper.close(receiverRunner);
        CloseHelper.close(senderRunner);
        CloseHelper.close(conductorRunner);
        CloseHelper.close(sharedInvoker);

        if (ctx.useWindowsHighResTimer() && SystemUtil.osName().startsWith("win"))
        {
            if (!wasHighResTimerEnabled)
            {
                HighResolutionTimer.disable();
            }
        }
    }

    /**
     * Used to access the configured aeronDirectoryName for this MediaDriver, typically used after the
     * {@link #launchEmbedded()} method is used.
     *
     * @return the context aeronDirectoryName
     */
    public String aeronDirectoryName()
    {
        return ctx.aeronDirectoryName();
    }

    private MediaDriver start()
    {
        if (ctx.useWindowsHighResTimer() && SystemUtil.osName().startsWith("win"))
        {
            wasHighResTimerEnabled = HighResolutionTimer.isEnabled();
            if (!wasHighResTimerEnabled)
            {
                HighResolutionTimer.enable();
            }
        }

        if (null != conductorRunner)
        {
            AgentRunner.startOnThread(conductorRunner, ctx.conductorThreadFactory());
        }

        if (null != senderRunner)
        {
            AgentRunner.startOnThread(senderRunner, ctx.senderThreadFactory());
        }

        if (null != receiverRunner)
        {
            AgentRunner.startOnThread(receiverRunner, ctx.receiverThreadFactory());
        }

        if (null != sharedNetworkRunner)
        {
            AgentRunner.startOnThread(sharedNetworkRunner, ctx.sharedNetworkThreadFactory());
        }

        if (null != sharedRunner)
        {
            AgentRunner.startOnThread(sharedRunner, ctx.sharedThreadFactory());
        }

        if (null != sharedInvoker)
        {
            sharedInvoker.start();
        }

        return this;
    }

    private static void ensureDirectoryIsRecreated(final Context ctx)
    {
        if (ctx.aeronDirectory().isDirectory())
        {
            if (ctx.warnIfDirectoryExists())
            {
                System.err.println("WARNING: " + ctx.aeronDirectory() + " already exists.");
            }

            if (!ctx.dirDeleteOnStart())
            {
                final Consumer<String> logger = ctx.warnIfDirectoryExists() ? System.err::println : (s) -> {};
                final MappedByteBuffer cncByteBuffer = ctx.mapExistingCncFile(logger);
                try
                {
                    if (CommonContext.isDriverActive(ctx.driverTimeoutMs(), logger, cncByteBuffer))
                    {
                        throw new ActiveDriverException("active driver detected");
                    }

                    reportExistingErrors(ctx, cncByteBuffer);
                }
                finally
                {
                    IoUtil.unmap(cncByteBuffer);
                }
            }

            ctx.deleteAeronDirectory();
        }

        IoUtil.ensureDirectoryExists(ctx.aeronDirectory(), "aeron");
    }

    private static void reportExistingErrors(final Context ctx, final MappedByteBuffer cncByteBuffer)
    {
        try
        {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final int observations = ctx.saveErrorLog(new PrintStream(baos, false, "UTF-8"), cncByteBuffer);
            if (observations > 0)
            {
                final StringBuilder builder = new StringBuilder(ctx.aeronDirectoryName());
                removeTrailingSlashes(builder);

                final SimpleDateFormat dateFormat = new SimpleDateFormat("-yyyy-MM-dd-HH-mm-ss-SSSZ");
                builder.append(dateFormat.format(new Date())).append("-error.log");
                final String errorLogFilename = builder.toString();

                System.err.println("WARNING: Existing errors saved to: " + errorLogFilename);
                try (FileOutputStream out = new FileOutputStream(errorLogFilename))
                {
                    baos.writeTo(out);
                }
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private static void removeTrailingSlashes(final StringBuilder builder)
    {
        while (builder.length() > 1)
        {
            final int lastCharIndex = builder.length() - 1;
            final char c = builder.charAt(lastCharIndex);
            if ('/' == c || '\\' == c)
            {
                builder.setLength(lastCharIndex);
            }
            else
            {
                break;
            }
        }
    }

    /**
     * Context for the {@link MediaDriver} that can be used to provide overrides for {@link Configuration}.
     * <p>
     * <b>Note:</b> Do not reuse instances of this {@link Context} across different {@link MediaDriver}s.
     * <p>
     * The context will be owned by {@link DriverConductor} after a successful
     * {@link MediaDriver#launch(Context)} and closed via {@link MediaDriver#close()}.
     */
    public static class Context extends CommonContext
    {
        private boolean printConfigurationOnStart = Configuration.printConfigurationOnStart();
        private boolean useWindowsHighResTimer = Configuration.useWindowsHighResTimer();
        private boolean warnIfDirectoryExists = Configuration.warnIfDirExists();
        private boolean dirDeleteOnStart = Configuration.dirDeleteOnStart();
        private boolean termBufferSparseFile = Configuration.termBufferSparseFile();
        private boolean performStorageChecks = Configuration.performStorageChecks();
        private boolean spiesSimulateConnection = Configuration.spiesSimulateConnection();
        private boolean reliableStream = Configuration.reliableStream();
        private boolean tetherSubscriptions = Configuration.tetherSubscriptions();

        private long lowStorageWarningThreshold = Configuration.lowStorageWarningThreshold();
        private long timerIntervalNs = Configuration.timerIntervalNs();
        private long clientLivenessTimeoutNs = Configuration.clientLivenessTimeoutNs();
        private long imageLivenessTimeoutNs = Configuration.imageLivenessTimeoutNs();
        private long publicationUnblockTimeoutNs = Configuration.publicationUnblockTimeoutNs();
        private long publicationConnectionTimeoutNs = Configuration.publicationConnectionTimeoutNs();
        private long publicationLingerTimeoutNs = Configuration.publicationLingerTimeoutNs();
        private long untetheredWindowLimitTimeoutNs = Configuration.untetheredWindowLimitTimeoutNs();
        private long untetheredRestingTimeoutNs = Configuration.untetheredRestingTimeoutNs();
        private long statusMessageTimeoutNs = Configuration.statusMessageTimeoutNs();
        private long counterFreeToReuseTimeoutNs = Configuration.counterFreeToReuseTimeoutNs();
        private long retransmitUnicastDelayNs = Configuration.retransmitUnicastDelayNs();
        private long retransmitUnicastLingerNs = Configuration.retransmitUnicastLingerNs();
        private long nakUnicastDelayNs = Configuration.nakUnicastDelayNs();
        private long nakMulticastMaxBackoffNs = Configuration.nakMulticastMaxBackoffNs();
        private int conductorBufferLength = Configuration.conductorBufferLength();
        private int toClientsBufferLength = Configuration.toClientsBufferLength();
        private int counterValuesBufferLength = Configuration.counterValuesBufferLength();
        private int errorBufferLength = Configuration.errorBufferLength();
        private int nakMulticastGroupSize = Configuration.nakMulticastGroupSize();
        private int publicationTermBufferLength = Configuration.termBufferLength();
        private int ipcTermBufferLength = Configuration.ipcTermBufferLength();
        private int publicationTermWindowLength = Configuration.publicationTermWindowLength();
        private int ipcPublicationTermWindowLength = Configuration.ipcPublicationTermWindowLength();
        private int initialWindowLength = Configuration.initialWindowLength();
        private int socketSndbufLength = Configuration.socketSndbufLength();
        private int socketRcvbufLength = Configuration.socketRcvbufLength();
        private int socketMulticastTtl = Configuration.socketMulticastTtl();
        private int mtuLength = Configuration.mtuLength();
        private int ipcMtuLength = Configuration.ipcMtuLength();
        private int filePageSize = Configuration.filePageSize();
        private int publicationReservedSessionIdLow = Configuration.publicationReservedSessionIdLow();
        private int publicationReservedSessionIdHigh = Configuration.publicationReservedSessionIdHigh();
        private int lossReportBufferLength = Configuration.lossReportBufferLength();
        private int sendToStatusMessagePollRatio = Configuration.sendToStatusMessagePollRatio();

        private EpochClock epochClock;
        private NanoClock nanoClock;
        private CachedEpochClock cachedEpochClock;
        private CachedNanoClock cachedNanoClock;
        private ThreadingMode threadingMode;
        private ThreadFactory conductorThreadFactory;
        private ThreadFactory senderThreadFactory;
        private ThreadFactory receiverThreadFactory;
        private ThreadFactory sharedThreadFactory;
        private ThreadFactory sharedNetworkThreadFactory;
        private IdleStrategy conductorIdleStrategy;
        private IdleStrategy senderIdleStrategy;
        private IdleStrategy receiverIdleStrategy;
        private IdleStrategy sharedNetworkIdleStrategy;
        private IdleStrategy sharedIdleStrategy;
        private SendChannelEndpointSupplier sendChannelEndpointSupplier;
        private ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier;
        private ReceiveChannelEndpointThreadLocals receiveChannelEndpointThreadLocals;
        private MutableDirectBuffer tempBuffer;
        private FlowControlSupplier unicastFlowControlSupplier;
        private FlowControlSupplier multicastFlowControlSupplier;
        private byte[] applicationSpecificFeedback;
        private CongestionControlSupplier congestionControlSupplier;
        private FeedbackDelayGenerator unicastFeedbackDelayGenerator;
        private FeedbackDelayGenerator multicastFeedbackDelayGenerator;
        private FeedbackDelayGenerator retransmitUnicastDelayGenerator;
        private FeedbackDelayGenerator retransmitUnicastLingerGenerator;
        private TerminationValidator terminationValidator;
        private Runnable terminationHook;

        private DistinctErrorLog errorLog;
        private ErrorHandler errorHandler;
        private boolean useConcurrentCountersManager;
        private CountersManager countersManager;
        private SystemCounters systemCounters;
        private LossReport lossReport;

        private LogFactory logFactory;
        private DataTransportPoller dataTransportPoller;
        private ControlTransportPoller controlTransportPoller;
        private ManyToOneConcurrentArrayQueue<Runnable> driverCommandQueue;
        private OneToOneConcurrentArrayQueue<Runnable> receiverCommandQueue;
        private OneToOneConcurrentArrayQueue<Runnable> senderCommandQueue;
        private ReceiverProxy receiverProxy;
        private SenderProxy senderProxy;
        private DriverConductorProxy driverConductorProxy;
        private ClientProxy clientProxy;
        private RingBuffer toDriverCommands;

        private MappedByteBuffer lossReportBuffer;
        private MappedByteBuffer cncByteBuffer;
        private UnsafeBuffer cncMetaDataBuffer;

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            return (Context)super.clone();
        }

        /**
         * Free up resources but don't delete files in case they are required for debugging.
         */
        public void close()
        {
            final MappedByteBuffer lossReportBuffer = this.lossReportBuffer;
            this.lossReportBuffer = null;
            IoUtil.unmap(lossReportBuffer);


            final MappedByteBuffer cncByteBuffer = this.cncByteBuffer;
            this.cncByteBuffer = null;
            IoUtil.unmap(cncByteBuffer);

            super.close();
        }

        public Context conclude()
        {
            super.conclude();

            try
            {
                concludeNullProperties();

                validateMtuLength(mtuLength);
                validateMtuLength(ipcMtuLength);
                validatePageSize(filePageSize);
                validateSessionIdRange(publicationReservedSessionIdLow, publicationReservedSessionIdHigh);

                LogBufferDescriptor.checkTermLength(publicationTermBufferLength);
                LogBufferDescriptor.checkTermLength(ipcTermBufferLength);
                validateInitialWindowLength(initialWindowLength, mtuLength);
                validateUnblockTimeout(publicationUnblockTimeoutNs, clientLivenessTimeoutNs, timerIntervalNs);

                cncByteBuffer = mapNewFile(
                    cncFile(),
                    CncFileDescriptor.computeCncFileLength(
                        conductorBufferLength +
                            toClientsBufferLength +
                            Configuration.countersMetadataBufferLength(counterValuesBufferLength) +
                            counterValuesBufferLength +
                            errorBufferLength,
                        filePageSize));

                cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
                CncFileDescriptor.fillMetaData(
                    cncMetaDataBuffer,
                    conductorBufferLength,
                    toClientsBufferLength,
                    Configuration.countersMetadataBufferLength(counterValuesBufferLength),
                    counterValuesBufferLength,
                    clientLivenessTimeoutNs,
                    errorBufferLength,
                    epochClock.time(),
                    SystemUtil.getPid());

                concludeCounters();
                concludeDependantProperties();
                concludeIdleStrategies();

                toDriverCommands.consumerHeartbeatTime(epochClock.time());
                CncFileDescriptor.signalCncReady(cncMetaDataBuffer);
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            if (printConfigurationOnStart)
            {
                System.out.println(this.toString());
            }

            return this;
        }

        /**
         * {@inheritDoc}
         */
        public Context aeronDirectoryName(final String dirName)
        {
            super.aeronDirectoryName(dirName);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        public Context driverTimeoutMs(final long value)
        {
            super.driverTimeoutMs(value);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        public Context countersMetaDataBuffer(final UnsafeBuffer countersMetaDataBuffer)
        {
            super.countersMetaDataBuffer(countersMetaDataBuffer);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        public Context countersValuesBuffer(final UnsafeBuffer countersValuesBuffer)
        {
            super.countersValuesBuffer(countersValuesBuffer);
            return this;
        }

        /**
         * Should the driver print it's configuration on start to {@link System#out} at the end of {@link #conclude()}.
         *
         * @return true if the configuration should be printed on start.
         * @see Configuration#PRINT_CONFIGURATION_ON_START_PROP_NAME
         */
        public boolean printConfigurationOnStart()
        {
            return printConfigurationOnStart;
        }

        /**
         * Should the driver print it's configuration on start to {@link System#out} at the end of {@link #conclude()}.
         *
         * @param printConfigurationOnStart if the configuration should be printed on start.
         * @return this for a fluent API.
         * @see Configuration#PRINT_CONFIGURATION_ON_START_PROP_NAME
         */
        public Context printConfigurationOnStart(final boolean printConfigurationOnStart)
        {
            this.printConfigurationOnStart = printConfigurationOnStart;
            return this;
        }

        /**
         * Should an attempt be made to use the high resolution timers for waiting on Windows.
         *
         * @param useWindowsHighResTimers Should an attempt be made to use the high-res timers for waiting on Windows.
         * @return this for a fluent API.
         * @see Configuration#USE_WINDOWS_HIGH_RES_TIMER_PROP_NAME
         */
        public Context useWindowsHighResTimer(final boolean useWindowsHighResTimers)
        {
            this.useWindowsHighResTimer = useWindowsHighResTimers;
            return this;
        }

        /**
         * Should an attempt be made to use the high resolution timers for waiting on Windows.
         *
         * @return true if an attempt be made to use the high resolution timers for waiting on Windows.
         * @see Configuration#USE_WINDOWS_HIGH_RES_TIMER_PROP_NAME
         */
        public boolean useWindowsHighResTimer()
        {
            return useWindowsHighResTimer;
        }

        /**
         * Should a warning be issued if the {@link #aeronDirectoryName()} exists?
         *
         * @return should a warning be issued if the {@link #aeronDirectoryName()} exists?
         * @see Configuration#DIR_WARN_IF_EXISTS_PROP_NAME
         */
        public boolean warnIfDirectoryExists()
        {
            return warnIfDirectoryExists;
        }

        /**
         * Should a warning be issued if the {@link #aeronDirectoryName()} exists?
         *
         * @param warnIfDirectoryExists warn if the {@link #aeronDirectoryName()} exists?
         * @return this for a fluent API.
         * @see Configuration#DIR_WARN_IF_EXISTS_PROP_NAME
         */
        public Context warnIfDirectoryExists(final boolean warnIfDirectoryExists)
        {
            this.warnIfDirectoryExists = warnIfDirectoryExists;
            return this;
        }

        /**
         * Will the driver attempt to immediately delete {@link #aeronDirectoryName()} on startup.
         *
         * @return true when directory will be deleted, otherwise false.
         * @see Configuration#DIR_DELETE_ON_START_PROP_NAME
         */
        public boolean dirDeleteOnStart()
        {
            return dirDeleteOnStart;
        }

        /**
         * Should the driver attempt to immediately delete {@link #aeronDirectoryName()} on startup.
         *
         * @param dirDeleteOnStart Attempt deletion.
         * @return this for a fluent API.
         * @see Configuration#DIR_DELETE_ON_START_PROP_NAME
         */
        public Context dirDeleteOnStart(final boolean dirDeleteOnStart)
        {
            this.dirDeleteOnStart = dirDeleteOnStart;
            return this;
        }

        /**
         * Should the term buffers be created with sparse files?
         *
         * @return should the term buffers be created with sparse files?
         * @see Configuration#TERM_BUFFER_SPARSE_FILE_PROP_NAME
         */
        public boolean termBufferSparseFile()
        {
            return termBufferSparseFile;
        }

        /**
         * Should the term buffer be created with sparse files?
         *
         * @param termBufferSparseFile should the term buffers be created with sparse files?
         * @return this for a fluent API.
         * @see Configuration#TERM_BUFFER_SPARSE_FILE_PROP_NAME
         */
        public Context termBufferSparseFile(final boolean termBufferSparseFile)
        {
            this.termBufferSparseFile = termBufferSparseFile;
            return this;
        }

        /**
         * Length of the {@link RingBuffer} for sending commands to the driver conductor from clients.
         *
         * @return length of the {@link RingBuffer} for sending commands to the driver conductor from clients.
         * @see Configuration#CONDUCTOR_BUFFER_LENGTH_PROP_NAME
         */
        public int conductorBufferLength()
        {
            return conductorBufferLength;
        }

        /**
         * Length of the {@link RingBuffer} for sending commands to the driver conductor from clients.
         *
         * @param length of the {@link RingBuffer} for sending commands to the driver conductor from clients.
         * @return this for a fluent API.
         * @see Configuration#CONDUCTOR_BUFFER_LENGTH_PROP_NAME
         */
        public Context conductorBufferLength(final int length)
        {
            conductorBufferLength = length;
            return this;
        }

        /**
         * Length of the {@link BroadcastTransmitter} buffer for sending events to the clients.
         *
         * @return length of the {@link BroadcastTransmitter} buffer for sending events to the clients.
         * @see Configuration#TO_CLIENTS_BUFFER_LENGTH_PROP_NAME
         */
        public int toClientsBufferLength()
        {
            return toClientsBufferLength;
        }

        /**
         * Length of the {@link BroadcastTransmitter} buffer for sending events to the clients.
         *
         * @param length of the {@link BroadcastTransmitter} buffer for sending events to the clients.
         * @return this for a fluent API.
         * @see Configuration#TO_CLIENTS_BUFFER_LENGTH_PROP_NAME
         */
        public Context toClientsBufferLength(final int length)
        {
            toClientsBufferLength = length;
            return this;
        }

        /**
         * Length of the buffer for storing values by the {@link CountersManager}.
         *
         * @return length of the of the buffer for storing values by the {@link CountersManager}.
         * @see Configuration#COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME
         */
        public int counterValuesBufferLength()
        {
            return counterValuesBufferLength;
        }

        /**
         * Length of the buffer for storing values by the {@link CountersManager}.
         *
         * @param length of the buffer for storing values by the {@link CountersManager}.
         * @return this for a fluent API.
         * @see Configuration#COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME
         */
        public Context counterValuesBufferLength(final int length)
        {
            counterValuesBufferLength = length;
            return this;
        }

        /**
         * Length of the {@link DistinctErrorLog} buffer for recording exceptions.
         *
         * @return length of the {@link DistinctErrorLog} buffer for recording exceptions.
         * @see Configuration#ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public int errorBufferLength()
        {
            return errorBufferLength;
        }

        /**
         * Length of the {@link DistinctErrorLog} buffer for recording exceptions.
         *
         * @param length of the {@link DistinctErrorLog} buffer for recording exceptions.
         * @return this for a fluent API.
         * @see Configuration#ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public Context errorBufferLength(final int length)
        {
            errorBufferLength = length;
            return this;
        }

        /**
         * Should the driver perform storage checks when allocating files.
         *
         * @return true if the driver should perform storage checks when allocating files.
         * @see Configuration#PERFORM_STORAGE_CHECKS_PROP_NAME
         */
        public boolean performStorageChecks()
        {
            return performStorageChecks;
        }

        /**
         * Should the driver perform storage checks when allocating files.
         *
         * @param performStorageChecks true if the driver should perform storage checks when allocating files.
         * @return this for a fluent API.
         * @see Configuration#PERFORM_STORAGE_CHECKS_PROP_NAME
         */
        public Context performStorageChecks(final boolean performStorageChecks)
        {
            this.performStorageChecks = performStorageChecks;
            return this;
        }

        /**
         * Get the threshold in bytes below which storage warnings are issued.
         *
         * @return the threshold below which storage warnings are issued.
         * @see Configuration#LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME
         */
        public long lowStorageWarningThreshold()
        {
            return lowStorageWarningThreshold;
        }

        /**
         * Get the threshold in bytes below which storage warnings are issued.
         *
         * @param lowStorageWarningThreshold to be set in bytes.
         * @return this for a fluent API.
         * @see Configuration#LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME
         */
        public Context lowStorageWarningThreshold(final long lowStorageWarningThreshold)
        {
            this.lowStorageWarningThreshold = lowStorageWarningThreshold;
            return this;
        }

        /**
         * The length in bytes of the loss report buffer.
         *
         * @return the length in bytes of the loss report buffer.
         * @see Configuration#LOSS_REPORT_BUFFER_LENGTH_PROP_NAME
         */
        public int lossReportBufferLength()
        {
            return lossReportBufferLength;
        }

        /**
         * The length in bytes of the loss report buffer.
         *
         * @param length of the buffer to be used for the loss report.
         * @return this for a fluent API.
         * @see Configuration#LOSS_REPORT_BUFFER_LENGTH_PROP_NAME
         */
        public Context lossReportBufferLength(final int length)
        {
            lossReportBufferLength = length;
            return this;
        }

        /**
         * Page size for alignment of all files.
         *
         * @return page size for alignment of all files.
         * @see Configuration#FILE_PAGE_SIZE_PROP_NAME
         */
        public int filePageSize()
        {
            return filePageSize;
        }

        /**
         * Page size for alignment of all files.
         *
         * @param filePageSize for alignment of file sizes.
         * @return this for a fluent API.
         * @see Configuration#FILE_PAGE_SIZE_PROP_NAME
         */
        public Context filePageSize(final int filePageSize)
        {
            this.filePageSize = filePageSize;
            return this;
        }

        /**
         * Interval in nanoseconds between checks for timers and timeouts.
         *
         * @return nanoseconds between checks for timers and timeouts.
         * @see Configuration#TIMER_INTERVAL_PROP_NAME
         */
        public long timerIntervalNs()
        {
            return timerIntervalNs;
        }

        /**
         * Interval in nanoseconds between checks for timers and timeouts.
         *
         * @param timerIntervalNs nanoseconds between checks for timers and timeouts.
         * @return this for a fluent API.
         * @see Configuration#TIMER_INTERVAL_PROP_NAME
         */
        public Context timerIntervalNs(final long timerIntervalNs)
        {
            this.timerIntervalNs = timerIntervalNs;
            return this;
        }

        /**
         * Time in nanoseconds an Image will be kept alive for its subscribers to consume it once disconnected.
         *
         * @return nanoseconds that an Image will be kept alive for its subscribers to consume it.
         * @see Configuration#IMAGE_LIVENESS_TIMEOUT_PROP_NAME
         */
        public long imageLivenessTimeoutNs()
        {
            return imageLivenessTimeoutNs;
        }

        /**
         * Time in nanoseconds an Image will be kept alive after for its subscribers to consume it once disconnected.
         *
         * @param timeout for keeping an image alive for its subscribers to consume it.
         * @return this for a fluent API.
         * @see Configuration#IMAGE_LIVENESS_TIMEOUT_PROP_NAME
         */
        public Context imageLivenessTimeoutNs(final long timeout)
        {
            this.imageLivenessTimeoutNs = timeout;
            return this;
        }

        /**
         * Time in nanoseconds a publication will linger once it is drained to recover potential tail loss.
         *
         * @return nanoseconds that a publication will linger once it is drained.
         * @see Configuration#PUBLICATION_LINGER_PROP_NAME
         */
        public long publicationLingerTimeoutNs()
        {
            return publicationLingerTimeoutNs;
        }

        /**
         * Time in nanoseconds a publication will linger once it is drained to recover potential tail loss.
         *
         * @param timeoutNs for keeping a publication once it is drained.
         * @return this for a fluent API.
         * @see Configuration#PUBLICATION_LINGER_PROP_NAME
         */
        public Context publicationLingerTimeoutNs(final long timeoutNs)
        {
            this.publicationLingerTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * The timeout for when an untethered subscription that is outside the window will participate
         * in local flow control.
         *
         * @return timeout that a untethered subscription outside the window limit will participate in flow control.
         * @see Configuration#UNTETHERED_WINDOW_LIMIT_TIMEOUT_PROP_NAME
         */
        public long untetheredWindowLimitTimeoutNs()
        {
            return untetheredWindowLimitTimeoutNs;
        }

        /**
         * The timeout for when an untethered subscription that is outside the window will participate
         * in local flow control.
         *
         * @param timeoutNs that a untethered subscription outside the window limit will participate in flow control.
         * @return this for a fluent API.
         * @see Configuration#UNTETHERED_WINDOW_LIMIT_TIMEOUT_PROP_NAME
         */
        public Context untetheredWindowLimitTimeoutNs(final long timeoutNs)
        {
            this.untetheredWindowLimitTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Timeout for when an untethered subscription is resting after not being able to keep up before it is allowed
         * to rejoin a stream.
         *
         * @return timeout that a untethered subscription is resting before being allowed to rejoin a stream.
         * @see Configuration#UNTETHERED_RESTING_TIMEOUT_PROP_NAME
         */
        public long untetheredRestingTimeoutNs()
        {
            return untetheredRestingTimeoutNs;
        }

        /**
         * Timeout for when an untethered subscription is resting after not being able to keep up before it is allowed
         * to rejoin a stream.
         *
         * @param timeoutNs that a untethered subscription is resting before being allowed to rejoin a stream.
         * @return this for a fluent API.
         * @see Configuration#UNTETHERED_RESTING_TIMEOUT_PROP_NAME
         */
        public Context untetheredRestingTimeoutNs(final long timeoutNs)
        {
            this.untetheredRestingTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * The delay before retransmitting after a NAK.
         *
         * @return delay before retransmitting after a NAK.
         * @see Configuration#RETRANSMIT_UNICAST_DELAY_PROP_NAME
         */
        public long retransmitUnicastDelayNs()
        {
            return retransmitUnicastDelayNs;
        }

        /**
         * The delay before retransmitting after a NAK.
         *
         * @param retransmitUnicastDelayNs delay before retransmitting after a NAK.
         * @return this for a fluent API.
         * @see Configuration#RETRANSMIT_UNICAST_DELAY_PROP_NAME
         */
        public Context retransmitUnicastDelayNs(final long retransmitUnicastDelayNs)
        {
            this.retransmitUnicastDelayNs = retransmitUnicastDelayNs;
            return this;
        }

        /**
         * The delay for lingering after a retransmission.
         *
         * @return delay before retransmitting after a NAK.
         * @see Configuration#RETRANSMIT_UNICAST_LINGER_PROP_NAME
         */
        public long retransmitUnicastLingerNs()
        {
            return retransmitUnicastLingerNs;
        }

        /**
         * The delay for lingering after a retransmission.
         *
         * @param retransmitUnicastLingerNs delay before retransmitting after a NAK.
         * @return this for a fluent API.
         * @see Configuration#RETRANSMIT_UNICAST_LINGER_PROP_NAME
         */
        public Context retransmitUnicastLingerNs(final long retransmitUnicastLingerNs)
        {
            this.retransmitUnicastLingerNs = retransmitUnicastLingerNs;
            return this;
        }

        /**
         * The delay before retransmission after an NAK on unicast.
         *
         * @return delay before retransmitting after a NAK.
         * @see Configuration#NAK_UNICAST_DELAY_PROP_NAME
         */
        public long nakUnicastDelayNs()
        {
            return nakUnicastDelayNs;
        }

        /**
         * The delay before retransmission after an NAK on unicast.
         *
         * @param nakUnicastDelayNs delay before retransmission after an NAK on unicast.
         * @return this for a fluent API.
         * @see Configuration#NAK_UNICAST_DELAY_PROP_NAME
         */
        public Context nakUnicastDelayNs(final long nakUnicastDelayNs)
        {
            this.nakUnicastDelayNs = nakUnicastDelayNs;
            return this;
        }


        /**
         * The maximum time to backoff before sending a NAK on multicast.
         *
         * @return maximum time to backoff before sending a NAK on multicast.
         * @see Configuration#NAK_MULTICAST_MAX_BACKOFF_PROP_NAME
         */
        public long nakMulticastMaxBackoffNs()
        {
            return nakMulticastMaxBackoffNs;
        }

        /**
         * The maximum time to backoff before sending a NAK on multicast.
         *
         * @param nakMulticastMaxBackoffNs maximum time to backoff before sending a NAK on multicast.
         * @return this for a fluent API.
         * @see Configuration#NAK_MULTICAST_MAX_BACKOFF_PROP_NAME
         */
        public Context nakMulticastMaxBackoffNs(final long nakMulticastMaxBackoffNs)
        {
            this.nakMulticastMaxBackoffNs = nakUnicastDelayNs;
            return this;
        }

        /**
         * Estimate of the multicast receiver group size on a stream.
         *
         * @return estimate of the multicast receiver group size on a stream.
         * @see Configuration#NAK_MULTICAST_GROUP_SIZE_PROP_NAME
         */
        public int nakMulticastGroupSize()
        {
            return nakMulticastGroupSize;
        }

        /**
         * Estimate of the multicast receiver group size on a stream.
         *
         * @param nakMulticastGroupSize estimate of the multicast receiver group size on a stream.
         * @return this for a fluent API.
         * @see Configuration#NAK_MULTICAST_GROUP_SIZE_PROP_NAME
         */
        public Context nakMulticastGroupSize(final int nakMulticastGroupSize)
        {
            this.nakMulticastGroupSize = nakMulticastGroupSize;
            return this;
        }

        /**
         * Time in nanoseconds after which a client is considered dead if a keep alive is not received.
         *
         * @return time in nanoseconds after which a client is considered dead if a keep alive is not received.
         * @see Configuration#CLIENT_LIVENESS_TIMEOUT_PROP_NAME
         */
        public long clientLivenessTimeoutNs()
        {
            return clientLivenessTimeoutNs;
        }

        /**
         * Time in nanoseconds after which a client is considered dead if a keep alive is not received.
         *
         * @param timeoutNs in nanoseconds after which a client is considered dead if a keep alive is not received.
         * @return this for a fluent API.
         * @see Configuration#CLIENT_LIVENESS_TIMEOUT_PROP_NAME
         */
        public Context clientLivenessTimeoutNs(final long timeoutNs)
        {
            this.clientLivenessTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Time in nanoseconds after which a status message will be sent if data is flowing slowly.
         *
         * @return time in nanoseconds after which a status message will be sent if data is flowing slowly.
         * @see Configuration#STATUS_MESSAGE_TIMEOUT_PROP_NAME
         */
        public long statusMessageTimeoutNs()
        {
            return statusMessageTimeoutNs;
        }

        /**
         * Time in nanoseconds after which a status message will be sent if data is flowing slowly.
         *
         * @param statusMessageTimeoutNs after which a status message will be sent if data is flowing slowly.
         * @return this for a fluent API.
         * @see Configuration#STATUS_MESSAGE_TIMEOUT_PROP_NAME
         */
        public Context statusMessageTimeoutNs(final long statusMessageTimeoutNs)
        {
            this.statusMessageTimeoutNs = statusMessageTimeoutNs;
            return this;
        }

        /**
         * Time in nanoseconds after which a freed counter may be reused.
         *
         * @return time in nanoseconds after which a freed counter may be reused.
         * @see Configuration#COUNTER_FREE_TO_REUSE_TIMEOUT_PROP_NAME
         */
        public long counterFreeToReuseTimeoutNs()
        {
            return counterFreeToReuseTimeoutNs;
        }

        /**
         * Time in nanoseconds after which a freed counter may be reused.
         *
         * @param counterFreeToReuseTimeoutNs after which a freed counter may be reused.
         * @return this for a fluent API.
         * @see Configuration#COUNTER_FREE_TO_REUSE_TIMEOUT_PROP_NAME
         */
        public Context counterFreeToReuseTimeoutNs(final long counterFreeToReuseTimeoutNs)
        {
            this.counterFreeToReuseTimeoutNs = counterFreeToReuseTimeoutNs;
            return this;
        }

        /**
         * Timeout in nanoseconds after which a publication will be unblocked if a offer is partially complete to allow
         * other publishers to make progress.
         *
         * @return timeout in nanoseconds after which a publication will be unblocked.
         * @see Configuration#PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME
         */
        public long publicationUnblockTimeoutNs()
        {
            return publicationUnblockTimeoutNs;
        }

        /**
         * Timeout in nanoseconds after which a publication will be unblocked if a offer is partially complete to allow
         * other publishers to make progress.
         *
         * @param timeoutNs in nanoseconds after which a publication will be unblocked.
         * @return this for a fluent API.
         * @see Configuration#PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME
         */
        public Context publicationUnblockTimeoutNs(final long timeoutNs)
        {
            this.publicationUnblockTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Timeout in nanoseconds after which a publication will be considered not connected if no status messages are
         * received.
         *
         * @return timeout in nanoseconds after which a publication is considered not connected.
         * @see Configuration#PUBLICATION_CONNECTION_TIMEOUT_PROP_NAME
         */
        public long publicationConnectionTimeoutNs()
        {
            return publicationConnectionTimeoutNs;
        }

        /**
         * Timeout in nanoseconds after which a publication will be considered not connected if no status messages are
         * received.
         *
         * @param timeoutNs in nanoseconds after which a publication will be considered not connected.
         * @return this for a fluent API.
         * @see Configuration#PUBLICATION_CONNECTION_TIMEOUT_PROP_NAME
         */
        public Context publicationConnectionTimeoutNs(final long timeoutNs)
        {
            this.publicationConnectionTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Does a spy subscription simulate a connection to a network publication.
         *
         * @return true if a spy subscription should simulate a connection to a network publication.
         * @see Configuration#SPIES_SIMULATE_CONNECTION_PROP_NAME
         */
        public boolean spiesSimulateConnection()
        {
            return spiesSimulateConnection;
        }

        /**
         * Does a spy subscription simulate a connection to a network publication.
         *
         * @param spiesSimulateConnection true if a spy subscription simulates a connection to a network publication.
         * @return this for a fluent API.
         * @see Configuration#SPIES_SIMULATE_CONNECTION_PROP_NAME
         */
        public Context spiesSimulateConnection(final boolean spiesSimulateConnection)
        {
            this.spiesSimulateConnection = spiesSimulateConnection;
            return this;
        }

        /**
         * Does a stream NAK when loss is detected, reliable=true, or gap fill, reliable=false.
         * <p>
         * The default can be overridden with a channel param.
         *
         * @return true if NAK on loss to be reliable otherwise false for gap fill.
         * @see Configuration#RELIABLE_STREAM_PROP_NAME
         * @see CommonContext#RELIABLE_STREAM_PARAM_NAME
         */
        public boolean reliableStream()
        {
            return reliableStream;
        }

        /**
         * Does a stream NAK when loss is detected, reliable=true, or gap fill, reliable=false.
         * <p>
         * The default can be overridden with a channel param.
         *
         * @param reliableStream true if a stream should NAK on loss otherwise gap fill.
         * @return this for a fluent API.
         * @see Configuration#RELIABLE_STREAM_PROP_NAME
         * @see CommonContext#RELIABLE_STREAM_PARAM_NAME
         */
        public Context reliableStream(final boolean reliableStream)
        {
            this.reliableStream = reliableStream;
            return this;
        }

        /**
         * Do subscriptions have a tether so they participates in local flow control when more than one.
         * <p>
         * The default can be overridden with a channel param.
         *
         * @return true if subscriptions should have a tether for local flow control.
         * @see Configuration#TETHER_SUBSCRIPTIONS_PROP_NAME
         * @see CommonContext#TETHER_PARAM_NAME
         */
        public boolean tetherSubscriptions()
        {
            return tetherSubscriptions;
        }

        /**
         * Do subscriptions have a tether so they participates in local flow control when more than one.
         * <p>
         * The default can be overridden with a channel param.
         *
         * @param tetherSubscription true if subscriptions should have a tether for local flow control.
         * @return this for a fluent API.
         * @see Configuration#TETHER_SUBSCRIPTIONS_PROP_NAME
         * @see CommonContext#TETHER_PARAM_NAME
         */
        public Context tetherSubscriptions(final boolean tetherSubscription)
        {
            this.tetherSubscriptions = tetherSubscription;
            return this;
        }

        /**
         * Default length for a term buffer on a network publication.
         *
         * @return default length for a term buffer on a network publication.
         * @see Configuration#TERM_BUFFER_LENGTH_PROP_NAME
         */
        public int publicationTermBufferLength()
        {
            return publicationTermBufferLength;
        }

        /**
         * Default length for a term buffer on a network publication.
         * <p>
         * This can be overridden on publication by using channel URI params.
         *
         * @param termBufferLength default length for a term buffer on a network publication.
         * @return this for a fluent API.
         * @see Configuration#TERM_BUFFER_LENGTH_PROP_NAME
         */
        public Context publicationTermBufferLength(final int termBufferLength)
        {
            this.publicationTermBufferLength = termBufferLength;
            return this;
        }

        /**
         * Default length for a term buffer on a IPC publication.
         *
         * @return default length for a term buffer on a IPC publication.
         * @see Configuration#IPC_TERM_BUFFER_LENGTH_PROP_NAME
         */
        public int ipcTermBufferLength()
        {
            return ipcTermBufferLength;
        }

        /**
         * Default length for a term buffer on a IPC publication.
         * <p>
         * This can be overridden on publication by using channel URI params.
         *
         * @param termBufferLength default length for a term buffer on a IPC publication.
         * @return this for a fluent API.
         * @see Configuration#IPC_TERM_BUFFER_LENGTH_PROP_NAME
         */
        public Context ipcTermBufferLength(final int termBufferLength)
        {
            this.ipcTermBufferLength = termBufferLength;
            return this;
        }

        /**
         * Default length for a term buffer window on a network publication.
         *
         * @return default length for a term buffer window on a network publication.
         * @see Configuration#PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME
         */
        public int publicationTermWindowLength()
        {
            return publicationTermWindowLength;
        }

        /**
         * Default length for a term buffer window on a network publication.
         *
         * @param termWindowLength default length for a term buffer window on a network publication.
         * @return this for a fluent API.
         * @see Configuration#PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME
         */
        public Context publicationTermWindowLength(final int termWindowLength)
        {
            this.publicationTermWindowLength = termWindowLength;
            return this;
        }

        /**
         * Default length for a term buffer window on a IPC publication.
         *
         * @return default length for a term buffer window on a IPC publication.
         * @see Configuration#IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME
         */
        public int ipcPublicationTermWindowLength()
        {
            return ipcPublicationTermWindowLength;
        }

        /**
         * Default length for a term buffer window on a IPC publication.
         *
         * @param termWindowLength default length for a term buffer window on a IPC publication.
         * @return this for a fluent API.
         * @see Configuration#IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME
         */
        public Context ipcPublicationTermWindowLength(final int termWindowLength)
        {
            this.ipcPublicationTermWindowLength = termWindowLength;
            return this;
        }

        /**
         * The initial window for in flight data on a connection which must be less than
         * {@link Configuration#SOCKET_RCVBUF_LENGTH_PROP_NAME}. This needs to be configured for throughput respecting
         * BDP.
         *
         * @return The initial window for in flight data on a connection
         * @see Configuration#INITIAL_WINDOW_LENGTH_PROP_NAME
         */
        public int initialWindowLength()
        {
            return initialWindowLength;
        }

        /**
         * The initial window for in flight data on a connection which must be less than
         * {@link Configuration#SOCKET_RCVBUF_LENGTH_PROP_NAME}. This needs to be configured for throughput respecting
         * BDP.
         *
         * @param initialWindowLength The initial window for in flight data on a connection
         * @return this for a fluent API.
         * @see Configuration#INITIAL_WINDOW_LENGTH_PROP_NAME
         */
        public Context initialWindowLength(final int initialWindowLength)
        {
            this.initialWindowLength = initialWindowLength;
            return this;
        }

        /**
         * The socket send buffer length which is the OS SO_SNDBUF.
         *
         * @return the socket send buffer length.
         * @see Configuration#SOCKET_SNDBUF_LENGTH_PROP_NAME
         */
        public int socketSndbufLength()
        {
            return socketSndbufLength;
        }

        /**
         * The socket send buffer length which is the OS SO_SNDBUF.
         *
         * @param socketSndbufLength which is the OS SO_SNDBUF.
         * @return this for a fluent API.
         * @see Configuration#SOCKET_SNDBUF_LENGTH_PROP_NAME
         */
        public Context socketSndbufLength(final int socketSndbufLength)
        {
            this.socketSndbufLength = socketSndbufLength;
            return this;
        }

        /**
         * The socket send buffer length which is the OS SO_RCVBUF.
         *
         * @return the socket send buffer length.
         * @see Configuration#SOCKET_RCVBUF_LENGTH_PROP_NAME
         */
        public int socketRcvbufLength()
        {
            return socketRcvbufLength;
        }

        /**
         * The socket send buffer length which is the OS SO_RCVBUF.
         *
         * @param socketRcvbufLength which is the OS SO_RCVBUF.
         * @return this for a fluent API.
         * @see Configuration#SOCKET_RCVBUF_LENGTH_PROP_NAME
         */
        public Context socketRcvbufLength(final int socketRcvbufLength)
        {
            this.socketRcvbufLength = socketRcvbufLength;
            return this;
        }

        /**
         * The TTL value to be used for multicast sockets.
         *
         * @return TTL value to be used for multicast sockets.
         * @see Configuration#SOCKET_MULTICAST_TTL_PROP_NAME
         */
        public int socketMulticastTtl()
        {
            return socketMulticastTtl;
        }

        /**
         * TTL value to be used for multicast sockets.
         *
         * @param ttl value to be used for multicast sockets.
         * @return this for a fluent API.
         * @see Configuration#SOCKET_MULTICAST_TTL_PROP_NAME
         */
        public Context socketMulticastTtl(final int ttl)
        {
            socketMulticastTtl = ttl;
            return this;
        }

        /**
         * MTU in bytes for datagrams sent to the network. Messages larger than this are fragmented.
         * <p>
         * Larger MTUs reduce system call overhead at the expense of possible increase in loss which
         * will need to be recovered. If this is greater than the network MTU for UDP then the packet will be
         * fragmented and can amplify the impact of loss.
         *
         * @return MTU in bytes for datagrams sent to the network.
         * @see Configuration#MTU_LENGTH_PROP_NAME
         */
        public int mtuLength()
        {
            return mtuLength;
        }

        /**
         * MTU in bytes for datagrams sent to the network. Messages larger than this are fragmented.
         * <p>
         * Larger MTUs reduce system call overhead at the expense of possible increase in loss which
         * will need to be recovered. If this is greater than the network MTU for UDP then the packet will be
         * fragmented and can amplify the impact of loss.
         *
         * @param mtuLength in bytes for datagrams sent to the network.
         * @return this for a fluent API.
         * @see Configuration#MTU_LENGTH_PROP_NAME
         */
        public Context mtuLength(final int mtuLength)
        {
            this.mtuLength = mtuLength;
            return this;
        }

        /**
         * MTU in bytes for datagrams sent over shared memory. Messages larger than this are fragmented.
         * <p>
         * Larger MTUs reduce fragmentation. If an IPC stream is recorded to be later sent over the network then
         * then a large MTU may be an issue.
         *
         * @return MTU in bytes for message fragments.
         * @see Configuration#IPC_MTU_LENGTH_PROP_NAME
         */
        public int ipcMtuLength()
        {
            return ipcMtuLength;
        }

        /**
         * MTU in bytes for datagrams sent over shared memory. Messages larger than this are fragmented.
         * <p>
         * Larger MTUs reduce fragmentation. If an IPC stream is recorded to be later sent over the network then
         * then a large MTU may be an issue.
         *
         * @param ipcMtuLength in bytes for message fragments.
         * @return this for a fluent API.
         * @see Configuration#IPC_MTU_LENGTH_PROP_NAME
         */
        public Context ipcMtuLength(final int ipcMtuLength)
        {
            this.ipcMtuLength = ipcMtuLength;
            return this;
        }

        /**
         * The {@link EpochClock} as a source of time in milliseconds for wall clock time.
         *
         * @return the {@link EpochClock} as a source of time in milliseconds for wall clock time.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * The {@link EpochClock} as a source of time in milliseconds for wall clock time.
         *
         * @param clock to be used.
         * @return this for a fluent API.
         */
        public Context epochClock(final EpochClock clock)
        {
            epochClock = clock;
            return this;
        }

        /**
         * The {@link NanoClock} as a source of time in nanoseconds for measuring duration.
         *
         * @return the {@link NanoClock} as a source of time in nanoseconds for measuring duration.
         */
        public NanoClock nanoClock()
        {
            return nanoClock;
        }

        /**
         * The {@link NanoClock} as a source of time in nanoseconds for measuring duration.
         *
         * @param clock to be used.
         * @return this for a fluent API.
         */
        public Context nanoClock(final NanoClock clock)
        {
            nanoClock = clock;
            return this;
        }

        /**
         * The {@link CachedEpochClock} as a source of time in milliseconds for wall clock time.
         *
         * @return the {@link CachedEpochClock} as a source of time in milliseconds for wall clock time.
         */
        public CachedEpochClock cachedEpochClock()
        {
            return cachedEpochClock;
        }

        /**
         * The {@link CachedEpochClock} as a source of time in milliseconds for wall clock time.
         *
         * @param clock to be used.
         * @return this for a fluent API.
         */
        public Context cachedEpochClock(final CachedEpochClock clock)
        {
            cachedEpochClock = clock;
            return this;
        }

        /**
         * The {@link CachedNanoClock} as a source of time in nanoseconds for measuring duration. This is updated
         * once per duty cycle of the {@link DriverConductor}.
         *
         * @return the {@link CachedNanoClock} as a source of time in nanoseconds for measuring duration.
         */
        public CachedNanoClock cachedNanoClock()
        {
            return cachedNanoClock;
        }

        /**
         * The {@link CachedNanoClock} as a source of time in nanoseconds for measuring duration.
         *
         * @param clock to be used.
         * @return this for a fluent API.
         */
        public Context cachedNanoClock(final CachedNanoClock clock)
        {
            cachedNanoClock = clock;
            return this;
        }

        /**
         * {@link ThreadingMode} that should be used for the driver.
         *
         * @return {@link ThreadingMode} that should be used for the driver.
         * @see Configuration#THREADING_MODE_PROP_NAME
         */
        public ThreadingMode threadingMode()
        {
            return threadingMode;
        }

        /**
         * {@link ThreadingMode} that should be used for the driver.
         *
         * @param threadingMode that should be used for the driver.
         * @return this for a fluent API.
         * @see Configuration#THREADING_MODE_PROP_NAME
         */
        public Context threadingMode(final ThreadingMode threadingMode)
        {
            this.threadingMode = threadingMode;
            return this;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the {@link Sender} when running in
         * {@link ThreadingMode#DEDICATED}.
         *
         * @return {@link ThreadFactory} to be used for creating agent thread for the {@link Sender}.
         */
        public ThreadFactory senderThreadFactory()
        {
            return senderThreadFactory;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the {@link Sender} when running in
         * {@link ThreadingMode#DEDICATED}.
         * <p>
         * If none is provided then this will default a simple new operation.
         *
         * @param factory to be used for creating agent thread for the {@link Sender}.
         * @return this for a fluent API.
         */
        public Context senderThreadFactory(final ThreadFactory factory)
        {
            senderThreadFactory = factory;
            return this;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the {@link Receiver} when running in
         * {@link ThreadingMode#DEDICATED}.
         *
         * @return {@link ThreadFactory} to be used for creating agent thread for the {@link Receiver}.
         */
        public ThreadFactory receiverThreadFactory()
        {
            return receiverThreadFactory;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the {@link Receiver} when running in
         * {@link ThreadingMode#DEDICATED}.
         * <p>
         * If none is provided then this will default a simple new operation.
         *
         * @param factory to be used for creating agent thread for the {@link Receiver}.
         * @return this for a fluent API.
         */
        public Context receiverThreadFactory(final ThreadFactory factory)
        {
            receiverThreadFactory = factory;
            return this;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the {@link DriverConductor} when running in
         * {@link ThreadingMode#DEDICATED} or {@link ThreadingMode#SHARED_NETWORK}.
         *
         * @return {@link ThreadFactory} to be used for creating agent thread for the {@link DriverConductor}.
         */
        public ThreadFactory conductorThreadFactory()
        {
            return conductorThreadFactory;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the {@link DriverConductor} when running in
         * {@link ThreadingMode#DEDICATED} or {@link ThreadingMode#SHARED_NETWORK}.
         * <p>
         * If none is provided then this will default a simple new operation.
         *
         * @param factory to be used for creating agent thread for the {@link DriverConductor}.
         * @return this for a fluent API.
         */
        public Context conductorThreadFactory(final ThreadFactory factory)
        {
            conductorThreadFactory = factory;
            return this;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the all driver agents as a
         * {@link CompositeAgent} when running in {@link ThreadingMode#SHARED}.
         *
         * @return {@link ThreadFactory} to be used for creating agent thread for the {@link CompositeAgent}.
         */
        public ThreadFactory sharedThreadFactory()
        {
            return sharedThreadFactory;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the all driver agents as a
         * {@link CompositeAgent} when running in {@link ThreadingMode#SHARED}.
         * <p>
         * If none is provided then this will default a simple new operation.
         *
         * @param factory to be used for creating agent thread for the {@link CompositeAgent}.
         * @return this for a fluent API.
         */
        public Context sharedThreadFactory(final ThreadFactory factory)
        {
            sharedThreadFactory = factory;
            return this;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the sender and receiver agents as a
         * {@link CompositeAgent} when running in {@link ThreadingMode#SHARED_NETWORK}.
         *
         * @return {@link ThreadFactory} to be used for creating agent thread for the {@link CompositeAgent}.
         */
        public ThreadFactory sharedNetworkThreadFactory()
        {
            return sharedNetworkThreadFactory;
        }

        /**
         * {@link ThreadFactory} to be used for creating agent thread for the sender and receiver agents as a
         * {@link CompositeAgent} when running in {@link ThreadingMode#SHARED_NETWORK}.
         * <p>
         * If none is provided then this will default a simple new operation.
         *
         * @param factory to be used for creating agent thread for the {@link CompositeAgent}.
         * @return this for a fluent API.
         */
        public Context sharedNetworkThreadFactory(final ThreadFactory factory)
        {
            sharedNetworkThreadFactory = factory;
            return this;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link Sender} when in {@link ThreadingMode#DEDICATED}.
         *
         * @return {@link IdleStrategy} to be used by the {@link Sender} when in {@link ThreadingMode#DEDICATED}.
         * @see Configuration#SENDER_IDLE_STRATEGY_PROP_NAME
         */
        public IdleStrategy senderIdleStrategy()
        {
            return senderIdleStrategy;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link Sender} when in {@link ThreadingMode#DEDICATED}.
         *
         * @param strategy to be used by the {@link Sender} when in {@link ThreadingMode#DEDICATED}.
         * @return this for a fluent API.
         * @see Configuration#SENDER_IDLE_STRATEGY_PROP_NAME
         */
        public Context senderIdleStrategy(final IdleStrategy strategy)
        {
            senderIdleStrategy = strategy;
            return this;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link Receiver} when in {@link ThreadingMode#DEDICATED}.
         *
         * @return {@link IdleStrategy} used by the {@link Receiver} when in {@link ThreadingMode#DEDICATED}.
         * @see Configuration#RECEIVER_IDLE_STRATEGY_PROP_NAME
         */
        public IdleStrategy receiverIdleStrategy()
        {
            return receiverIdleStrategy;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link Receiver} when in {@link ThreadingMode#DEDICATED}.
         *
         * @param strategy to be used by the {@link Receiver} when in {@link ThreadingMode#DEDICATED}.
         * @return this for a fluent API.
         * @see Configuration#RECEIVER_IDLE_STRATEGY_PROP_NAME
         */
        public Context receiverIdleStrategy(final IdleStrategy strategy)
        {
            receiverIdleStrategy = strategy;
            return this;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link DriverConductor} when in {@link ThreadingMode#DEDICATED}
         * or {@link ThreadingMode#SHARED_NETWORK}.
         *
         * @return {@link IdleStrategy} used by the {@link DriverConductor}
         * @see Configuration#CONDUCTOR_IDLE_STRATEGY_PROP_NAME
         */
        public IdleStrategy conductorIdleStrategy()
        {
            return conductorIdleStrategy;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link DriverConductor} when in {@link ThreadingMode#DEDICATED}
         * or {@link ThreadingMode#SHARED_NETWORK}.
         *
         * @param strategy to be used by the {@link DriverConductor}.
         * @return this for a fluent API.
         * @see Configuration#CONDUCTOR_IDLE_STRATEGY_PROP_NAME
         */
        public Context conductorIdleStrategy(final IdleStrategy strategy)
        {
            conductorIdleStrategy = strategy;
            return this;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link Sender} and {@link Receiver} agents when in
         * {@link ThreadingMode#SHARED_NETWORK}.
         *
         * @return {@link IdleStrategy} used by the {@link Sender} and {@link Receiver}.
         * @see Configuration#SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME
         */
        public IdleStrategy sharedNetworkIdleStrategy()
        {
            return sharedNetworkIdleStrategy;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link Sender} and {@link Receiver} agents when in
         * {@link ThreadingMode#SHARED_NETWORK}.
         *
         * @param strategy to be used by the {@link Sender} and {@link Receiver}.
         * @return this for a fluent API.
         * @see Configuration#SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME
         */
        public Context sharedNetworkIdleStrategy(final IdleStrategy strategy)
        {
            sharedNetworkIdleStrategy = strategy;
            return this;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link Sender}, {@link Receiver} and {@link DriverConductor}
         * agents when in {@link ThreadingMode#SHARED}.
         *
         * @return {@link IdleStrategy} used by the {@link Sender}, {@link Receiver} and {@link DriverConductor}.
         * @see Configuration#SHARED_IDLE_STRATEGY_PROP_NAME
         */
        public IdleStrategy sharedIdleStrategy()
        {
            return sharedIdleStrategy;
        }

        /**
         * {@link IdleStrategy} to be used by the {@link Sender}, {@link Receiver} and {@link DriverConductor}
         * agents when in {@link ThreadingMode#SHARED}.
         *
         * @param strategy to be used by the {@link Sender}, {@link Receiver} and {@link DriverConductor}.
         * @return this for a fluent API.
         * @see Configuration#SHARED_IDLE_STRATEGY_PROP_NAME
         */
        public Context sharedIdleStrategy(final IdleStrategy strategy)
        {
            sharedIdleStrategy = strategy;
            return this;
        }

        /**
         * Supplier of dynamically created {@link SendChannelEndpoint} subclasses for specialising interactions
         * with the send side of a network channel.
         *
         * @return the supplier of dynamically created {@link SendChannelEndpoint} subclasses.
         * @see Configuration#SEND_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME
         */
        public SendChannelEndpointSupplier sendChannelEndpointSupplier()
        {
            return sendChannelEndpointSupplier;
        }

        /**
         * Supplier of dynamically created {@link SendChannelEndpoint} subclasses for specialising interactions
         * with the send side of a network channel.
         *
         * @param supplier of dynamically created {@link SendChannelEndpoint} subclasses.
         * @return this for a fluent API.
         * @see Configuration#SEND_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME
         */
        public Context sendChannelEndpointSupplier(final SendChannelEndpointSupplier supplier)
        {
            sendChannelEndpointSupplier = supplier;
            return this;
        }

        /**
         * Supplier of dynamically created {@link ReceiveChannelEndpoint} subclasses for specialising interactions
         * with the receive side of a network channel.
         *
         * @return the supplier of dynamically created {@link ReceiveChannelEndpoint} subclasses.
         * @see Configuration#RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME
         */
        public ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier()
        {
            return receiveChannelEndpointSupplier;
        }

        /**
         * Supplier of dynamically created {@link ReceiveChannelEndpoint} subclasses for specialising interactions
         * with the receive side of a network channel.
         *
         * @param supplier of dynamically created {@link ReceiveChannelEndpoint} subclasses.
         * @return this for a fluent API.
         * @see Configuration#RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME
         */
        public Context receiveChannelEndpointSupplier(final ReceiveChannelEndpointSupplier supplier)
        {
            receiveChannelEndpointSupplier = supplier;
            return this;
        }

        /**
         * The thread local buffers and associated objects for use by subclasses of {@link ReceiveChannelEndpoint}.
         *
         * @return thread local buffers and associated objects for use by subclasses of {@link ReceiveChannelEndpoint}.
         */
        public ReceiveChannelEndpointThreadLocals receiveChannelEndpointThreadLocals()
        {
            return receiveChannelEndpointThreadLocals;
        }

        /**
         * The thread local buffers and associated objects for use by subclasses of {@link ReceiveChannelEndpoint}.
         *
         * @param threadLocals for use by subclasses of {@link ReceiveChannelEndpoint}.
         * @return this for a fluent API.
         */
        public Context receiveChannelEndpointThreadLocals(final ReceiveChannelEndpointThreadLocals threadLocals)
        {
            receiveChannelEndpointThreadLocals = threadLocals;
            return this;
        }

        /**
         * The temporary buffer than can be used to build up counter labels to avoid allocation.
         *
         * @return the temporary buffer than can be used to build up counter labels to avoid allocation.
         */
        public MutableDirectBuffer tempBuffer()
        {
            return tempBuffer;
        }

        /**
         * Set the temporary buffer than can be used to build up counter labels to avoid allocation.
         *
         * @param tempBuffer to be used to avoid allocation.
         * @return the temporary buffer than can be used to build up counter labels to avoid allocation.
         */
        public Context tempBuffer(final MutableDirectBuffer tempBuffer)
        {
            this.tempBuffer = tempBuffer;
            return this;
        }

        /**
         * Supplier of dynamically created {@link FlowControl} strategies for unicast connections.
         *
         * @return supplier of dynamically created {@link FlowControl} strategies for unicast connections.
         * @see Configuration#UNICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME
         */
        public FlowControlSupplier unicastFlowControlSupplier()
        {
            return unicastFlowControlSupplier;
        }

        /**
         * Supplier of dynamically created {@link FlowControl} strategies for unicast connections.
         *
         * @param flowControlSupplier of {@link FlowControl} strategies for unicast connections.
         * @return this for a fluent API.
         * @see Configuration#UNICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME
         */
        public Context unicastFlowControlSupplier(final FlowControlSupplier flowControlSupplier)
        {
            unicastFlowControlSupplier = flowControlSupplier;
            return this;
        }

        /**
         * Supplier of dynamically created {@link FlowControl} strategies for multicast connections.
         *
         * @return supplier of dynamically created {@link FlowControl} strategies for multicast connections.
         * @see Configuration#MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME
         */
        public FlowControlSupplier multicastFlowControlSupplier()
        {
            return multicastFlowControlSupplier;
        }

        /**
         * Supplier of dynamically created {@link FlowControl} strategies for multicast connections.
         *
         * @param flowControlSupplier of {@link FlowControl} strategies for multicast connections.
         * @return this for a fluent API.
         * @see Configuration#MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME
         */
        public Context multicastFlowControlSupplier(final FlowControlSupplier flowControlSupplier)
        {
            multicastFlowControlSupplier = flowControlSupplier;
            return this;
        }

        /**
         * Application specific feedback used to identify a receiver group when using a
         * {@link PreferredMulticastFlowControl} strategy which is added to Status Messages (SMs)..
         *
         * @return Application specific feedback used to identify receiver group for flow control.
         * @see Configuration#SM_APPLICATION_SPECIFIC_FEEDBACK_PROP_NAME
         */
        public byte[] applicationSpecificFeedback()
        {
            return applicationSpecificFeedback;
        }

        /**
         * Application specific feedback used to identify a receiver group when using a
         * {@link PreferredMulticastFlowControl} strategy which is added to Status Messages (SMs).
         *
         * @param asfBytes for identifying a receiver group.
         * @return this for a fluent API.
         * @see Configuration#SM_APPLICATION_SPECIFIC_FEEDBACK_PROP_NAME
         */
        public Context applicationSpecificFeedback(final byte[] asfBytes)
        {
            this.applicationSpecificFeedback = asfBytes;
            return this;
        }

        /**
         * Supplier of dynamically created {@link CongestionControl} strategies for individual connections.
         *
         * @return supplier of dynamically created {@link CongestionControl} strategies for individual connections.
         * @see Configuration#CONGESTION_CONTROL_STRATEGY_SUPPLIER_PROP_NAME
         */
        public CongestionControlSupplier congestionControlSupplier()
        {
            return congestionControlSupplier;
        }

        /**
         * Supplier of dynamically created {@link CongestionControl} strategies for individual connections.
         *
         * @param supplier of dynamically created {@link CongestionControl} strategies for individual connections.
         * @return this for a fluent API.
         * @see Configuration#CONGESTION_CONTROL_STRATEGY_SUPPLIER_PROP_NAME
         */
        public Context congestControlSupplier(final CongestionControlSupplier supplier)
        {
            this.congestionControlSupplier = supplier;
            return this;
        }

        /**
         * {@link ErrorHandler} to be used for reporting errors during {@link Agent}s operations.
         *
         * @return the {@link ErrorHandler} to be used for reporting errors during {@link Agent}s operations.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * {@link ErrorHandler} to be used for reporting errors during {@link Agent}s operations.
         * <p>
         * The default {@link ErrorHandler} will delegate to the {@link #errorLog()} and output to {@link System#err}
         * if the log is full.
         * <p>
         * <b>Note:</b> {@link ErrorHandler} should be thread safe.
         *
         * @param errorHandler to be used for reporting errors during {@link Agent}s operations.
         * @return this for a fluent API.
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Log to which exceptions are recorded.
         *
         * @return log to which exceptions are recorded.
         */
        public DistinctErrorLog errorLog()
        {
            return errorLog;
        }

        /**
         * Log to which exceptions are recorded.
         *
         * @param errorLog to which exceptions are recorded.
         * @return this for a fluent API.
         */
        public Context errorLog(final DistinctErrorLog errorLog)
        {
            this.errorLog = errorLog;
            return this;
        }

        /**
         * Should a {@link ConcurrentCountersManager} be used to allow for cross thread usage.
         *
         * @return true if a {@link ConcurrentCountersManager} should be used otherwise false.
         */
        public boolean useConcurrentCountersManager()
        {
            return useConcurrentCountersManager;
        }

        /**
         * Should a {@link ConcurrentCountersManager} be used to allow for cross thread usage.
         * <p>
         * The default is to use a normal {@link CountersManager} from only the {@link DriverConductor}. If the
         * {@link MediaDriver} is to be composed into another services that allocates counters then this should be
         * concurrent.
         *
         * @param useConcurrentCountersManager true if a {@link ConcurrentCountersManager} should be used.
         * @return this for a fluent API.
         */
        public Context useConcurrentCountersManager(final boolean useConcurrentCountersManager)
        {
            this.useConcurrentCountersManager = useConcurrentCountersManager;
            return this;
        }

        /**
         * Get the {@link CountersManager} that has been concluded for this context.
         *
         * @return the {@link CountersManager} that has been concluded for this context.
         */
        public CountersManager countersManager()
        {
            return countersManager;
        }

        /**
         * Set the {@link CountersManager} to override the one that would have been concluded.
         *
         * @param countersManager to override the one that would have been concluded.
         * @return this for a fluent API.
         */
        public Context countersManager(final CountersManager countersManager)
        {
            this.countersManager = countersManager;
            return this;
        }

        /**
         * The {@link SystemCounters} for the driver for recording aggregate events of system status.
         *
         * @return the {@link SystemCounters} for the driver for recording aggregate events of system status.
         */
        public SystemCounters systemCounters()
        {
            return systemCounters;
        }

        /**
         * The {@link SystemCounters} for the driver for recording aggregate events of system status.
         * <p>
         * The default should only be overridden for testing.
         *
         * @param systemCounters for the driver for recording aggregate events of system status.
         * @return this for a fluent API.
         */
        public Context systemCounters(final SystemCounters systemCounters)
        {
            this.systemCounters = systemCounters;
            return this;
        }

        /**
         * {@link LossReport}for identifying loss issues on specific connections.
         *
         * @return {@link LossReport} for identifying loss issues on specific connections.
         */
        public LossReport lossReport()
        {
            return lossReport;
        }

        /**
         * {@link LossReport}for identifying loss issues on specific connections.
         * <p>
         * The default should only be overridden for testing.
         *
         * @param lossReport for identifying loss issues on specific connections.
         * @return this for a fluent API.
         */
        public Context lossReport(final LossReport lossReport)
        {
            this.lossReport = lossReport;
            return this;
        }

        /**
         * Low end of the publication reserved session id range which will not be automatically assigned.
         *
         * @return low end of the publication reserved session id range which will not be automatically assigned.
         * @see #publicationReservedSessionIdHigh()
         * @see Configuration#PUBLICATION_RESERVED_SESSION_ID_LOW_PROP_NAME
         */
        public int publicationReservedSessionIdLow()
        {
            return publicationReservedSessionIdLow;
        }

        /**
         * Low end of the publication reserved session id range which will not be automatically assigned.
         *
         * @param sessionId for low end of the publication reserved session id range which will not be automatically
         *                 assigned.
         * @return this for fluent API.
         * @see #publicationReservedSessionIdHigh(int)
         * @see Configuration#PUBLICATION_RESERVED_SESSION_ID_LOW_PROP_NAME
         */
        public Context publicationReservedSessionIdLow(final int sessionId)
        {
            publicationReservedSessionIdLow = sessionId;
            return this;
        }

        /**
         * High end of the publication reserved session id range which will not be automatically assigned.
         *
         * @return high end of the publication reserved session id range which will not be automatically assigned.
         * @see #publicationReservedSessionIdLow()
         * @see Configuration#PUBLICATION_RESERVED_SESSION_ID_HIGH_PROP_NAME
         */
        public int publicationReservedSessionIdHigh()
        {
            return publicationReservedSessionIdHigh;
        }

        /**
         * High end of the publication reserved session id range which will not be automatically assigned.
         *
         * @param sessionId for high end of the publication reserved session id range which will not be automatically
         *                 assigned.
         * @return this for fluent API.
         * @see #publicationReservedSessionIdLow(int)
         * @see Configuration#PUBLICATION_RESERVED_SESSION_ID_HIGH_PROP_NAME
         */
        public Context publicationReservedSessionIdHigh(final int sessionId)
        {
            publicationReservedSessionIdHigh = sessionId;
            return this;
        }

        /**
         * {@link FeedbackDelayGenerator} for controlling the delay before sending a retransmit.
         *
         * @return {@link FeedbackDelayGenerator} for controlling the delay before sending a retransmit.
         * @see Configuration#RETRANSMIT_UNICAST_DELAY_PROP_NAME
         */
        public FeedbackDelayGenerator retransmitUnicastDelayGenerator()
        {
            return retransmitUnicastDelayGenerator;
        }

        /**
         * Set the {@link FeedbackDelayGenerator} for controlling the delay before sending a retransmit.
         *
         * @param feedbackDelayGenerator for controlling the delay before sending a retransmit.
         * @return this for a fluent API
         * @see Configuration#RETRANSMIT_UNICAST_DELAY_PROP_NAME
         */
        public Context retransmitUnicastDelayGenerator(final FeedbackDelayGenerator feedbackDelayGenerator)
        {
            retransmitUnicastDelayGenerator = feedbackDelayGenerator;
            return this;
        }

        /**
         * {@link FeedbackDelayGenerator} for controlling the linger after a retransmit.
         *
         * @return {@link FeedbackDelayGenerator} for controlling the linger after a retransmit.
         * @see Configuration#RETRANSMIT_UNICAST_LINGER_PROP_NAME
         */
        public FeedbackDelayGenerator retransmitUnicastLingerGenerator()
        {
            return retransmitUnicastLingerGenerator;
        }

        /**
         * Set the {@link FeedbackDelayGenerator} for controlling the linger after a retransmit.
         *
         * @param feedbackDelayGenerator for controlling the linger after a retransmit.
         * @return this for a fluent API
         * @see Configuration#RETRANSMIT_UNICAST_LINGER_PROP_NAME
         */
        public Context retransmitUnicastLingerGenerator(final FeedbackDelayGenerator feedbackDelayGenerator)
        {
            retransmitUnicastLingerGenerator = feedbackDelayGenerator;
            return this;
        }

        /**
         * {@link FeedbackDelayGenerator} for controlling the delay of sending NAK feedback on unicast.
         *
         * @return {@link FeedbackDelayGenerator} for controlling the delay of sending NAK feedback.
         * @see Configuration#NAK_UNICAST_DELAY_PROP_NAME
         */
        public FeedbackDelayGenerator unicastFeedbackDelayGenerator()
        {
            return unicastFeedbackDelayGenerator;
        }

        /**
         * Set the {@link FeedbackDelayGenerator} for controlling the delay of sending NAK feedback on unicast.
         *
         * @param feedbackDelayGenerator  for controlling the delay of sending NAK feedback.
         * @return this for a fluent API
         * @see Configuration#NAK_UNICAST_DELAY_PROP_NAME
         */
        public Context unicastFeedbackDelayGenerator(final FeedbackDelayGenerator feedbackDelayGenerator)
        {
            unicastFeedbackDelayGenerator = feedbackDelayGenerator;
            return this;
        }

        /**
         * {@link FeedbackDelayGenerator} for controlling the delay of sending NAK feedback on multicast.
         *
         * @return {@link FeedbackDelayGenerator} for controlling the delay of sending NAK feedback.
         * @see Configuration#NAK_MULTICAST_MAX_BACKOFF_PROP_NAME
         * @see Configuration#NAK_MULTICAST_GROUP_SIZE_PROP_NAME
         */
        public FeedbackDelayGenerator multicastFeedbackDelayGenerator()
        {
            return multicastFeedbackDelayGenerator;
        }

        /**
         * Set the {@link FeedbackDelayGenerator} for controlling the delay of sending NAK feedback on multicast.
         *
         * @param feedbackDelayGenerator  for controlling the delay of sending NAK feedback.
         * @return this for a fluent API
         * @see Configuration#NAK_MULTICAST_MAX_BACKOFF_PROP_NAME
         * @see Configuration#NAK_MULTICAST_GROUP_SIZE_PROP_NAME
         */
        public Context multicastFeedbackDelayGenerator(final FeedbackDelayGenerator feedbackDelayGenerator)
        {
            multicastFeedbackDelayGenerator = feedbackDelayGenerator;
            return this;
        }

        /**
         * Set the {@link Runnable} that is called when the {@link MediaDriver} processes a valid termination request.
         *
         * @param terminationHook that can be used to terminate a driver.
         * @return this for a fluent API.
         */
        public Context terminationHook(final Runnable terminationHook)
        {
            this.terminationHook = terminationHook;
            return this;
        }

        /**
         * Get the {@link Runnable} that is called when the {@link MediaDriver} processes a valid termination request.
         * <p>
         * The default action is nothing.
         *
         * @return the {@link Runnable} that can be used to terminate a consensus module.
         */
        public Runnable terminationHook()
        {
            return terminationHook;
        }

        /**
         * Set the {@link TerminationValidator} to be used to validate termination requests.
         *
         * @param validator to validate termination requests.
         * @return this for a fluent API.
         */
        public Context terminationValidator(final TerminationValidator validator)
        {
            this.terminationValidator = validator;
            return this;
        }

        /**
         * Get the {@link TerminationValidator} to be used to validate termination requests.
         *
         * @return {@link TerminationValidator} to validate termination requests.
         */
        public TerminationValidator terminationValidator()
        {
            return terminationValidator;
        }

        /**
         * Get the ratio for sending data to polling status messages in the Sender.
         *
         * @return ratio for sending data to polling status messages in the Sender.
         */
        public int sendToStatusMessagePollRatio()
        {
            return sendToStatusMessagePollRatio;
        }

        /**
         * Set the ratio for sending data to polling status messages in the Sender.
         *
         * @param ratio to use.
         * @return this for fluent API.
         */
        public Context sendToStatusMessagePollRatio(final int ratio)
        {
            this.sendToStatusMessagePollRatio = ratio;
            return this;
        }

        OneToOneConcurrentArrayQueue<Runnable> receiverCommandQueue()
        {
            return receiverCommandQueue;
        }

        Context receiverCommandQueue(final OneToOneConcurrentArrayQueue<Runnable> receiverCommandQueue)
        {
            this.receiverCommandQueue = receiverCommandQueue;
            return this;
        }

        OneToOneConcurrentArrayQueue<Runnable> senderCommandQueue()
        {
            return senderCommandQueue;
        }

        Context senderCommandQueue(final OneToOneConcurrentArrayQueue<Runnable> senderCommandQueue)
        {
            this.senderCommandQueue = senderCommandQueue;
            return this;
        }

        ManyToOneConcurrentArrayQueue<Runnable> driverCommandQueue()
        {
            return driverCommandQueue;
        }

        Context driverCommandQueue(final ManyToOneConcurrentArrayQueue<Runnable> queue)
        {
            this.driverCommandQueue = queue;
            return this;
        }

        ClientProxy clientProxy()
        {
            return clientProxy;
        }

        Context clientProxy(final ClientProxy clientProxy)
        {
            this.clientProxy = clientProxy;
            return this;
        }

        RingBuffer toDriverCommands()
        {
            return toDriverCommands;
        }

        Context toDriverCommands(final RingBuffer toDriverCommands)
        {
            this.toDriverCommands = toDriverCommands;
            return this;
        }

        LogFactory logFactory()
        {
            return logFactory;
        }

        Context logFactory(final LogFactory logFactory)
        {
            this.logFactory = logFactory;
            return this;
        }

        DataTransportPoller dataTransportPoller()
        {
            return dataTransportPoller;
        }

        Context dataTransportPoller(final DataTransportPoller transportPoller)
        {
            this.dataTransportPoller = transportPoller;
            return this;
        }

        ControlTransportPoller controlTransportPoller()
        {
            return controlTransportPoller;
        }

        Context controlTransportPoller(final ControlTransportPoller transportPoller)
        {
            this.controlTransportPoller = transportPoller;
            return this;
        }

        ReceiverProxy receiverProxy()
        {
            return receiverProxy;
        }

        Context receiverProxy(final ReceiverProxy receiverProxy)
        {
            this.receiverProxy = receiverProxy;
            return this;
        }

        SenderProxy senderProxy()
        {
            return senderProxy;
        }

        Context senderProxy(final SenderProxy senderProxy)
        {
            this.senderProxy = senderProxy;
            return this;
        }

        DriverConductorProxy driverConductorProxy()
        {
            return driverConductorProxy;
        }

        Context driverConductorProxy(final DriverConductorProxy driverConductorProxy)
        {
            this.driverConductorProxy = driverConductorProxy;
            return this;
        }

        @SuppressWarnings("MethodLength")
        void concludeNullProperties()
        {
            if (null == tempBuffer)
            {
                tempBuffer = new UnsafeBuffer(new byte[METADATA_LENGTH]);
            }

            if (null == epochClock)
            {
                epochClock = new SystemEpochClock();
            }

            if (null == nanoClock)
            {
                nanoClock = new SystemNanoClock();
            }

            if (null == cachedEpochClock)
            {
                cachedEpochClock = new CachedEpochClock();
            }

            if (null == cachedNanoClock)
            {
                cachedNanoClock = new CachedNanoClock();
            }

            if (null == unicastFlowControlSupplier)
            {
                unicastFlowControlSupplier = Configuration.unicastFlowControlSupplier();
            }

            if (null == multicastFlowControlSupplier)
            {
                multicastFlowControlSupplier = Configuration.multicastFlowControlSupplier();
            }

            if (null == sendChannelEndpointSupplier)
            {
                sendChannelEndpointSupplier = Configuration.sendChannelEndpointSupplier();
            }

            if (null == receiveChannelEndpointSupplier)
            {
                receiveChannelEndpointSupplier = Configuration.receiveChannelEndpointSupplier();
            }

            if (null == dataTransportPoller)
            {
                dataTransportPoller = new DataTransportPoller();
            }

            if (null == controlTransportPoller)
            {
                controlTransportPoller = new ControlTransportPoller();
            }

            if (null == applicationSpecificFeedback)
            {
                applicationSpecificFeedback = Configuration.applicationSpecificFeedback();
            }

            if (null == receiveChannelEndpointThreadLocals)
            {
                receiveChannelEndpointThreadLocals = new ReceiveChannelEndpointThreadLocals(this);
            }

            if (null == congestionControlSupplier)
            {
                congestionControlSupplier = Configuration.congestionControlSupplier();
            }

            if (null == driverCommandQueue)
            {
                driverCommandQueue = new ManyToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY);
            }

            if (null == receiverCommandQueue)
            {
                receiverCommandQueue = new OneToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY);
            }

            if (null == senderCommandQueue)
            {
                senderCommandQueue = new OneToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY);
            }

            if (null == retransmitUnicastDelayGenerator)
            {
                retransmitUnicastDelayGenerator = new StaticDelayGenerator(retransmitUnicastDelayNs, false);
            }

            if (null == retransmitUnicastLingerGenerator)
            {
                retransmitUnicastLingerGenerator = new StaticDelayGenerator(retransmitUnicastLingerNs, false);
            }

            if (null == unicastFeedbackDelayGenerator)
            {
                unicastFeedbackDelayGenerator = new StaticDelayGenerator(nakUnicastDelayNs, true);
            }

            if (null == multicastFeedbackDelayGenerator)
            {
                multicastFeedbackDelayGenerator = new OptimalMulticastDelayGenerator(
                    nakMulticastMaxBackoffNs, nakMulticastGroupSize);
            }

            if (null == threadingMode)
            {
                threadingMode = Configuration.threadingMode();
            }

            if (null == terminationValidator)
            {
                terminationValidator = Configuration.terminationValidator();
            }
        }

        private void concludeDependantProperties()
        {
            clientProxy = new ClientProxy(new BroadcastTransmitter(
                createToClientsBuffer(cncByteBuffer, cncMetaDataBuffer)));

            toDriverCommands = new ManyToOneRingBuffer(createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

            if (null == errorLog)
            {
                errorLog = new DistinctErrorLog(createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer), epochClock);
            }

            if (null == errorHandler)
            {
                errorHandler = new LoggingErrorHandler(errorLog);
            }

            receiverProxy = new ReceiverProxy(
                threadingMode, receiverCommandQueue, systemCounters.get(RECEIVER_PROXY_FAILS));
            senderProxy = new SenderProxy(
                threadingMode, senderCommandQueue, systemCounters.get(SENDER_PROXY_FAILS));
            driverConductorProxy = new DriverConductorProxy(
                threadingMode, driverCommandQueue, systemCounters.get(CONDUCTOR_PROXY_FAILS));

            if (null == logFactory)
            {
                logFactory = new FileStoreLogFactory(
                    aeronDirectoryName(), filePageSize, performStorageChecks, lowStorageWarningThreshold, errorHandler);
            }

            if (null == lossReport)
            {
                lossReportBuffer = mapLossReport(aeronDirectoryName(), align(lossReportBufferLength, filePageSize));
                lossReport = new LossReport(new UnsafeBuffer(lossReportBuffer));
            }
        }

        private void concludeCounters()
        {
            if (null == countersManager)
            {
                if (countersMetaDataBuffer() == null)
                {
                    countersMetaDataBuffer(createCountersMetaDataBuffer(cncByteBuffer, cncMetaDataBuffer));
                }

                if (countersValuesBuffer() == null)
                {
                    countersValuesBuffer(createCountersValuesBuffer(cncByteBuffer, cncMetaDataBuffer));
                }

                final EpochClock clock;
                final long reuseTimeoutMs;
                if (counterFreeToReuseTimeoutNs > 0)
                {
                    clock = epochClock;
                    reuseTimeoutMs = Math.min(TimeUnit.NANOSECONDS.toMillis(counterFreeToReuseTimeoutNs), 1);
                }
                else
                {
                    clock = () -> 0;
                    reuseTimeoutMs = 0;
                }

                if (useConcurrentCountersManager)
                {
                    countersManager = new ConcurrentCountersManager(
                        countersMetaDataBuffer(), countersValuesBuffer(), US_ASCII, clock, reuseTimeoutMs);
                }
                else
                {
                    countersManager = new CountersManager(
                        countersMetaDataBuffer(), countersValuesBuffer(), US_ASCII, clock, reuseTimeoutMs);
                }
            }

            if (null == systemCounters)
            {
                systemCounters = new SystemCounters(countersManager);
            }
        }

        private void concludeIdleStrategies()
        {
            final StatusIndicator indicator = new UnsafeBufferStatusIndicator(
                countersManager.valuesBuffer(), CONTROLLABLE_IDLE_STRATEGY.id());

            switch (threadingMode)
            {
                case INVOKER:
                    break;

                case SHARED:
                    if (null == sharedThreadFactory)
                    {
                        sharedThreadFactory = Thread::new;
                    }
                    if (null == sharedIdleStrategy)
                    {
                        sharedIdleStrategy = Configuration.sharedIdleStrategy(indicator);
                    }
                    break;

                case SHARED_NETWORK:
                    if (null == conductorThreadFactory)
                    {
                        conductorThreadFactory = Thread::new;
                    }
                    if (null == conductorIdleStrategy)
                    {
                        conductorIdleStrategy = Configuration.conductorIdleStrategy(indicator);
                    }
                    if (null == sharedNetworkThreadFactory)
                    {
                        sharedNetworkThreadFactory = Thread::new;
                    }
                    if (null == sharedNetworkIdleStrategy)
                    {
                        sharedNetworkIdleStrategy = Configuration.sharedNetworkIdleStrategy(indicator);
                    }
                    break;

                case DEDICATED:
                    if (null == conductorThreadFactory)
                    {
                        conductorThreadFactory = Thread::new;
                    }
                    if (null == senderThreadFactory)
                    {
                        senderThreadFactory = Thread::new;
                    }
                    if (null == receiverThreadFactory)
                    {
                        receiverThreadFactory = Thread::new;
                    }
                    if (null == conductorIdleStrategy)
                    {
                        conductorIdleStrategy = Configuration.conductorIdleStrategy(indicator);
                    }
                    if (null == senderIdleStrategy)
                    {
                        senderIdleStrategy = Configuration.senderIdleStrategy(indicator);
                    }
                    if (null == receiverIdleStrategy)
                    {
                        receiverIdleStrategy = Configuration.receiverIdleStrategy(indicator);
                    }
                    break;
            }
        }

        public String toString()
        {
            return "MediaDriver.Context{" +
                "\n    aeronDirectoryName=" + aeronDirectoryName() +
                "\n    driverTimeoutMs=" + driverTimeoutMs() +
                "\n    printConfigurationOnStart=" + printConfigurationOnStart +
                "\n    useWindowsHighResTimer=" + useWindowsHighResTimer +
                "\n    warnIfDirectoryExists=" + warnIfDirectoryExists +
                "\n    dirDeleteOnStart=" + dirDeleteOnStart +
                "\n    termBufferSparseFile=" + termBufferSparseFile +
                "\n    performStorageChecks=" + performStorageChecks +
                "\n    spiesSimulateConnection=" + spiesSimulateConnection +
                "\n    reliableStream=" + reliableStream +
                "\n    tetherSubscriptions=" + tetherSubscriptions +
                "\n    conductorBufferLength=" + conductorBufferLength +
                "\n    toClientsBufferLength=" + toClientsBufferLength +
                "\n    counterValuesBufferLength=" + counterValuesBufferLength +
                "\n    errorBufferLength=" + errorBufferLength +
                "\n    lowStorageWarningThreshold=" + lowStorageWarningThreshold +
                "\n    timerIntervalNs=" + timerIntervalNs +
                "\n    clientLivenessTimeoutNs=" + clientLivenessTimeoutNs +
                "\n    imageLivenessTimeoutNs=" + imageLivenessTimeoutNs +
                "\n    publicationUnblockTimeoutNs=" + publicationUnblockTimeoutNs +
                "\n    publicationConnectionTimeoutNs=" + publicationConnectionTimeoutNs +
                "\n    publicationLingerTimeoutNs=" + publicationLingerTimeoutNs +
                "\n    untetheredWindowLimitTimeoutNs=" + untetheredWindowLimitTimeoutNs +
                "\n    untetheredRestingTimeoutNs=" + untetheredRestingTimeoutNs +
                "\n    retransmitUnicastDelayNs=" + retransmitUnicastDelayNs +
                "\n    retransmitUnicastLingerNs=" + retransmitUnicastLingerNs +
                "\n    nakUnicastDelayNs=" + nakUnicastDelayNs +
                "\n    nakMulticastMaxBackoffNs=" + nakMulticastMaxBackoffNs +
                "\n    nakMulticastGroupSize=" + nakMulticastGroupSize +
                "\n    statusMessageTimeoutNs=" + statusMessageTimeoutNs +
                "\n    counterFreeToReuseTimeoutNs=" + counterFreeToReuseTimeoutNs +
                "\n    publicationTermBufferLength=" + publicationTermBufferLength +
                "\n    ipcTermBufferLength=" + ipcTermBufferLength +
                "\n    publicationTermWindowLength=" + publicationTermWindowLength +
                "\n    ipcPublicationTermWindowLength=" + ipcPublicationTermWindowLength +
                "\n    initialWindowLength=" + initialWindowLength +
                "\n    socketSndbufLength=" + socketSndbufLength +
                "\n    socketRcvbufLength=" + socketRcvbufLength +
                "\n    socketMulticastTtl=" + socketMulticastTtl +
                "\n    mtuLength=" + mtuLength +
                "\n    ipcMtuLength=" + ipcMtuLength +
                "\n    filePageSize=" + filePageSize +
                "\n    publicationReservedSessionIdLow=" + publicationReservedSessionIdLow +
                "\n    publicationReservedSessionIdHigh=" + publicationReservedSessionIdHigh +
                "\n    lossReportBufferLength=" + lossReportBufferLength +
                "\n    epochClock=" + epochClock +
                "\n    nanoClock=" + nanoClock +
                "\n    cachedEpochClock=" + cachedEpochClock +
                "\n    cachedNanoClock=" + cachedNanoClock +
                "\n    threadingMode=" + threadingMode +
                "\n    conductorThreadFactory=" + conductorThreadFactory +
                "\n    senderThreadFactory=" + senderThreadFactory +
                "\n    receiverThreadFactory=" + receiverThreadFactory +
                "\n    sharedThreadFactory=" + sharedThreadFactory +
                "\n    sharedNetworkThreadFactory=" + sharedNetworkThreadFactory +
                "\n    conductorIdleStrategy=" + conductorIdleStrategy +
                "\n    senderIdleStrategy=" + senderIdleStrategy +
                "\n    receiverIdleStrategy=" + receiverIdleStrategy +
                "\n    sharedNetworkIdleStrategy=" + sharedNetworkIdleStrategy +
                "\n    sharedIdleStrategy=" + sharedIdleStrategy +
                "\n    sendChannelEndpointSupplier=" + sendChannelEndpointSupplier +
                "\n    receiveChannelEndpointSupplier=" + receiveChannelEndpointSupplier +
                "\n    receiveChannelEndpointThreadLocals=" + receiveChannelEndpointThreadLocals +
                "\n    tempBuffer=" + tempBuffer +
                "\n    unicastFlowControlSupplier=" + unicastFlowControlSupplier +
                "\n    multicastFlowControlSupplier=" + multicastFlowControlSupplier +
                "\n    applicationSpecificFeedback=" + Arrays.toString(applicationSpecificFeedback) +
                "\n    congestionControlSupplier=" + congestionControlSupplier +
                "\n    terminationValidator=" + terminationValidator +
                "\n    terminationHook=" + terminationHook +
                "\n    sendToStatusMessagePollRatio=" + sendToStatusMessagePollRatio +
                "\n    unicastFeedbackDelayGenerator=" + unicastFeedbackDelayGenerator +
                "\n    multicastFeedbackDelayGenerator=" + multicastFeedbackDelayGenerator +
                "\n    retransmitUnicastDelayGenerator=" + retransmitUnicastDelayGenerator +
                "\n    retransmitUnicastLingerGenerator=" + retransmitUnicastLingerGenerator +
                "\n    errorLog=" + errorLog +
                "\n    errorHandler=" + errorHandler +
                "\n    useConcurrentCountersManager=" + useConcurrentCountersManager +
                "\n    countersManager=" + countersManager +
                "\n    systemCounters=" + systemCounters +
                "\n    lossReport=" + lossReport +
                "\n    logFactory=" + logFactory +
                "\n    dataTransportPoller=" + dataTransportPoller +
                "\n    controlTransportPoller=" + controlTransportPoller +
                "\n    driverCommandQueue=" + driverCommandQueue +
                "\n    receiverCommandQueue=" + receiverCommandQueue +
                "\n    senderCommandQueue=" + senderCommandQueue +
                "\n    receiverProxy=" + receiverProxy +
                "\n    senderProxy=" + senderProxy +
                "\n    driverConductorProxy=" + driverConductorProxy +
                "\n    clientProxy=" + clientProxy +
                "\n    toDriverCommands=" + toDriverCommands +
                "\n    lossReportBuffer=" + lossReportBuffer +
                "\n    cncByteBuffer=" + cncByteBuffer +
                "\n    cncMetaDataBuffer=" + cncMetaDataBuffer +
                "\n}";
        }
    }
}
