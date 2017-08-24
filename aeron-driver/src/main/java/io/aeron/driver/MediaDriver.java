/*
 * Copyright 2014-2017 Real Logic Ltd.
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
import io.aeron.driver.buffer.RawLogFactory;
import io.aeron.driver.cmd.*;
import io.aeron.driver.exceptions.*;
import io.aeron.driver.media.*;
import io.aeron.driver.reports.LossReport;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.ringbuffer.*;
import org.agrona.concurrent.status.*;

import java.io.*;
import java.net.*;
import java.nio.MappedByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import static io.aeron.CncFileDescriptor.*;
import static io.aeron.driver.Configuration.*;
import static io.aeron.driver.reports.LossReportUtil.mapLossReport;
import static io.aeron.driver.status.SystemCounterDescriptor.*;
import static io.aeron.driver.status.SystemCounterDescriptor.CONTROLLABLE_IDLE_STRATEGY;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.IoUtil.mapNewFile;

/**
 * Main class for JVM-based media driver
 * <p>
 * Usage:
 * <code>
 * $ java -jar aeron-driver.jar
 * $ java -Doption=value -jar aeron-driver.jar
 * </code>
 * <p>
 * {@link Configuration}
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
     * Load system properties from a given filename or url.
     * <p>
     * File is first searched for in resources, then file system, then URL. All are loaded if multiples found.
     *
     * @param filenameOrUrl that holds properties
     */
    public static void loadPropertiesFile(final String filenameOrUrl)
    {
        final Properties properties = new Properties(System.getProperties());

        final URL resource = MediaDriver.class.getClassLoader().getResource(filenameOrUrl);
        if (null != resource)
        {
            try (InputStream in = resource.openStream())
            {
                properties.load(in);
            }
            catch (final Exception ignore)
            {
            }
        }

        final File file = new File(filenameOrUrl);
        if (file.exists())
        {
            try (FileInputStream in = new FileInputStream(file))
            {
                properties.load(in);
            }
            catch (final Exception ignore)
            {
            }
        }

        try (InputStream in = new URL(filenameOrUrl).openStream())
        {
            properties.load(in);
        }
        catch (final Exception ignore)
        {
        }

        System.setProperties(properties);
    }

    /**
     * Load system properties from a given set of filenames or URLs.
     *
     * @param filenamesOrUrls that holds properties
     * @see #loadPropertiesFile(String)
     */
    public static void loadPropertiesFiles(final String[] filenamesOrUrls)
    {
        for (final String filenameOrUrl : filenamesOrUrls)
        {
            loadPropertiesFile(filenameOrUrl);
        }
    }

    /**
     * Start Media Driver as a stand-alone process.
     *
     * @param args command line arguments
     * @throws Exception if an error occurs
     */
    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        try (MediaDriver ignore = MediaDriver.launch())
        {
            new ShutdownSignalBarrier().await();

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

        // TODO: eliminate queues if mode is not concurrent
        ctx
            .driverCommandQueue(new ManyToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY))
            .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY))
            .senderCommandQueue(new OneToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY))
            .conclude();

        final DriverConductor conductor = new DriverConductor(ctx);
        final Receiver receiver = new Receiver(ctx);
        final Sender sender = new Sender(ctx);

        ctx.receiverProxy().receiver(receiver);
        ctx.senderProxy().sender(sender);
        ctx.driverConductorProxy().driverConductor(conductor);

        final AtomicCounter errorCounter = ctx.systemCounters().get(ERRORS);
        final ErrorHandler errorHandler = ctx.errorHandler();

        switch (ctx.threadingMode)
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
                    ctx.sharedIdleStrategy,
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
                    ctx.sharedNetworkIdleStrategy, errorHandler, errorCounter, new CompositeAgent(sender, receiver));
                conductorRunner = new AgentRunner(ctx.conductorIdleStrategy, errorHandler, errorCounter, conductor);
                sharedRunner = null;
                receiverRunner = null;
                senderRunner = null;
                sharedInvoker = null;
                break;

            default:
            case DEDICATED:
                senderRunner = new AgentRunner(ctx.senderIdleStrategy, errorHandler, errorCounter, sender);
                receiverRunner = new AgentRunner(ctx.receiverIdleStrategy, errorHandler, errorCounter, receiver);
                conductorRunner = new AgentRunner(ctx.conductorIdleStrategy, errorHandler, errorCounter, conductor);
                sharedNetworkRunner = null;
                sharedRunner = null;
                sharedInvoker = null;
                break;
        }
    }

    /**
     * Launch an isolated MediaDriver embedded in the current process with a generated aeronDirectoryName that can be
     * retrieved by calling aeronDirectoryName.
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
     * If the aeronDirectoryName is configured then it will be used.
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
        CloseHelper.quietClose(sharedRunner);
        CloseHelper.quietClose(sharedNetworkRunner);
        CloseHelper.quietClose(receiverRunner);
        CloseHelper.quietClose(senderRunner);
        CloseHelper.quietClose(conductorRunner);
        CloseHelper.quietClose(sharedInvoker);

        if (ctx.useWindowsHighResTimer() && SystemUtil.osName().startsWith("win"))
        {
            if (!wasHighResTimerEnabled)
            {
                HighResolutionTimer.disable();
            }
        }

        ctx.close();
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
            AgentRunner.startOnThread(conductorRunner, ctx.conductorThreadFactory);
        }

        if (null != senderRunner)
        {
            AgentRunner.startOnThread(senderRunner, ctx.senderThreadFactory);
        }

        if (null != receiverRunner)
        {
            AgentRunner.startOnThread(receiverRunner, ctx.receiverThreadFactory);
        }

        if (null != sharedNetworkRunner)
        {
            AgentRunner.startOnThread(sharedNetworkRunner, ctx.sharedNetworkThreadFactory);
        }

        if (null != sharedRunner)
        {
            AgentRunner.startOnThread(sharedRunner, ctx.sharedThreadFactory);
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
                        throw new ActiveDriverException("Active driver detected");
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
                final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSSZ");
                final String errorLogFilename =
                    ctx.aeronDirectoryName() + '-' + dateFormat.format(new Date()) + "-error.log";

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

    /**
     * Configuration for the {@link MediaDriver} that can be used to override {@link Configuration}.
     * <p>
     * <b>Note:</b> Do not reuse instances of this {@link Context} across different {@link MediaDriver}s.
     */
    public static class Context extends CommonContext
    {
        private boolean useWindowsHighResTimer = Configuration.USE_WINDOWS_HIGH_RES_TIMER;
        private boolean warnIfDirectoryExists = Configuration.DIR_WARN_IF_EXISTS;
        private boolean dirDeleteOnStart = Configuration.DIR_DELETE_ON_START;
        private boolean termBufferSparseFile = Configuration.TERM_BUFFER_SPARSE_FILE;

        private long clientLivenessTimeoutNs = Configuration.CLIENT_LIVENESS_TIMEOUT_NS;
        private long imageLivenessTimeoutNs = Configuration.IMAGE_LIVENESS_TIMEOUT_NS;
        private long publicationUnblockTimeoutNs = Configuration.PUBLICATION_UNBLOCK_TIMEOUT_NS;
        private long statusMessageTimeoutNs = Configuration.statusMessageTimeout();
        private int maxTermBufferLength = Configuration.maxTermBufferLength();
        private int publicationTermBufferLength = Configuration.termBufferLength();
        private int ipcPublicationTermBufferLength = Configuration.ipcTermBufferLength(publicationTermBufferLength);
        private int initialWindowLength = Configuration.initialWindowLength();
        private int mtuLength = Configuration.MTU_LENGTH;
        private int ipcMtuLength = Configuration.IPC_MTU_LENGTH;

        private EpochClock epochClock;
        private NanoClock nanoClock;
        private RawLogFactory rawLogFactory;
        private DataTransportPoller dataTransportPoller;
        private ControlTransportPoller controlTransportPoller;
        private FlowControlSupplier unicastFlowControlSupplier;
        private FlowControlSupplier multicastFlowControlSupplier;
        private ManyToOneConcurrentArrayQueue<DriverConductorCmd> driverCommandQueue;
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
        private DistinctErrorLog errorLog;
        private ErrorHandler errorHandler;

        private MappedByteBuffer lossReportBuffer;
        private LossReport lossReport;

        private MappedByteBuffer cncByteBuffer;
        private UnsafeBuffer cncMetaDataBuffer;

        private boolean useConcurrentCountersManager;
        private CountersManager countersManager;
        private SystemCounters systemCounters;

        private ThreadingMode threadingMode;
        private ThreadFactory conductorThreadFactory;
        private ThreadFactory senderThreadFactory;
        private ThreadFactory receiverThreadFactory;
        private ThreadFactory sharedThreadFactory;
        private ThreadFactory sharedNetworkThreadFactory;

        private SendChannelEndpointSupplier sendChannelEndpointSupplier;
        private ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier;
        private ReceiveChannelEndpointThreadLocals receiveChannelEndpointThreadLocals;

        private byte[] applicationSpecificFeedback = Configuration.SM_APPLICATION_SPECIFIC_FEEDBACK;
        private CongestionControlSupplier congestionControlSupplier;

        /**
         * Free up resources but don't delete files in case they are required for debugging.
         */
        public void close()
        {
            IoUtil.unmap(cncByteBuffer);
            IoUtil.unmap(lossReportBuffer);

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

                LogBufferDescriptor.checkTermLength(maxTermBufferLength);
                LogBufferDescriptor.checkTermLength(publicationTermBufferLength);
                LogBufferDescriptor.checkTermLength(ipcPublicationTermBufferLength);

                if (publicationTermBufferLength > maxTermBufferLength)
                {
                    throw new ConfigurationException(
                        "publication term buffer length " + publicationTermBufferLength +
                            " greater than max length " + maxTermBufferLength);
                }

                if (ipcPublicationTermBufferLength > maxTermBufferLength)
                {
                    throw new ConfigurationException(
                        "IPC publication term buffer length " + ipcPublicationTermBufferLength +
                            " greater than max length " + maxTermBufferLength);
                }

                Configuration.validateInitialWindowLength(initialWindowLength, mtuLength);

                cncByteBuffer = mapNewFile(
                    cncFile(),
                    CncFileDescriptor.computeCncFileLength(
                        CONDUCTOR_BUFFER_LENGTH + TO_CLIENTS_BUFFER_LENGTH +
                            COUNTERS_METADATA_BUFFER_LENGTH + COUNTERS_VALUES_BUFFER_LENGTH + ERROR_BUFFER_LENGTH));

                cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
                CncFileDescriptor.fillMetaData(
                    cncMetaDataBuffer,
                    CONDUCTOR_BUFFER_LENGTH,
                    TO_CLIENTS_BUFFER_LENGTH,
                    COUNTERS_METADATA_BUFFER_LENGTH,
                    COUNTERS_VALUES_BUFFER_LENGTH,
                    clientLivenessTimeoutNs,
                    ERROR_BUFFER_LENGTH);

                clientProxy(new ClientProxy(new BroadcastTransmitter(
                    createToClientsBuffer(cncByteBuffer, cncMetaDataBuffer))));

                toDriverCommands(new ManyToOneRingBuffer(createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer)));

                if (null == errorLog)
                {
                    errorLog = new DistinctErrorLog(createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer), epochClock);
                }

                if (null == errorHandler)
                {
                    errorHandler =
                        (throwable) ->
                        {
                            if (!errorLog.record(throwable))
                            {
                                System.err.println(
                                    "Error Log is full, consider increasing " + ERROR_BUFFER_LENGTH_PROP_NAME);
                                throwable.printStackTrace(System.err);
                            }
                        };
                }

                concludeCounters();

                receiverProxy(new ReceiverProxy(
                    threadingMode, receiverCommandQueue(), systemCounters.get(RECEIVER_PROXY_FAILS)));
                senderProxy(new SenderProxy(
                    threadingMode, senderCommandQueue(), systemCounters.get(SENDER_PROXY_FAILS)));
                driverConductorProxy(new DriverConductorProxy(
                    threadingMode, driverCommandQueue, systemCounters.get(CONDUCTOR_PROXY_FAILS)));

                rawLogBuffersFactory(new RawLogFactory(
                    aeronDirectoryName(), maxTermBufferLength, termBufferSparseFile, errorLog));

                if (null == lossReport)
                {
                    lossReportBuffer = mapLossReport(aeronDirectoryName(), Configuration.LOSS_REPORT_BUFFER_LENGTH);
                    lossReport = new LossReport(new UnsafeBuffer(lossReportBuffer));
                }

                concludeIdleStrategies();

                toDriverCommands.consumerHeartbeatTime(epochClock.time());
                CncFileDescriptor.signalCncReady(cncMetaDataBuffer);
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return this;
        }

        /**
         * Should an attempt be made to use the high resolution timers for waiting on Windows.
         *
         * @param useWindowsHighResTimers Should an attempt be made to use the high-res timers for waiting on Windows.
         * @return this for a fluent API.
         */
        public Context useWindowsHighResTimer(final boolean useWindowsHighResTimers)
        {
            this.useWindowsHighResTimer = useWindowsHighResTimers;
            return this;
        }

        /*
         * Should an attempt be made to use the high resolution timers for waiting on Windows.
         */
        public boolean useWindowsHighResTimer()
        {
            return useWindowsHighResTimer;
        }

        /**
         * Should a warning be issued if the {@link #aeronDirectoryName()} exists?
         *
         * @return should a warning be issued if the {@link #aeronDirectoryName()} exists?
         */
        public boolean warnIfDirectoryExists()
        {
            return  warnIfDirectoryExists;
        }

        /**
         * Should a warning be issued if the {@link #aeronDirectoryName()} exists?
         *
         * @param warnIfDirectoryExists warn if the {@link #aeronDirectoryName()} exists?
         * @return this for a fluent API.
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
         */
        public Context termBufferSparseFile(final boolean termBufferSparseFile)
        {
            this.termBufferSparseFile = termBufferSparseFile;
            return this;
        }

        /**
         * Time in nanoseconds an Image will be kept alive for its subscribers to consume it once disconnected.
         *
         * @return nanoseconds that an Image will be kept alive for its subscribers to consume it.
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
         */
        public Context imageLivenessTimeoutNs(final long timeout)
        {
            this.imageLivenessTimeoutNs = timeout;
            return this;
        }

        /**
         * Time in nanoseconds after which a client is considered dead if a keep alive is not received.
         *
         * @return time in nanoseconds after which a client is considered dead if a keep alive is not received.
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
         */
        public Context statusMessageTimeoutNs(final long statusMessageTimeoutNs)
        {
            this.statusMessageTimeoutNs = statusMessageTimeoutNs;
            return this;
        }

        /**
         * Timeout in nanoseconds after which a publication will be unblocked if a offer is partially complete to allow
         * other publishers to make progress.
         *
         * @return timeout in nanoseconds after which a publication will be unblocked.
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
         */
        public Context publicationUnblockTimeoutNs(final long timeoutNs)
        {
            this.publicationUnblockTimeoutNs = timeoutNs;
            return this;
        }

        /**
         * Maximum length for a term buffer in the log which must be a power of two.
         *
         * @return maximum length for a term buffer in the log which must be a power of two.
         */
        public int maxTermBufferLength()
        {
            return maxTermBufferLength;
        }

        /**
         * Maximum length for a term buffer in the log which must be a power of two.
         *
         * @param maxTermBufferLength for a term buffer in the log which must be a power of two.
         * @return this for a fluent API.
         */
        public Context maxTermBufferLength(final int maxTermBufferLength)
        {
            this.maxTermBufferLength = maxTermBufferLength;
            return this;
        }

        /**
         * Default length for a term buffer on a network publication.
         *
         * @return default length for a term buffer on a network publication.
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
         */
        public int ipcTermBufferLength()
        {
            return ipcPublicationTermBufferLength;
        }

        /**
         * Default length for a term buffer on a IPC publication.
         * <p>
         * This can be overridden on publication by using channel URI params.
         *
         * @param termBufferLength default length for a term buffer on a IPC publication.
         * @return this for a fluent API.
         */
        public Context ipcTermBufferLength(final int termBufferLength)
        {
            this.ipcPublicationTermBufferLength = termBufferLength;
            return this;
        }

        /**
         * The initial window for in flight data on a connection which must be less than
         * {@link Configuration#SOCKET_RCVBUF_LENGTH}. This needs to be configured for throughput respecting BDP.
         *
         * @return The initial window for in flight data on a connection
         */
        public int initialWindowLength()
        {
            return initialWindowLength;
        }

        /**
         * The initial window for in flight data on a connection which must be less than
         * {@link Configuration#SOCKET_RCVBUF_LENGTH}. This needs to be configured for throughput respecting BDP.
         *
         * @param initialWindowLength The initial window for in flight data on a connection
         * @return this for a fluent API.
         */
        public Context initialWindowLength(final int initialWindowLength)
        {
            this.initialWindowLength = initialWindowLength;
            return this;
        }

        /**
         * MTU in bytes for datagrams sent to the network. Messages larger than this are fragmented.
         * <p>
         * Larger MTUs reduce system call overhead at the expense of possible increase in loss which
         * will need to be recovered.
         *
         * @return MTU in bytes for datagrams sent to the network.
         */
        public int mtuLength()
        {
            return mtuLength;
        }

        /**
         * MTU in bytes for datagrams sent to the network. Messages larger than this are fragmented.
         * <p>
         * Larger MTUs reduce system call overhead at the expense of possible increase in loss which
         * will need to be recovered.
         *
         * @param mtuLength in bytes for datagrams sent to the network.
         * @return this for a fluent API.
         */
        public Context mtuLength(final int mtuLength)
        {
            this.mtuLength = mtuLength;
            return this;
        }

        /**
         * MTU in bytes for datagrams sent to the network. Messages larger than this are fragmented.
         * <p>
         * Larger MTUs reduce fragmentation.
         *
         * @return MTU in bytes for message fragments.
         */
        public int ipcMtuLength()
        {
            return ipcMtuLength;
        }

        /**
         * MTU in bytes for datagrams sent to the network. Messages larger than this are fragmented.
         * <p>
         * Larger MTUs reduce fragmentation.
         *
         * @param ipcMtuLength in bytes for message fragments.
         * @return this for a fluent API.
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
            this.epochClock = clock;
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
            this.nanoClock = clock;
            return this;
        }

        public Context unicastFlowControlSupplier(final FlowControlSupplier senderFlowControl)
        {
            this.unicastFlowControlSupplier = senderFlowControl;
            return this;
        }

        public Context multicastFlowControlSupplier(final FlowControlSupplier senderFlowControl)
        {
            this.multicastFlowControlSupplier = senderFlowControl;
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

        public Context useConcurrentCountersManager(final boolean useConcurrentCountersManager)
        {
            this.useConcurrentCountersManager = useConcurrentCountersManager;
            return this;
        }

        public Context countersManager(final CountersManager countersManager)
        {
            this.countersManager = countersManager;
            return this;
        }

        public Context errorLog(final DistinctErrorLog errorLog)
        {
            this.errorLog = errorLog;
            return this;
        }

        public Context lossReport(final LossReport lossReport)
        {
            this.lossReport = lossReport;
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

        public Context senderThreadFactory(final ThreadFactory factory)
        {
            this.senderThreadFactory = factory;
            return this;
        }

        public Context receiverThreadFactory(final ThreadFactory factory)
        {
            this.receiverThreadFactory = factory;
            return this;
        }

        public Context conductorThreadFactory(final ThreadFactory factory)
        {
            this.conductorThreadFactory = factory;
            return this;
        }

        public Context sharedThreadFactory(final ThreadFactory factory)
        {
            this.sharedThreadFactory = factory;
            return this;
        }

        public Context sharedNetworkThreadFactory(final ThreadFactory factory)
        {
            this.sharedNetworkThreadFactory = factory;
            return this;
        }

        /**
         * @see CommonContext#aeronDirectoryName(String)
         */
        public Context aeronDirectoryName(final String dirName)
        {
            super.aeronDirectoryName(dirName);
            return this;
        }

        public Context sendChannelEndpointSupplier(final SendChannelEndpointSupplier supplier)
        {
            this.sendChannelEndpointSupplier = supplier;
            return this;
        }

        public Context receiveChannelEndpointSupplier(final ReceiveChannelEndpointSupplier supplier)
        {
            this.receiveChannelEndpointSupplier = supplier;
            return this;
        }

        public Context receiveChannelEndpointThreadLocals(final ReceiveChannelEndpointThreadLocals threadLocals)
        {
            this.receiveChannelEndpointThreadLocals = threadLocals;
            return this;
        }

        public Context applicationSpecificFeedback(final byte[] bytes)
        {
            this.applicationSpecificFeedback = bytes;
            return this;
        }

        public Context congestControlSupplier(final CongestionControlSupplier supplier)
        {
            this.congestionControlSupplier = supplier;
            return this;
        }

        public ManyToOneConcurrentArrayQueue<DriverConductorCmd> driverCommandQueue()
        {
            return driverCommandQueue;
        }

        public RawLogFactory rawLogBuffersFactory()
        {
            return rawLogFactory;
        }

        public DataTransportPoller dataTransportPoller()
        {
            return dataTransportPoller;
        }

        public ControlTransportPoller controlTransportPoller()
        {
            return controlTransportPoller;
        }

        public FlowControlSupplier unicastFlowControlSupplier()
        {
            return unicastFlowControlSupplier;
        }

        public FlowControlSupplier multicastFlowControlSupplier()
        {
            return multicastFlowControlSupplier;
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

        public ThreadingMode threadingMode()
        {
            return threadingMode;
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

        public ThreadFactory senderThreadFactory()
        {
            return this.senderThreadFactory;
        }

        public ThreadFactory receiverThreadFactory()
        {
            return this.receiverThreadFactory;
        }

        public ThreadFactory conductorThreadFactory()
        {
            return this.conductorThreadFactory;
        }

        public ThreadFactory sharedThreadFactory()
        {
            return this.sharedThreadFactory;
        }

        public ThreadFactory sharedNetworkThreadFactory()
        {
            return this.sharedNetworkThreadFactory;
        }

        public ClientProxy clientProxy()
        {
            return clientProxy;
        }

        public RingBuffer toDriverCommands()
        {
            return toDriverCommands;
        }

        public boolean useConcurrentCountersManager()
        {
            return useConcurrentCountersManager;
        }

        public CountersManager countersManager()
        {
            return countersManager;
        }

        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        public DistinctErrorLog errorLog()
        {
            return errorLog;
        }

        public LossReport lossReport()
        {
            return lossReport;
        }

        public SystemCounters systemCounters()
        {
            return systemCounters;
        }

        public SendChannelEndpointSupplier sendChannelEndpointSupplier()
        {
            return sendChannelEndpointSupplier;
        }

        public ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier()
        {
            return receiveChannelEndpointSupplier;
        }

        public ReceiveChannelEndpointThreadLocals receiveChannelEndpointThreadLocals()
        {
            return receiveChannelEndpointThreadLocals;
        }

        public byte[] applicationSpecificFeedback()
        {
            return applicationSpecificFeedback;
        }

        public CongestionControlSupplier congestionControlSupplier()
        {
            return congestionControlSupplier;
        }

        // Methods for testing.

        public Context driverCommandQueue(final ManyToOneConcurrentArrayQueue<DriverConductorCmd> queue)
        {
            this.driverCommandQueue = queue;
            return this;
        }

        public Context rawLogBuffersFactory(final RawLogFactory rawLogFactory)
        {
            this.rawLogFactory = rawLogFactory;
            return this;
        }

        public Context dataTransportPoller(final DataTransportPoller transportPoller)
        {
            this.dataTransportPoller = transportPoller;
            return this;
        }

        public Context controlTransportPoller(final ControlTransportPoller transportPoller)
        {
            this.controlTransportPoller = transportPoller;
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

                final UnsafeBuffer metaDataBuffer = countersMetaDataBuffer();
                final UnsafeBuffer valuesBuffer = countersValuesBuffer();
                if (useConcurrentCountersManager)
                {
                    countersManager(new ConcurrentCountersManager(metaDataBuffer, valuesBuffer, US_ASCII));
                }
                else
                {
                    countersManager(new CountersManager(metaDataBuffer, valuesBuffer, US_ASCII));
                }
            }

            if (null == systemCounters)
            {
                systemCounters = new SystemCounters(countersManager);
            }
        }

        private void concludeNullProperties()
        {
            if (null == epochClock)
            {
                epochClock = new SystemEpochClock();
            }

            if (null == nanoClock)
            {
                nanoClock = new SystemNanoClock();
            }

            if (null == threadingMode)
            {
                threadingMode = Configuration.THREADING_MODE_DEFAULT;
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

            if (null == sharedThreadFactory)
            {
                sharedThreadFactory = Thread::new;
            }

            if (null == sharedNetworkThreadFactory)
            {
                sharedNetworkThreadFactory = Thread::new;
            }

            if (null == receiveChannelEndpointThreadLocals)
            {
                receiveChannelEndpointThreadLocals = new ReceiveChannelEndpointThreadLocals(this);
            }

            if (null == congestionControlSupplier)
            {
                congestionControlSupplier = Configuration.congestionControlSupplier();
            }
        }

        private void concludeIdleStrategies()
        {
            final StatusIndicator controllableIdleStrategyStatus = new UnsafeBufferStatusIndicator(
                countersManager.valuesBuffer(), CONTROLLABLE_IDLE_STRATEGY.id());

            if (null == conductorIdleStrategy)
            {
                conductorIdleStrategy(Configuration.conductorIdleStrategy(controllableIdleStrategyStatus));
            }

            if (null == senderIdleStrategy)
            {
                senderIdleStrategy(Configuration.senderIdleStrategy(controllableIdleStrategyStatus));
            }

            if (null == receiverIdleStrategy)
            {
                receiverIdleStrategy(Configuration.receiverIdleStrategy(controllableIdleStrategyStatus));
            }

            if (null == sharedNetworkIdleStrategy)
            {
                sharedNetworkIdleStrategy(Configuration.sharedNetworkIdleStrategy(controllableIdleStrategyStatus));
            }

            if (null == sharedIdleStrategy)
            {
                sharedIdleStrategy(Configuration.sharedIdleStrategy(controllableIdleStrategyStatus));
            }
        }
    }
}
