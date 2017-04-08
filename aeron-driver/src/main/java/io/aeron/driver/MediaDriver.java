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

import io.aeron.CncFileDescriptor;
import io.aeron.CommonContext;
import io.aeron.driver.buffer.RawLogFactory;
import io.aeron.driver.cmd.DriverConductorCmd;
import io.aeron.driver.cmd.ReceiverCmd;
import io.aeron.driver.cmd.SenderCmd;
import io.aeron.driver.exceptions.ActiveDriverException;
import io.aeron.driver.exceptions.ConfigurationException;
import io.aeron.driver.media.ControlTransportPoller;
import io.aeron.driver.media.DataTransportPoller;
import io.aeron.driver.media.ReceiveChannelEndpointThreadLocals;
import io.aeron.driver.reports.LossReport;
import io.aeron.driver.status.SystemCounters;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.broadcast.BroadcastTransmitter;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.*;

import java.io.*;
import java.net.StandardSocketOptions;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.DatagramChannel;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

import static io.aeron.driver.reports.LossReportUtil.mapLossReport;
import static java.lang.Boolean.getBoolean;
import static org.agrona.IoUtil.mapNewFile;
import static io.aeron.CncFileDescriptor.*;
import static io.aeron.driver.Configuration.*;
import static io.aeron.driver.status.SystemCounterDescriptor.*;

/**
 * Main class for JVM-based media driver
 *
 * Usage:
 * <code>
 * $ java -jar aeron-driver.jar
 * $ java -Doption=value -jar aeron-driver.jar
 * </code>
 *
 * {@link Configuration}
 */
public final class MediaDriver implements AutoCloseable
{
    /**
     * Attempt to delete directories on start if they exist
     */
    public static final String DIRS_DELETE_ON_START_PROP_NAME = "aeron.dir.delete.on.start";

    private final AgentRunner sharedRunner;
    private final AgentRunner sharedNetworkRunner;
    private final AgentRunner conductorRunner;
    private final AgentRunner receiverRunner;
    private final AgentRunner senderRunner;
    private final Context ctx;

    /**
     * Load system properties from a given filename or url.
     *
     * File is first searched for in resources, then file system, then URL. All are loaded if multiples found.
     *
     * @param filenameOrUrl that holds properties
     */
    public static void loadPropertiesFile(final String filenameOrUrl)
    {
        final Properties properties = new Properties(System.getProperties());

        try (InputStream in = MediaDriver.class.getClassLoader().getResourceAsStream(filenameOrUrl))
        {
            properties.load(in);
        }
        catch (final Exception ignore)
        {
        }

        try (FileInputStream in = new FileInputStream(filenameOrUrl))
        {
            properties.load(in);
        }
        catch (final Exception ignore)
        {
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

        try (MediaDriver ignored = MediaDriver.launch())
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
        validateSufficientSocketBufferLengths(ctx);

        ctx
            .toConductorFromReceiverCommandQueue(new OneToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY))
            .toConductorFromSenderCommandQueue(new OneToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY))
            .receiverCommandQueue(new OneToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY))
            .senderCommandQueue(new OneToOneConcurrentArrayQueue<>(CMD_QUEUE_CAPACITY))
            .conclude();

        final Receiver receiver = new Receiver(ctx);
        final Sender sender = new Sender(ctx);
        final DriverConductor conductor = new DriverConductor(ctx);

        ctx.receiverProxy().receiver(receiver);
        ctx.senderProxy().sender(sender);
        ctx.fromReceiverDriverConductorProxy().driverConductor(conductor);
        ctx.fromSenderDriverConductorProxy().driverConductor(conductor);
        ctx.toDriverCommands().consumerHeartbeatTime(ctx.epochClock().time());

        final AtomicCounter errorCounter = ctx.systemCounters().get(ERRORS);
        final ErrorHandler errorHandler = ctx.errorHandler();

        switch (ctx.threadingMode)
        {
            case SHARED:
                this.sharedRunner = new AgentRunner(
                    ctx.sharedIdleStrategy,
                    errorHandler,
                    errorCounter,
                    new CompositeAgent(sender, receiver, conductor));
                this.sharedNetworkRunner = null;
                this.conductorRunner = null;
                this.receiverRunner = null;
                this.senderRunner = null;
                break;

            case SHARED_NETWORK:
                this.sharedNetworkRunner = new AgentRunner(
                    ctx.sharedNetworkIdleStrategy,
                    errorHandler,
                    errorCounter,
                    new CompositeAgent(sender, receiver));
                this.conductorRunner = new AgentRunner(
                    ctx.conductorIdleStrategy, errorHandler, errorCounter, conductor);
                this.sharedRunner = null;
                this.receiverRunner = null;
                this.senderRunner = null;
                break;

            default:
            case DEDICATED:
                this.senderRunner = new AgentRunner(ctx.senderIdleStrategy, errorHandler, errorCounter, sender);
                this.receiverRunner = new AgentRunner(ctx.receiverIdleStrategy, errorHandler, errorCounter, receiver);
                this.conductorRunner = new AgentRunner(
                    ctx.conductorIdleStrategy, errorHandler, errorCounter, conductor);
                this.sharedNetworkRunner = null;
                this.sharedRunner = null;
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
     *
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
     * Shutdown the media driver by stopping all threads and freeing resources.
     */
    public void close()
    {
        CloseHelper.quietClose(sharedRunner);
        CloseHelper.quietClose(sharedNetworkRunner);
        CloseHelper.quietClose(receiverRunner);
        CloseHelper.quietClose(senderRunner);
        CloseHelper.quietClose(conductorRunner);

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
        if (SystemUtil.osName().startsWith("win") && !HighResolutionTimer.isEnabled())
        {
            HighResolutionTimer.enable();
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

        return this;
    }

    private static void validateSufficientSocketBufferLengths(final Context ctx)
    {
        try (DatagramChannel probe = DatagramChannel.open())
        {
            final int defaultSoSndBuf = probe.getOption(StandardSocketOptions.SO_SNDBUF);

            probe.setOption(StandardSocketOptions.SO_SNDBUF, Integer.MAX_VALUE);
            final int maxSoSndBuf = probe.getOption(StandardSocketOptions.SO_SNDBUF);

            if (maxSoSndBuf < SOCKET_SNDBUF_LENGTH)
            {
                System.err.format(
                    "WARNING: Could not get desired SO_SNDBUF, adjust OS buffer to match %s: attempted=%d, actual=%d%n",
                    SOCKET_SNDBUF_LENGTH_PROP_NAME,
                    SOCKET_SNDBUF_LENGTH,
                    maxSoSndBuf);
            }

            probe.setOption(StandardSocketOptions.SO_RCVBUF, Integer.MAX_VALUE);
            final int maxSoRcvBuf = probe.getOption(StandardSocketOptions.SO_RCVBUF);

            if (maxSoRcvBuf < SOCKET_RCVBUF_LENGTH)
            {
                System.err.format(
                    "WARNING: Could not get desired SO_RCVBUF, adjust OS buffer to match %s: attempted=%d, actual=%d%n",
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
        }
        catch (final IOException ex)
        {
            throw new RuntimeException("probe socket: " + ex.toString(), ex);
        }
    }

    private static void ensureDirectoryIsRecreated(final Context ctx)
    {
        if (ctx.aeronDirectory().isDirectory())
        {
            if (ctx.warnIfDirectoriesExist())
            {
                System.err.println("WARNING: " + ctx.aeronDirectory() + " already exists.");
            }

            if (!ctx.dirsDeleteOnStart())
            {
                final Consumer<String> logProgress = ctx.warnIfDirectoriesExist() ? System.err::println : (s) -> {};
                final MappedByteBuffer cncByteBuffer = ctx.mapExistingCncFile();
                try
                {
                    if (ctx.isDriverActive(ctx.driverTimeoutMs(), logProgress, cncByteBuffer))
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

    public static class Context extends CommonContext
    {
        private RawLogFactory rawLogFactory;
        private DataTransportPoller dataTransportPoller;
        private ControlTransportPoller controlTransportPoller;
        private FlowControlSupplier unicastFlowControlSupplier;
        private FlowControlSupplier multicastFlowControlSupplier;
        private EpochClock epochClock;
        private NanoClock nanoClock;
        private OneToOneConcurrentArrayQueue<DriverConductorCmd> toConductorFromReceiverCommandQueue;
        private OneToOneConcurrentArrayQueue<DriverConductorCmd> toConductorFromSenderCommandQueue;
        private OneToOneConcurrentArrayQueue<ReceiverCmd> receiverCommandQueue;
        private OneToOneConcurrentArrayQueue<SenderCmd> senderCommandQueue;
        private ReceiverProxy receiverProxy;
        private SenderProxy senderProxy;
        private DriverConductorProxy fromReceiverDriverConductorProxy;
        private DriverConductorProxy fromSenderDriverConductorProxy;
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

        private CountersManager countersManager;
        private SystemCounters systemCounters;

        private long imageLivenessTimeoutNs = Configuration.IMAGE_LIVENESS_TIMEOUT_NS;
        private long clientLivenessTimeoutNs = Configuration.CLIENT_LIVENESS_TIMEOUT_NS;
        private long publicationUnblockTimeoutNs = Configuration.PUBLICATION_UNBLOCK_TIMEOUT_NS;

        private Boolean termBufferSparseFile;
        private int publicationTermBufferLength;
        private int ipcPublicationTermBufferLength;
        private int maxTermBufferLength;
        private int initialWindowLength;
        private long statusMessageTimeout;
        private int mtuLength;

        private boolean warnIfDirectoriesExist;
        private boolean dirsDeleteOnStart;
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

        public Context()
        {
            publicationTermBufferLength(Configuration.termBufferLength());
            maxTermBufferLength(Configuration.maxTermBufferLength());
            initialWindowLength(Configuration.initialWindowLength());
            statusMessageTimeout(Configuration.statusMessageTimeout());
            mtuLength(Configuration.MTU_LENGTH);

            warnIfDirectoriesExist = true;

            dirsDeleteOnStart(getBoolean(DIRS_DELETE_ON_START_PROP_NAME));
        }

        public Context conclude()
        {
            super.conclude();

            try
            {
                concludeNullProperties();

                Configuration.validateTermBufferLength(maxTermBufferLength);
                Configuration.validateTermBufferLength(publicationTermBufferLength);
                Configuration.validateTermBufferLength(ipcPublicationTermBufferLength);

                if (publicationTermBufferLength > maxTermBufferLength)
                {
                    throw new ConfigurationException(String.format(
                        "publication term buffer length %d greater than max length %d",
                        publicationTermBufferLength, maxTermBufferLength));
                }

                if (ipcPublicationTermBufferLength > maxTermBufferLength)
                {
                    throw new ConfigurationException(String.format(
                        "IPC publication term buffer length %d greater than max length %d",
                        ipcPublicationTermBufferLength, maxTermBufferLength));
                }

                Configuration.validateInitialWindowLength(initialWindowLength(), mtuLength());

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

                final BroadcastTransmitter transmitter =
                    new BroadcastTransmitter(createToClientsBuffer(cncByteBuffer, cncMetaDataBuffer));
                clientProxy(new ClientProxy(transmitter));

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
                fromReceiverDriverConductorProxy(new DriverConductorProxy(
                    threadingMode, toConductorFromReceiverCommandQueue, systemCounters.get(CONDUCTOR_PROXY_FAILS)));
                fromSenderDriverConductorProxy(new DriverConductorProxy(
                    threadingMode, toConductorFromSenderCommandQueue, systemCounters.get(CONDUCTOR_PROXY_FAILS)));

                rawLogBuffersFactory(new RawLogFactory(
                    aeronDirectoryName(),
                    maxTermBufferLength,
                    termBufferSparseFile,
                    errorLog));

                if (null == lossReport)
                {
                    lossReportBuffer = mapLossReport(aeronDirectoryName(), Configuration.LOSS_REPORT_BUFFER_LENGTH);
                    lossReport = new LossReport(new UnsafeBuffer(lossReportBuffer));
                }

                concludeIdleStrategies();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return this;
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

            if (0 == ipcPublicationTermBufferLength)
            {
                ipcPublicationTermBufferLength = Configuration.ipcTermBufferLength(publicationTermBufferLength());
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

            if (null == termBufferSparseFile)
            {
                if (null != Configuration.TERM_BUFFER_SPARSE_FILE)
                {
                    termBufferSparseFile = Boolean.valueOf(Configuration.TERM_BUFFER_SPARSE_FILE);
                }
                else
                {
                    termBufferSparseFile = Boolean.FALSE;
                }
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

        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        public Context nanoClock(final NanoClock clock)
        {
            this.nanoClock = clock;
            return this;
        }

        public Context toConductorFromReceiverCommandQueue(final OneToOneConcurrentArrayQueue<DriverConductorCmd> queue)
        {
            this.toConductorFromReceiverCommandQueue = queue;
            return this;
        }

        public Context toConductorFromSenderCommandQueue(final OneToOneConcurrentArrayQueue<DriverConductorCmd> queue)
        {
            this.toConductorFromSenderCommandQueue = queue;
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

        public Context fromReceiverDriverConductorProxy(final DriverConductorProxy driverConductorProxy)
        {
            this.fromReceiverDriverConductorProxy = driverConductorProxy;
            return this;
        }

        public Context fromSenderDriverConductorProxy(final DriverConductorProxy driverConductorProxy)
        {
            this.fromSenderDriverConductorProxy = driverConductorProxy;
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

        public Context termBufferSparseFile(final Boolean termBufferSparseFile)
        {
            this.termBufferSparseFile = termBufferSparseFile;
            return this;
        }

        public Context publicationTermBufferLength(final int termBufferLength)
        {
            this.publicationTermBufferLength = termBufferLength;
            return this;
        }

        public Context maxTermBufferLength(final int maxTermBufferLength)
        {
            this.maxTermBufferLength = maxTermBufferLength;
            return this;
        }

        public Context ipcTermBufferLength(final int ipcTermBufferLength)
        {
            this.ipcPublicationTermBufferLength = ipcTermBufferLength;
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

        public Context imageLivenessTimeoutNs(final long timeout)
        {
            this.imageLivenessTimeoutNs = timeout;
            return this;
        }

        public Context clientLivenessTimeoutNs(final long timeout)
        {
            this.clientLivenessTimeoutNs = timeout;
            return this;
        }

        public Context publicationUnblockTimeoutNs(final long timeout)
        {
            this.publicationUnblockTimeoutNs = timeout;
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
         * Set whether or not this application will attempt to delete the Aeron directories when starting.
         *
         * @param dirsDeleteOnStart Attempt deletion.
         * @return this Object for method chaining.
         */
        public Context dirsDeleteOnStart(final boolean dirsDeleteOnStart)
        {
            this.dirsDeleteOnStart = dirsDeleteOnStart;
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

        public EpochClock epochClock()
        {
            return epochClock;
        }

        public NanoClock nanoClock()
        {
            return nanoClock;
        }

        public OneToOneConcurrentArrayQueue<DriverConductorCmd> toConductorFromReceiverCommandQueue()
        {
            return toConductorFromReceiverCommandQueue;
        }

        public OneToOneConcurrentArrayQueue<DriverConductorCmd> toConductorFromSenderCommandQueue()
        {
            return toConductorFromSenderCommandQueue;
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

        public DriverConductorProxy fromReceiverDriverConductorProxy()
        {
            return fromReceiverDriverConductorProxy;
        }

        public DriverConductorProxy fromSenderDriverConductorProxy()
        {
            return fromSenderDriverConductorProxy;
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

        public CountersManager countersManager()
        {
            return countersManager;
        }

        public long imageLivenessTimeoutNs()
        {
            return imageLivenessTimeoutNs;
        }

        public long clientLivenessTimeoutNs()
        {
            return clientLivenessTimeoutNs;
        }

        public long publicationUnblockTimeoutNs()
        {
            return publicationUnblockTimeoutNs;
        }

        public int publicationTermBufferLength()
        {
            return publicationTermBufferLength;
        }

        public int maxTermBufferLength()
        {
            return maxTermBufferLength;
        }

        public int ipcTermBufferLength()
        {
            return ipcPublicationTermBufferLength;
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

        public int mtuLength()
        {
            return mtuLength;
        }

        public CommonContext mtuLength(final int mtuLength)
        {
            Configuration.validateMtuLength(mtuLength);
            this.mtuLength = mtuLength;
            return this;
        }

        public SystemCounters systemCounters()
        {
            return systemCounters;
        }

        /**
         * Get whether or not this application will attempt to delete the Aeron directories when starting.
         *
         * @return true when directories will be deleted, otherwise false.
         */
        public boolean dirsDeleteOnStart()
        {
            return dirsDeleteOnStart;
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

        public void close()
        {
            // do not close the systemsCounters so that all counters are kept as is.
            IoUtil.unmap(cncByteBuffer);
            IoUtil.unmap(lossReportBuffer);

            super.close();
        }

        private void concludeCounters()
        {
            if (countersManager() == null)
            {
                if (countersMetaDataBuffer() == null)
                {
                    countersMetaDataBuffer(createCountersMetaDataBuffer(cncByteBuffer, cncMetaDataBuffer));
                }

                if (countersValuesBuffer() == null)
                {
                    countersValuesBuffer(createCountersValuesBuffer(cncByteBuffer, cncMetaDataBuffer));
                }

                countersManager(new CountersManager(countersMetaDataBuffer(), countersValuesBuffer()));
            }

            if (null == systemCounters)
            {
                systemCounters = new SystemCounters(countersManager);
            }
        }

        private void concludeIdleStrategies()
        {
            final StatusIndicator controllableIdleStrategyStatus =
                new UnsafeBufferStatusIndicator(countersManager.valuesBuffer(), CONTROLLABLE_IDLE_STRATEGY.id());

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
