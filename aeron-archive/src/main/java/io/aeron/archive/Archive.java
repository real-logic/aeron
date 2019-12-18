/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthenticatorSupplier;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.LoggingErrorHandler;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.StatusIndicator;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Supplier;

import static io.aeron.archive.ArchiveThreadingMode.DEDICATED;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static java.lang.System.getProperty;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static org.agrona.SystemUtil.*;

/**
 * The Aeron Archive which allows for the recording and replay of local and remote {@link io.aeron.Publication}s .
 */
public class Archive implements AutoCloseable
{
    private final Context ctx;
    private final AgentRunner conductorRunner;
    private final AgentInvoker conductorInvoker;

    Archive(final Context ctx)
    {
        this.ctx = ctx;
        ctx.conclude();

        final ArchiveConductor conductor = DEDICATED == ctx.threadingMode() ?
            new DedicatedModeArchiveConductor(ctx) :
            new SharedModeArchiveConductor(ctx);

        if (ArchiveThreadingMode.INVOKER == ctx.threadingMode())
        {
            conductorInvoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), conductor);
            conductorRunner = null;
        }
        else
        {
            conductorInvoker = null;
            conductorRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), ctx.errorCounter(), conductor);
        }
    }

    /**
     * Launch an {@link Archive} with that communicates with an out of process {@link io.aeron.driver.MediaDriver}
     * and await a shutdown signal.
     *
     * @param args command line argument which is a list for properties files as URLs or filenames.
     */
    public static void main(final String[] args)
    {
        loadPropertiesFiles(args);

        try (Archive ignore = launch())
        {
            new ShutdownSignalBarrier().await();

            System.out.println("Shutdown Archive...");
        }
    }

    /**
     * Get the {@link Archive.Context} that is used by this {@link Archive}.
     *
     * @return the {@link Archive.Context} that is used by this {@link Archive}.
     */
    public Context context()
    {
        return ctx;
    }

    public void close()
    {
        CloseHelper.close(conductorInvoker);
        CloseHelper.close(conductorRunner);
    }

    private Archive start()
    {
        if (ArchiveThreadingMode.INVOKER == ctx.threadingMode())
        {
            conductorInvoker.start();
        }
        else
        {
            AgentRunner.startOnThread(conductorRunner, ctx.threadFactory());
        }

        return this;
    }

    /**
     * Get the {@link AgentInvoker} for the archive if it is running in {@link ArchiveThreadingMode#INVOKER}.
     *
     * @return the {@link AgentInvoker} for the archive if it is running in {@link ArchiveThreadingMode#INVOKER}
     * otherwise null.
     */
    public AgentInvoker invoker()
    {
        return conductorInvoker;
    }

    /**
     * Launch an Archive using a default configuration.
     *
     * @return a new instance of an Archive.
     */
    public static Archive launch()
    {
        return launch(new Context());
    }

    /**
     * Launch an Archive by providing a configuration context.
     *
     * @param ctx for the configuration parameters.
     * @return a new instance of an Archive.
     */
    public static Archive launch(final Context ctx)
    {
        return new Archive(ctx).start();
    }

    /**
     * Configuration for system properties and defaults.
     * <p>
     * Details for the individual parameters can be found in the Javadoc for the {@link Context} setters.
     */
    public static class Configuration
    {
        /**
         * Filename for the single instance of a {@link Catalog} contents for an archive.
         */
        static final String CATALOG_FILE_NAME = "archive.catalog";

        /**
         * Recording segment file suffix extension.
         */
        static final String RECORDING_SEGMENT_SUFFIX = ".rec";

        /**
         * Maximum block length of data read from disk in a single operation during a replay.
         */
        static final int MAX_BLOCK_LENGTH = 2 * 1024 * 1024;

        /**
         * Directory in which the archive stores it files such as the catalog and recordings.
         */
        public static final String ARCHIVE_DIR_PROP_NAME = "aeron.archive.dir";

        /**
         * Default directory for the archive files.
         * @see #ARCHIVE_DIR_PROP_NAME
         */
        public static final String ARCHIVE_DIR_DEFAULT = "aeron-archive";

        /**
         * Recordings will be segmented on disk in files limited to the segment length which must be a multiple of
         * the term length for each stream. For lots of small recording this value may be reduced.
         */
        public static final String SEGMENT_FILE_LENGTH_PROP_NAME = "aeron.archive.segment.file.length";

        /**
         * Default segment file length which is multiple of terms.
         * @see #SEGMENT_FILE_LENGTH_PROP_NAME
         */
        public static final int SEGMENT_FILE_LENGTH_DEFAULT = 128 * 1024 * 1024;

        /**
         * The level at which recording files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         */
        public static final String FILE_SYNC_LEVEL_PROP_NAME = "aeron.archive.file.sync.level";

        /**
         * Default is to use normal file writes which may mean some data loss in the event of a power failure.
         * @see #FILE_SYNC_LEVEL_PROP_NAME
         */
        public static final int FILE_SYNC_LEVEL_DEFAULT = 0;

        /**
         * The level at which catalog updates and directory should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         */
        public static final String CATALOG_FILE_SYNC_LEVEL_PROP_NAME = "aeron.archive.catalog.file.sync.level";

        /**
         * Default is to use normal file writes which may mean some data loss in the event of a power failure.
         * @see #CATALOG_FILE_SYNC_LEVEL_PROP_NAME
         */
        public static final int CATALOG_FILE_SYNC_LEVEL_DEFAULT = FILE_SYNC_LEVEL_DEFAULT;

        /**
         * What {@link ArchiveThreadingMode} should be used.
         */
        public static final String THREADING_MODE_PROP_NAME = "aeron.archive.threading.mode";

        /**
         * Default {@link IdleStrategy} to be used for the archive {@link Agent}s when not busy.
         */
        public static final String ARCHIVE_IDLE_STRATEGY_PROP_NAME = "aeron.archive.idle.strategy";

        /**
         * The {@link IdleStrategy} to be used for the archive recorder {@link Agent} when not busy.
         */
        public static final String ARCHIVE_RECORDER_IDLE_STRATEGY_PROP_NAME = "aeron.archive.recorder.idle.strategy";

        /**
         * The {@link IdleStrategy} to be used for the archive replayer {@link Agent} when not busy.
         */
        public static final String ARCHIVE_REPLAYER_IDLE_STRATEGY_PROP_NAME = "aeron.archive.replayer.idle.strategy";

        /**
         * Default {@link IdleStrategy} to be used for the archive {@link Agent}s when not busy.
         * @see #ARCHIVE_IDLE_STRATEGY_PROP_NAME
         */
        public static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";

        /**
         * Maximum number of concurrent recordings which can be active at a time. Going beyond this number will
         * result in an exception and further recordings will be rejected. Since wildcard subscriptions can have
         * multiple images, and thus multiple recordings, then the limit may come later. It is best to
         * use session based subscriptions.
         */
        public static final String MAX_CONCURRENT_RECORDINGS_PROP_NAME = "aeron.archive.max.concurrent.recordings";

        /**
         * Default maximum number of concurrent recordings. Unless on a very fast SSD and having sufficient memory
         * for the page cache then this number should be kept low, especially when sync'ing writes.
         * @see #MAX_CONCURRENT_RECORDINGS_PROP_NAME
         */
        public static final int MAX_CONCURRENT_RECORDINGS_DEFAULT = 20;

        /**
         * Maximum number of concurrent replays. Beyond this maximum an exception will be raised and further replays
         * will be rejected.
         */
        public static final String MAX_CONCURRENT_REPLAYS_PROP_NAME = "aeron.archive.max.concurrent.replays";

        /**
         * Default maximum number of concurrent replays. Unless on a fast SSD and having sufficient memory
         * for the page cache then this number should be kept low.
         */
        public static final int MAX_CONCURRENT_REPLAYS_DEFAULT = 20;

        /**
         * Maximum number of entries for the archive {@link Catalog}. Increasing this limit will require use of the
         * {@link CatalogTool}. The number of entries can be reduced by extending existing recordings rather than
         * creating new ones.
         */
        public static final String MAX_CATALOG_ENTRIES_PROP_NAME = "aeron.archive.max.catalog.entries";

        /**
         * Default limit for the entries in the {@link Catalog}
         * @see #MAX_CATALOG_ENTRIES_PROP_NAME
         */
        public static final long MAX_CATALOG_ENTRIES_DEFAULT = Catalog.DEFAULT_MAX_ENTRIES;

        /**
         * Timeout for making a connection back to a client for a control session or replay.
         */
        public static final String CONNECT_TIMEOUT_PROP_NAME = "aeron.archive.connect.timeout";

        /**
         * Default timeout for connecting back to a client for a control session or replay. You may want to
         * increase this on higher latency networks.
         * @see #CONNECT_TIMEOUT_PROP_NAME
         */
        public static final long CONNECT_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

        /**
         * How long a replay publication should linger after all data is sent. Longer linger can help avoid tail loss.
         */
        public static final String REPLAY_LINGER_TIMEOUT_PROP_NAME = "aeron.archive.replay.linger.timeout";

        /**
         * Default for long to linger a replay connection which defaults to
         * {@link io.aeron.driver.Configuration#publicationLingerTimeoutNs()}.
         * @see #REPLAY_LINGER_TIMEOUT_PROP_NAME
         */
        public static final long REPLAY_LINGER_TIMEOUT_DEFAULT_NS =
            io.aeron.driver.Configuration.publicationLingerTimeoutNs();

        /**
         * Should the archive delete existing files on start. Default is false and should only be true for testing.
         */
        public static final String ARCHIVE_DIR_DELETE_ON_START_PROP_NAME = "aeron.archive.dir.delete.on.start";

        /**
         * Channel for receiving replication streams replayed from another archive.
         */
        public static final String REPLICATION_CHANNEL_PROP_NAME = "aeron.archive.replication.channel";

        /**
         * Channel for receiving replication streams replayed from another archive.
         * @see #REPLICATION_CHANNEL_PROP_NAME
         */
        public static final String REPLICATION_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8040";

        /**
         * Name of class to use as a supplier of {@link Authenticator} for the archive.
         */
        public static final String AUTHENTICATOR_SUPPLIER_PROP_NAME = "aeron.archive.authenticator.supplier";

        /**
         * Name of the class to use as a supplier of {@link Authenticator} for the archive. Default is
         * a non-authenticating option.
         */
        public static final String AUTHENTICATOR_SUPPLIER_DEFAULT = "io.aeron.security.DefaultAuthenticatorSupplier";

        /**
         * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
         */
        public static final int ARCHIVE_ERROR_COUNT_TYPE_ID = 101;

        /**
         * Size in bytes of the error buffer for the archive when not externally provided.
         */
        public static final String ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.archive.error.buffer.length";

        /**
         * Size in bytes of the error buffer for the archive when not eternally provided.
         */
        public static final int ERROR_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

        /**
         * Should the CRC be computed for each recorder fragment.
         */
        public static final String RECORDING_CRC_ENABLED_PROP_NAME = "aeron.archive.recording.crc.enabled";

        /**
         * Do not compute the CRC for each recorded fragment by default.
         */
        public static final boolean RECORDING_CRC_ENABLED_DEFAULT = false;

        /**
         * Should the CRC be validated during replay.
         */
        public static final String REPLAY_CRC_ENABLED_PROP_NAME = "aeron.archive.replay.crc.enabled";

        /**
         * Do not validate the CRC for each replayed fragment by default.
         */
        public static final boolean REPLAY_CRC_ENABLED_DEFAULT = false;

        /**
         * Get the directory name to be used for storing the archive.
         *
         * @return the directory name to be used for storing the archive.
         */
        public static String archiveDirName()
        {
            return System.getProperty(ARCHIVE_DIR_PROP_NAME, ARCHIVE_DIR_DEFAULT);
        }

        /**
         * The length of file to be used for storing recording segments that must be a power of 2.
         * <p>
         * If the {@link Image#termBufferLength()} is greater then this will take priority.
         *
         * @return length of file to be used for storing recording segments.
         */
        public static int segmentFileLength()
        {
            return getSizeAsInt(SEGMENT_FILE_LENGTH_PROP_NAME, SEGMENT_FILE_LENGTH_DEFAULT);
        }

        /**
         * The level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return level at which files should be sync'ed to disk.
         * @see #FILE_SYNC_LEVEL_PROP_NAME
         */
        public static int fileSyncLevel()
        {
            return Integer.getInteger(FILE_SYNC_LEVEL_PROP_NAME, FILE_SYNC_LEVEL_DEFAULT);
        }

        /**
         * The level at which the catalog file and directory should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return level at which files should be sync'ed to disk.
         * @see #CATALOG_FILE_SYNC_LEVEL_PROP_NAME
         */
        public static int catalogFileSyncLevel()
        {
            return Integer.getInteger(CATALOG_FILE_SYNC_LEVEL_PROP_NAME, CATALOG_FILE_SYNC_LEVEL_DEFAULT);
        }

        /**
         * The threading mode to be employed by the archive.
         *
         * @return the threading mode to be employed by the archive.
         */
        public static ArchiveThreadingMode threadingMode()
        {
            return ArchiveThreadingMode.valueOf(System.getProperty(THREADING_MODE_PROP_NAME, DEDICATED.name()));
        }

        /**
         * Create a supplier of {@link IdleStrategy}s for the {@link #ARCHIVE_IDLE_STRATEGY_PROP_NAME}
         * system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy {@link Supplier}.
         */
        public static Supplier<IdleStrategy> idleStrategySupplier(final StatusIndicator controllableStatus)
        {
            return () ->
            {
                final String name = System.getProperty(ARCHIVE_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);
                return io.aeron.driver.Configuration.agentIdleStrategy(name, controllableStatus);
            };
        }

        /**
         * Create a supplier of {@link IdleStrategy}s for the {@link #ARCHIVE_RECORDER_IDLE_STRATEGY_PROP_NAME}
         * system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy {@link Supplier}.
         */
        public static Supplier<IdleStrategy> recorderIdleStrategySupplier(final StatusIndicator controllableStatus)
        {
            final String name = System.getProperty(ARCHIVE_RECORDER_IDLE_STRATEGY_PROP_NAME);
            if (null == name)
            {
                return null;
            }

            return () -> io.aeron.driver.Configuration.agentIdleStrategy(name, controllableStatus);
        }

        /**
         * Create a supplier of {@link IdleStrategy}s for the {@link #ARCHIVE_REPLAYER_IDLE_STRATEGY_PROP_NAME}
         * system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy {@link Supplier}.
         */
        public static Supplier<IdleStrategy> replayerIdleStrategySupplier(final StatusIndicator controllableStatus)
        {
            final String name = System.getProperty(ARCHIVE_REPLAYER_IDLE_STRATEGY_PROP_NAME);
            if (null == name)
            {
                return null;
            }

            return () -> io.aeron.driver.Configuration.agentIdleStrategy(name, controllableStatus);
        }

        /**
         * The maximum number of recordings that can operate concurrently after which new requests will be rejected.
         *
         * @return the maximum number of recordings that can operate concurrently.
         */
        public static int maxConcurrentRecordings()
        {
            return Integer.getInteger(MAX_CONCURRENT_RECORDINGS_PROP_NAME, MAX_CONCURRENT_RECORDINGS_DEFAULT);
        }

        /**
         * The maximum number of replays that can operate concurrently after which new requests will be rejected.
         *
         * @return the maximum number of replays that can operate concurrently.
         */
        public static int maxConcurrentReplays()
        {
            return Integer.getInteger(MAX_CONCURRENT_REPLAYS_PROP_NAME, MAX_CONCURRENT_REPLAYS_DEFAULT);
        }

        /**
         * Maximum number of catalog entries to allocate for the catalog file.
         *
         * @return the maximum number of catalog entries to support for the catalog file.
         */
        public static long maxCatalogEntries()
        {
            return Long.getLong(MAX_CATALOG_ENTRIES_PROP_NAME, MAX_CATALOG_ENTRIES_DEFAULT);
        }

        /**
         * The timeout in nanoseconds to wait for a connection.
         *
         * @return timeout in nanoseconds to wait for a connection.
         * @see #CONNECT_TIMEOUT_PROP_NAME
         */
        public static long connectTimeoutNs()
        {
            return getDurationInNanos(CONNECT_TIMEOUT_PROP_NAME, CONNECT_TIMEOUT_DEFAULT_NS);
        }

        /**
         * The timeout in nanoseconds to for a replay network publication to linger after draining.
         *
         * @return timeout in nanoseconds for a replay network publication to wait in linger.
         * @see #REPLAY_LINGER_TIMEOUT_PROP_NAME
         * @see io.aeron.driver.Configuration#PUBLICATION_LINGER_PROP_NAME
         */
        public static long replayLingerTimeoutNs()
        {
            return getDurationInNanos(REPLAY_LINGER_TIMEOUT_PROP_NAME, REPLAY_LINGER_TIMEOUT_DEFAULT_NS);
        }

        /**
         * Whether to delete directory on start or not.
         *
         * @return whether to delete directory on start or not.
         * @see #ARCHIVE_DIR_DELETE_ON_START_PROP_NAME
         */
        public static boolean deleteArchiveOnStart()
        {
            return "true".equalsIgnoreCase(getProperty(ARCHIVE_DIR_DELETE_ON_START_PROP_NAME, "false"));
        }

        /**
         * The value {@link #REPLICATION_CHANNEL_DEFAULT} or system property
         * {@link #REPLICATION_CHANNEL_PROP_NAME} if set.
         *
         * @return {@link #REPLICATION_CHANNEL_DEFAULT} or system property
         * {@link #REPLICATION_CHANNEL_PROP_NAME} if set.
         */
        public static String replicationChannel()
        {
            return System.getProperty(REPLICATION_CHANNEL_PROP_NAME, REPLICATION_CHANNEL_DEFAULT);
        }

        /**
         * Size in bytes of the error buffer in the mark file.
         *
         * @return length of error buffer in bytes.
         * @see #ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public static int errorBufferLength()
        {
            return getSizeAsInt(ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT);
        }

        /**
         * The value {@link #AUTHENTICATOR_SUPPLIER_DEFAULT} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         *
         * @return {@link #AUTHENTICATOR_SUPPLIER_DEFAULT} or system property
         * {@link #AUTHENTICATOR_SUPPLIER_PROP_NAME} if set.
         */
        public static AuthenticatorSupplier authenticatorSupplier()
        {
            final String supplierClassName = System.getProperty(
                AUTHENTICATOR_SUPPLIER_PROP_NAME, AUTHENTICATOR_SUPPLIER_DEFAULT);

            AuthenticatorSupplier supplier = null;
            try
            {
                supplier = (AuthenticatorSupplier)Class.forName(supplierClassName).getConstructor().newInstance();
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return supplier;
        }

        /**
         * Should the CRC be computed for each recorder fragment.
         *
         * @return {@code true} if the CRC computation should be enabled.
         * @see Configuration#RECORDING_CRC_ENABLED_DEFAULT
         */
        public static boolean recordingCrcEnabled()
        {
            final String propValue = getProperty(RECORDING_CRC_ENABLED_PROP_NAME);
            return null != propValue ? "true".equals(propValue) : RECORDING_CRC_ENABLED_DEFAULT;
        }

        /**
         * Should the CRC be validated during replay.
         *
         * @return {@code true} if the CRC validation should be done during replay.
         * @see Configuration#REPLAY_CRC_ENABLED_DEFAULT
         */
        public static boolean replayCrcEnabled()
        {
            final String propValue = getProperty(REPLAY_CRC_ENABLED_PROP_NAME);
            return null != propValue ? "true".equals(propValue) : REPLAY_CRC_ENABLED_DEFAULT;
        }
    }

    /**
     * Overrides for the defaults and system properties.
     * <p>
     * The context will be owned by {@link ArchiveConductor} after a successful
     * {@link Archive#launch(Context)} and closed via {@link Archive#close()}.
     */
    public static class Context implements Cloneable
    {
        /**
         * Using an integer because there is no support for boolean. 1 is concluded, 0 is not concluded.
         */
        private static final AtomicIntegerFieldUpdater<Context> IS_CONCLUDED_UPDATER = newUpdater(
            Context.class, "isConcluded");
        private volatile int isConcluded;

        private boolean deleteArchiveOnStart = Configuration.deleteArchiveOnStart();
        private boolean ownsAeronClient = false;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;
        private File archiveDir;
        private String archiveDirectoryName = Configuration.archiveDirName();
        private FileChannel archiveDirChannel;
        private Catalog catalog;
        private ArchiveMarkFile markFile;
        private AeronArchive.Context archiveClientContext;

        private String controlChannel = AeronArchive.Configuration.controlChannel();
        private int controlStreamId = AeronArchive.Configuration.controlStreamId();
        private String localControlChannel = AeronArchive.Configuration.localControlChannel();
        private int localControlStreamId = AeronArchive.Configuration.localControlStreamId();
        private boolean controlTermBufferSparse = AeronArchive.Configuration.controlTermBufferSparse();
        private int controlTermBufferLength = AeronArchive.Configuration.controlTermBufferLength();
        private int controlMtuLength = AeronArchive.Configuration.controlMtuLength();
        private String recordingEventsChannel = AeronArchive.Configuration.recordingEventsChannel();
        private int recordingEventsStreamId = AeronArchive.Configuration.recordingEventsStreamId();
        private boolean recordingEventsEnabled = AeronArchive.Configuration.recordingEventsEnabled();
        private String replicationChannel = Configuration.replicationChannel();

        private long connectTimeoutNs = Configuration.connectTimeoutNs();
        private long replayLingerTimeoutNs = Configuration.replayLingerTimeoutNs();
        private long maxCatalogEntries = Configuration.maxCatalogEntries();
        private int segmentFileLength = Configuration.segmentFileLength();
        private int fileSyncLevel = Configuration.fileSyncLevel();
        private int catalogFileSyncLevel = Configuration.catalogFileSyncLevel();

        private ArchiveThreadingMode threadingMode = Configuration.threadingMode();
        private ThreadFactory threadFactory;
        private CountDownLatch abortLatch;

        private Supplier<IdleStrategy> idleStrategySupplier;
        private Supplier<IdleStrategy> replayerIdleStrategySupplier;
        private Supplier<IdleStrategy> recorderIdleStrategySupplier;
        private EpochClock epochClock;
        private AuthenticatorSupplier authenticatorSupplier;

        private int errorBufferLength = 0;
        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;

        private AgentInvoker mediaDriverAgentInvoker;
        private int maxConcurrentRecordings = Configuration.maxConcurrentRecordings();
        private int maxConcurrentReplays = Configuration.maxConcurrentReplays();

        private boolean recordingCrcEnabled = Configuration.recordingCrcEnabled();
        private boolean replayCrcEnabled = Configuration.replayCrcEnabled();

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            try
            {
                return (Context)super.clone();
            }
            catch (final CloneNotSupportedException ex)
            {
                throw new RuntimeException(ex);
            }
        }

        /**
         * Conclude the configuration parameters by resolving dependencies and null values to use defaults.
         */
        @SuppressWarnings("MethodLength")
        public void conclude()
        {
            if (0 != IS_CONCLUDED_UPDATER.getAndSet(this, 1))
            {
                throw new ConcurrentConcludeException();
            }

            if (catalogFileSyncLevel < fileSyncLevel)
            {
                throw new ConfigurationException(
                    "catalogFileSyncLevel " + catalogFileSyncLevel + " < fileSyncLevel " + fileSyncLevel);
            }

            if (null == archiveDir)
            {
                archiveDir = new File(archiveDirectoryName);
            }

            if (deleteArchiveOnStart && archiveDir.exists())
            {
                IoUtil.delete(archiveDir, false);
            }

            if (!archiveDir.exists() && !archiveDir.mkdirs())
            {
                throw new ArchiveException(
                    "failed to create archive dir: " + archiveDir.getAbsolutePath());
            }

            archiveDirChannel = channelForDirectorySync(archiveDir, catalogFileSyncLevel);

            if (null == epochClock)
            {
                epochClock = SystemEpochClock.INSTANCE;
            }

            if (null != aeron)
            {
                aeronDirectoryName = aeron.context().aeronDirectoryName();
            }

            if (null == markFile)
            {
                if (0 == errorBufferLength && null == errorHandler)
                {
                    errorBufferLength = Configuration.errorBufferLength();
                }

                markFile = new ArchiveMarkFile(this);
            }

            if (null == errorHandler)
            {
                errorHandler = new LoggingErrorHandler(new DistinctErrorLog(markFile.errorBuffer(), epochClock));
            }

            if (null == aeron)
            {
                ownsAeronClient = true;

                aeron = Aeron.connect(
                    new Aeron.Context()
                        .aeronDirectoryName(aeronDirectoryName)
                        .errorHandler(errorHandler)
                        .epochClock(epochClock)
                        .driverAgentInvoker(mediaDriverAgentInvoker)
                        .useConductorAgentInvoker(true)
                        .awaitingIdleStrategy(YieldingIdleStrategy.INSTANCE)
                        .clientLock(NoOpLock.INSTANCE));

                if (null == errorCounter)
                {
                    errorCounter = aeron.addCounter(Configuration.ARCHIVE_ERROR_COUNT_TYPE_ID, "Archive errors");
                }
            }

            Objects.requireNonNull(errorCounter, "Error counter must be supplied if aeron client is");

            if (null == countedErrorHandler)
            {
                countedErrorHandler = new CountedErrorHandler(errorHandler, errorCounter);
                if (ownsAeronClient)
                {
                    aeron.context().errorHandler(countedErrorHandler);
                }
            }

            if (null == threadFactory)
            {
                threadFactory = Thread::new;
            }

            if (null == idleStrategySupplier)
            {
                idleStrategySupplier = Configuration.idleStrategySupplier(null);
            }

            if (DEDICATED == threadingMode)
            {
                if (null == recorderIdleStrategySupplier)
                {
                    recorderIdleStrategySupplier = Configuration.recorderIdleStrategySupplier(null);
                    if (null == recorderIdleStrategySupplier)
                    {
                        recorderIdleStrategySupplier = idleStrategySupplier;
                    }
                }

                if (null == replayerIdleStrategySupplier)
                {
                    replayerIdleStrategySupplier = Configuration.replayerIdleStrategySupplier(null);
                    if (null == replayerIdleStrategySupplier)
                    {
                        replayerIdleStrategySupplier = idleStrategySupplier;
                    }
                }
            }

            if (!BitUtil.isPowerOfTwo(segmentFileLength))
            {
                throw new ArchiveException("segment file length not a power of 2: " + segmentFileLength);
            }
            else if (segmentFileLength < TERM_MIN_LENGTH || segmentFileLength > TERM_MAX_LENGTH)
            {
                throw new ArchiveException("segment file length not in valid range: " + segmentFileLength);
            }

            if (null == authenticatorSupplier)
            {
                authenticatorSupplier = Configuration.authenticatorSupplier();
            }

            if (null == catalog)
            {
                catalog = new Catalog(
                    archiveDir, archiveDirChannel, catalogFileSyncLevel, maxCatalogEntries, epochClock);
            }

            if (null == archiveClientContext)
            {
                archiveClientContext = new AeronArchive.Context();
            }

            archiveClientContext.aeron(aeron).lock(NoOpLock.INSTANCE).errorHandler(errorHandler);

            int expectedCount = DEDICATED == threadingMode ? 2 : 0;
            expectedCount += aeron.conductorAgentInvoker() == null ? 1 : 0;
            abortLatch = new CountDownLatch(expectedCount);

            markFile.signalReady();
        }

        /**
         * Should an existing archive be deleted on start. Useful only for testing.
         *
         * @param deleteArchiveOnStart true if an existing archive should be deleted on startup.
         * @return this for a fluent API.
         */
        public Context deleteArchiveOnStart(final boolean deleteArchiveOnStart)
        {
            this.deleteArchiveOnStart = deleteArchiveOnStart;
            return this;
        }

        /**
         * Should an existing archive be deleted on start. Useful only for testing.
         *
         * @return true if an existing archive should be deleted on start up.
         */
        public boolean deleteArchiveOnStart()
        {
            return deleteArchiveOnStart;
        }

        /**
         * Set the directory name to be used for the archive to store recordings and the {@link Catalog}.
         * This name is used if {@link #archiveDir(File)} is not set.
         *
         * @param archiveDirectoryName to store recordings and the {@link Catalog}.
         * @return this for a fluent API.
         * @see Configuration#ARCHIVE_DIR_PROP_NAME
         */
        public Context archiveDirectoryName(final String archiveDirectoryName)
        {
            this.archiveDirectoryName = archiveDirectoryName;
            return this;
        }

        /**
         * Get the directory name to be used to store recordings and the {@link Catalog}.
         *
         * @return the directory name to be used for the archive to store recordings and the {@link Catalog}.
         */
        public String archiveDirectoryName()
        {
            return archiveDirectoryName;
        }

        /**
         * Get the directory in which the Archive will store recordings and the {@link Catalog}.
         *
         * @return the directory in which the Archive will store recordings and the {@link Catalog}.
         */
        public File archiveDir()
        {
            return archiveDir;
        }

        /**
         * Set the the directory in which the Archive will store recordings and the {@link Catalog}.
         *
         * @param archiveDir the directory in which the Archive will store recordings and the {@link Catalog}.
         * @return this for a fluent API.
         */
        public Context archiveDir(final File archiveDir)
        {
            this.archiveDir = archiveDir;
            return this;
        }

        /**
         * Get the {@link FileChannel} for the directory in which the Archive will store recordings and the
         * {@link Catalog}. This can be used for sync'ing the directory.
         *
         * @return the directory in which the Archive will store recordings and the {@link Catalog}.
         */
        public FileChannel archiveDirChannel()
        {
            return archiveDirChannel;
        }

        /**
         * Set the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating
         * with a remote archive for replication.
         *
         * @param archiveContext that should be used for communicating with a remote Archive.
         * @return this for a fluent API.
         */
        public Context archiveClientContext(final AeronArchive.Context archiveContext)
        {
            this.archiveClientContext = archiveContext;
            return this;
        }

        /**
         * Get the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating
         * with a remote archive for replication.
         *
         * @return the {@link io.aeron.archive.client.AeronArchive.Context} that should be used for communicating
         * with a remote archive for replication.
         */
        public AeronArchive.Context archiveClientContext()
        {
            return archiveClientContext;
        }

        /**
         * Get the channel URI on which the control request subscription will listen.
         *
         * @return the channel URI on which the control request subscription will listen.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        public String controlChannel()
        {
            return controlChannel;
        }

        /**
         * Set the channel URI on which the control request subscription will listen.
         *
         * @param controlChannel channel URI on which the control request subscription will listen.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_CHANNEL_PROP_NAME
         */
        public Context controlChannel(final String controlChannel)
        {
            this.controlChannel = controlChannel;
            return this;
        }

        /**
         * Get the stream id on which the control request subscription will listen.
         *
         * @return the stream id on which the control request subscription will listen.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_STREAM_ID_PROP_NAME
         */
        public int controlStreamId()
        {
            return controlStreamId;
        }

        /**
         * Set the stream id on which the control request subscription will listen.
         *
         * @param controlStreamId stream id on which the control request subscription will listen.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_STREAM_ID_PROP_NAME
         */
        public Context controlStreamId(final int controlStreamId)
        {
            this.controlStreamId = controlStreamId;
            return this;
        }

        /**
         * Get the driver local channel URI on which the control request subscription will listen.
         *
         * @return the channel URI on which the control request subscription will listen.
         * @see io.aeron.archive.client.AeronArchive.Configuration#LOCAL_CONTROL_CHANNEL_PROP_NAME
         */
        public String localControlChannel()
        {
            return localControlChannel;
        }

        /**
         * Set the driver local channel URI on which the control request subscription will listen.
         *
         * @param controlChannel channel URI on which the control request subscription will listen.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#LOCAL_CONTROL_CHANNEL_PROP_NAME
         */
        public Context localControlChannel(final String controlChannel)
        {
            this.localControlChannel = controlChannel;
            return this;
        }

        /**
         * Get the local stream id on which the control request subscription will listen.
         *
         * @return the stream id on which the control request subscription will listen.
         * @see io.aeron.archive.client.AeronArchive.Configuration#LOCAL_CONTROL_STREAM_ID_PROP_NAME
         */
        public int localControlStreamId()
        {
            return localControlStreamId;
        }

        /**
         * Should the control streams use sparse file term buffers.
         *
         * @param controlTermBufferSparse for the control stream.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_TERM_BUFFER_SPARSE_PROP_NAME
         */
        public Context controlTermBufferSparse(final boolean controlTermBufferSparse)
        {
            this.controlTermBufferSparse = controlTermBufferSparse;
            return this;
        }

        /**
         * Should the control streams use sparse file term buffers.
         *
         * @return true if the control stream should use sparse file term buffers.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_TERM_BUFFER_SPARSE_PROP_NAME
         */
        public boolean controlTermBufferSparse()
        {
            return controlTermBufferSparse;
        }

        /**
         * Set the term buffer length for the control streams.
         *
         * @param controlTermBufferLength for the control streams.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_TERM_BUFFER_LENGTH_PROP_NAME
         */
        public Context controlTermBufferLength(final int controlTermBufferLength)
        {
            this.controlTermBufferLength = controlTermBufferLength;
            return this;
        }

        /**
         * Get the term buffer length for the control streams.
         *
         * @return the term buffer length for the control streams.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_TERM_BUFFER_LENGTH_PROP_NAME
         */
        public int controlTermBufferLength()
        {
            return controlTermBufferLength;
        }

        /**
         * Set the MTU length for the control streams.
         *
         * @param controlMtuLength for the control streams.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_MTU_LENGTH_PROP_NAME
         */
        public Context controlMtuLength(final int controlMtuLength)
        {
            this.controlMtuLength = controlMtuLength;
            return this;
        }

        /**
         * Get the MTU length for the control streams.
         *
         * @return the MTU length for the control streams.
         * @see io.aeron.archive.client.AeronArchive.Configuration#CONTROL_MTU_LENGTH_PROP_NAME
         */
        public int controlMtuLength()
        {
            return controlMtuLength;
        }

        /**
         * Set the local stream id on which the control request subscription will listen.
         *
         * @param controlStreamId stream id on which the control request subscription will listen.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#LOCAL_CONTROL_STREAM_ID_PROP_NAME
         */
        public Context localControlStreamId(final int controlStreamId)
        {
            this.localControlStreamId = controlStreamId;
            return this;
        }

        /**
         * Get the channel URI on which the recording events publication will publish.
         *
         * @return the channel URI on which the recording events publication will publish.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_CHANNEL_PROP_NAME
         */
        public String recordingEventsChannel()
        {
            return recordingEventsChannel;
        }

        /**
         * Set the channel URI on which the recording events publication will publish.
         * <p>
         * To support dynamic subscribers then this can be set to multicast or MDC (Multi-Destination-Cast) if
         * multicast cannot be supported for on the available the network infrastructure.
         *
         * @param recordingEventsChannel channel URI on which the recording events publication will publish.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_CHANNEL_PROP_NAME
         * @see io.aeron.CommonContext#MDC_CONTROL_PARAM_NAME
         */
        public Context recordingEventsChannel(final String recordingEventsChannel)
        {
            this.recordingEventsChannel = recordingEventsChannel;
            return this;
        }

        /**
         * Get the stream id on which the recording events publication will publish.
         *
         * @return the stream id on which the recording events publication will publish.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_STREAM_ID_PROP_NAME
         */
        public int recordingEventsStreamId()
        {
            return recordingEventsStreamId;
        }

        /**
         * Set the stream id on which the recording events publication will publish.
         *
         * @param recordingEventsStreamId stream id on which the recording events publication will publish.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_STREAM_ID_PROP_NAME
         */
        public Context recordingEventsStreamId(final int recordingEventsStreamId)
        {
            this.recordingEventsStreamId = recordingEventsStreamId;
            return this;
        }

        /**
         * Should the recording events channel be enabled.
         *
         * @return true if the recording events channel should be enabled.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_ENABLED_PROP_NAME
         */
        public boolean recordingEventsEnabled()
        {
            return recordingEventsEnabled;
        }

        /**
         * Set if the recording events channel should be enabled.
         *
         * @param recordingEventsEnabled indication of if the recording events channel should be enabled.
         * @return this for a fluent API.
         * @see io.aeron.archive.client.AeronArchive.Configuration#RECORDING_EVENTS_ENABLED_PROP_NAME
         */
        public Context recordingEventsEnabled(final boolean recordingEventsEnabled)
        {
            this.recordingEventsEnabled = recordingEventsEnabled;
            return this;
        }

        /**
         * Get the channel URI for replicating stream from another archive as replays.
         *
         * @return the channel URI for replicating stream from another archive as replays.
         * @see Archive.Configuration#REPLICATION_CHANNEL_PROP_NAME
         */
        public String replicationChannel()
        {
            return replicationChannel;
        }

        /**
         * The channel URI for replicating stream from another archive as replays.
         *
         * @param replicationChannel channel URI for replicating stream from another archive as replays.
         * @return this for a fluent API.
         * @see Archive.Configuration#REPLICATION_CHANNEL_PROP_NAME
         */
        public Context replicationChannel(final String replicationChannel)
        {
            this.replicationChannel = replicationChannel;
            return this;
        }

        /**
         * The timeout in nanoseconds to wait for connection to be established.
         *
         * @param connectTimeoutNs to wait for a connection to be established.
         * @return this for a fluent API.
         * @see Configuration#CONNECT_TIMEOUT_PROP_NAME
         */
        public Context connectTimeoutNs(final long connectTimeoutNs)
        {
            this.connectTimeoutNs = connectTimeoutNs;
            return this;
        }

        /**
         * The timeout in nanoseconds to wait for connection to be established.
         *
         * @return the message timeout in nanoseconds to wait for a connection to be established.
         * @see Configuration#CONNECT_TIMEOUT_PROP_NAME
         */
        public long connectTimeoutNs()
        {
            return connectTimeoutNs;
        }

        /**
         * The timeout in nanoseconds for or a replay publication to linger after draining.
         *
         * @param replayLingerTimeoutNs in nanoseconds for a replay publication to linger after draining.
         * @return this for a fluent API.
         * @see Configuration#REPLAY_LINGER_TIMEOUT_PROP_NAME
         * @see io.aeron.driver.Configuration#PUBLICATION_LINGER_PROP_NAME
         */
        public Context replayLingerTimeoutNs(final long replayLingerTimeoutNs)
        {
            this.replayLingerTimeoutNs = replayLingerTimeoutNs;
            return this;
        }

        /**
         * The timeout in nanoseconds for a replay publication to linger after draining.
         *
         * @return the timeout in nanoseconds for a replay publication to linger after draining.
         * @see Configuration#REPLAY_LINGER_TIMEOUT_PROP_NAME
         * @see io.aeron.driver.Configuration#PUBLICATION_LINGER_PROP_NAME
         */
        public long replayLingerTimeoutNs()
        {
            return replayLingerTimeoutNs;
        }

        /**
         * Should the CRC be computed for each recorder fragment.
         *
         * @param recordingCrcEnabled {@code true} if the CRC computation should be enabled.
         * @return this for a fluent API.
         * @see Configuration#RECORDING_CRC_ENABLED_PROP_NAME
         */
        public Context recordingCrcEnabled(final boolean recordingCrcEnabled)
        {
            this.recordingCrcEnabled = recordingCrcEnabled;
            return this;
        }

        /**
         * Should the CRC be computed for each recorder fragment.
         *
         * @return {@code true} if the CRC computation should be enabled.
         * @see Configuration#RECORDING_CRC_ENABLED_PROP_NAME
         */
        public boolean recordingCrcEnabled()
        {
            return recordingCrcEnabled;
        }

        /**
         * Should the CRC be validated during replay.
         *
         * @param replayCrcEnabled {@code true} if the CRC validation should be done during replay.
         * @return this for a fluent API.
         * @see Configuration#REPLAY_CRC_ENABLED_PROP_NAME
         */
        public Context replayCrcEnabled(final boolean replayCrcEnabled)
        {
            this.replayCrcEnabled = replayCrcEnabled;
            return this;
        }

        /**
         * Should the CRC be validated during replay.
         *
         * @return {@code true} if the CRC validation should be done during replay.
         * @see Configuration#REPLAY_CRC_ENABLED_PROP_NAME
         */
        public boolean replayCrcEnabled()
        {
            return replayCrcEnabled;
        }

        /**
         * Provides an {@link IdleStrategy} supplier for idling the conductor or composite {@link Agent}. Which is also
         * the default for recorder and replayer {@link Agent}s.
         *
         * @param idleStrategySupplier supplier for idling the conductor.
         * @return this for a fluent API.
         */
        public Context idleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.idleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} for idling the conductor or composite {@link Agent}. Which is also
         * the default for recorder and replayer {@link Agent}s.
         *
         * @return a new {@link IdleStrategy} for idling the conductor or composite {@link Agent}.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
        }

        /**
         * Provides an {@link IdleStrategy} supplier for idling the recorder {@link Agent}.
         *
         * @param idleStrategySupplier supplier for idling the conductor.
         * @return this for a fluent API.
         */
        public Context recorderIdleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.recorderIdleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} for idling the recorder {@link Agent}.
         *
         * @return a new {@link IdleStrategy} for idling the recorder {@link Agent}.
         */
        public IdleStrategy recorderIdleStrategy()
        {
            return recorderIdleStrategySupplier.get();
        }

        /**
         * Provides an {@link IdleStrategy} supplier for idling the replayer {@link Agent}.
         *
         * @param idleStrategySupplier supplier for idling the replayer.
         * @return this for a fluent API.
         */
        public Context replayerIdleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.replayerIdleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} for idling the replayer {@link Agent}.
         *
         * @return a new {@link IdleStrategy} for idling the replayer {@link Agent}.
         */
        public IdleStrategy replayerIdleStrategy()
        {
            return replayerIdleStrategySupplier.get();
        }

        /**
         * Set the {@link EpochClock} to be used for tracking wall clock time.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time.
         * @return this for a fluent API.
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        /**
         * Get the {@link EpochClock} to used for tracking wall clock time.
         *
         * @return the {@link EpochClock} to used for tracking wall clock time.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * Get the file length used for recording data segment files.
         *
         * @return the file length used for recording data segment files
         */
        int segmentFileLength()
        {
            return segmentFileLength;
        }

        /**
         * Set the file length to be used for recording data segment files. If the {@link Image#termBufferLength()} is
         * larger than the segment file length then the term length will be used.
         *
         * @param segmentFileLength the file length to be used for recording data segment files.
         * @return this for a fluent API.
         */
        public Context segmentFileLength(final int segmentFileLength)
        {
            this.segmentFileLength = segmentFileLength;
            return this;
        }

        /**
         * Get level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return the level to be applied for file write.
         * @see #catalogFileSyncLevel()
         * @see Configuration#FILE_SYNC_LEVEL_PROP_NAME
         */
        int fileSyncLevel()
        {
            return fileSyncLevel;
        }

        /**
         * Set level at which files should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @param syncLevel to be applied for file writes.
         * @return this for a fluent API.
         * @see #catalogFileSyncLevel()
         * @see Configuration#FILE_SYNC_LEVEL_PROP_NAME
         */
        public Context fileSyncLevel(final int syncLevel)
        {
            this.fileSyncLevel = syncLevel;
            return this;
        }

        /**
         * Get level at which the catalog file should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @return the level to be applied for file write.
         * @see #fileSyncLevel()
         * @see Configuration#CATALOG_FILE_SYNC_LEVEL_PROP_NAME
         */
        int catalogFileSyncLevel()
        {
            return catalogFileSyncLevel;
        }

        /**
         * Set level at which the catalog file should be sync'ed to disk.
         * <ul>
         * <li>0 - normal writes.</li>
         * <li>1 - sync file data.</li>
         * <li>2 - sync file data + metadata.</li>
         * </ul>
         *
         * @param syncLevel to be applied for file writes.
         * @return this for a fluent API.
         * @see #fileSyncLevel()
         * @see Configuration#CATALOG_FILE_SYNC_LEVEL_PROP_NAME
         */
        public Context catalogFileSyncLevel(final int syncLevel)
        {
            this.catalogFileSyncLevel = syncLevel;
            return this;
        }

        /**
         * Get the {@link AgentInvoker} that should be used for the Media Driver if running in a lightweight mode.
         *
         * @return the {@link AgentInvoker} that should be used for the Media Driver if running in a lightweight mode.
         */
        AgentInvoker mediaDriverAgentInvoker()
        {
            return mediaDriverAgentInvoker;
        }

        /**
         * Set the {@link AgentInvoker} that should be used for the Media Driver if running in a lightweight mode.
         *
         * @param mediaDriverAgentInvoker that should be used for the Media Driver if running in a lightweight mode.
         * @return this for a fluent API.
         */
        public Context mediaDriverAgentInvoker(final AgentInvoker mediaDriverAgentInvoker)
        {
            this.mediaDriverAgentInvoker = mediaDriverAgentInvoker;
            return this;
        }

        /**
         * Get the {@link ErrorHandler} to be used by the Archive.
         *
         * @return the {@link ErrorHandler} to be used by the Archive.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Set the {@link ErrorHandler} to be used by the Archive.
         *
         * @param errorHandler the error handler to be used by the Archive.
         * @return this for a fluent API
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Non-default for context.
         *
         * @param countedErrorHandler to override the default.
         * @return this for a fluent API.
         */
        public Context countedErrorHandler(final CountedErrorHandler countedErrorHandler)
        {
            this.countedErrorHandler = countedErrorHandler;
            return this;
        }

        /**
         * The {@link #errorHandler()} that will increment {@link #errorCounter()} by default.
         *
         * @return {@link #errorHandler()} that will increment {@link #errorCounter()} by default.
         */
        public CountedErrorHandler countedErrorHandler()
        {
            return countedErrorHandler;
        }

        /**
         * Get the archive threading mode.
         *
         * @return the archive threading mode.
         */
        public ArchiveThreadingMode threadingMode()
        {
            return threadingMode;
        }

        /**
         * Set the archive threading mode.
         *
         * @param threadingMode archive threading mode.
         * @return this for a fluent API.
         */
        public Context threadingMode(final ArchiveThreadingMode threadingMode)
        {
            this.threadingMode = threadingMode;
            return this;
        }

        /**
         * Get the thread factory used for creating threads in {@link ArchiveThreadingMode#SHARED} and
         * {@link ArchiveThreadingMode#DEDICATED} threading modes.
         *
         * @return thread factory used for creating threads in SHARED and DEDICATED threading modes.
         */
        public ThreadFactory threadFactory()
        {
            return threadFactory;
        }

        /**
         * Set the thread factory used for creating threads in {@link ArchiveThreadingMode#SHARED} and
         * {@link ArchiveThreadingMode#DEDICATED} threading modes.
         *
         * @param threadFactory used for creating threads in SHARED and DEDICATED threading modes.
         * @return this for a fluent API.
         */
        public Context threadFactory(final ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * Set the error buffer length in bytes to use.
         *
         * @param errorBufferLength in bytes to use.
         * @return this for a fluent API.
         * @see Configuration#ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public Context errorBufferLength(final int errorBufferLength)
        {
            this.errorBufferLength = errorBufferLength;
            return this;
        }

        /**
         * The error buffer length in bytes.
         *
         * @return error buffer length in bytes.
         * @see Configuration#ERROR_BUFFER_LENGTH_PROP_NAME
         */
        public int errorBufferLength()
        {
            return errorBufferLength;
        }

        /**
         * Get the error counter that will record the number of errors observed.
         *
         * @return the error counter that will record the number of errors observed.
         */
        public AtomicCounter errorCounter()
        {
            return errorCounter;
        }

        /**
         * Set the error counter that will record the number of errors observed.
         *
         * @param errorCounter the error counter that will record the number of errors observed.
         * @return this for a fluent API.
         */
        public Context errorCounter(final AtomicCounter errorCounter)
        {
            this.errorCounter = errorCounter;
            return this;
        }

        /**
         * Get the max number of concurrent recordings.
         *
         * @return the max number of concurrent recordings.
         */
        public int maxConcurrentRecordings()
        {
            return maxConcurrentRecordings;
        }

        /**
         * Set the max number of concurrent recordings.
         *
         * @param maxConcurrentRecordings the max number of concurrent recordings.
         * @return this for a fluent API.
         */
        public Context maxConcurrentRecordings(final int maxConcurrentRecordings)
        {
            this.maxConcurrentRecordings = maxConcurrentRecordings;
            return this;
        }

        /**
         * Get the max number of concurrent replays.
         *
         * @return the max number of concurrent replays.
         */
        public int maxConcurrentReplays()
        {
            return maxConcurrentReplays;
        }

        /**
         * Set the max number of concurrent replays.
         *
         * @param maxConcurrentReplays the max number of concurrent replays.
         * @return this for a fluent API.
         */
        public Context maxConcurrentReplays(final int maxConcurrentReplays)
        {
            this.maxConcurrentReplays = maxConcurrentReplays;
            return this;
        }

        /**
         * Delete the archive directory if the {@link #archiveDir()} value is not null.
         */
        public void deleteArchiveDirectory()
        {
            if (null != archiveDir)
            {
                IoUtil.delete(archiveDir, false);
            }
        }

        /**
         * Set the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @param aeronDirectoryName the top level Aeron directory.
         * @return this for a fluent API.
         */
        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        /**
         * Get the top level Aeron directory used for communication between the Aeron client and Media Driver.
         *
         * @return The top level Aeron directory.
         */
        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * This client will be closed when the {@link Archive#close()} or {@link #close()} methods are called if
         * {@link #ownsAeronClient()} is true.
         *
         * @param aeron client for communicating with the local Media Driver.
         * @return this for a fluent API.
         * @see Aeron#connect()
         */
        public Context aeron(final Aeron aeron)
        {
            this.aeron = aeron;
            return this;
        }

        /**
         * {@link Aeron} client for communicating with the local Media Driver.
         * <p>
         * If not provided then a default will be established during {@link #conclude()} by calling
         * {@link Aeron#connect()}.
         *
         * @return client for communicating with the local Media Driver.
         */
        public Aeron aeron()
        {
            return aeron;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @param ownsAeronClient does this context own the {@link #aeron()} client.
         * @return this for a fluent API.
         */
        public Context ownsAeronClient(final boolean ownsAeronClient)
        {
            this.ownsAeronClient = ownsAeronClient;
            return this;
        }

        /**
         * Does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         *
         * @return does this context own the {@link #aeron()} client and this takes responsibility for closing it?
         */
        public boolean ownsAeronClient()
        {
            return ownsAeronClient;
        }

        /**
         * The {@link Catalog} describing the contents of the Archive.
         *
         * @param catalog {@link Catalog} describing the contents of the Archive.
         * @return this for a fluent API.
         */
        public Context catalog(final Catalog catalog)
        {
            this.catalog = catalog;
            return this;
        }

        /**
         * The {@link Catalog} describing the contents of the Archive.
         *
         * @return the {@link Catalog} describing the contents of the Archive.
         */
        public Catalog catalog()
        {
            return catalog;
        }

        /**
         * The {@link ArchiveMarkFile} for the Archive.
         *
         * @param archiveMarkFile {@link ArchiveMarkFile} for the Archive.
         * @return this for a fluent API.
         */
        public Context archiveMarkFile(final ArchiveMarkFile archiveMarkFile)
        {
            this.markFile = archiveMarkFile;
            return this;
        }

        /**
         * The {@link ArchiveMarkFile} for the Archive.
         *
         * @return {@link ArchiveMarkFile} for the Archive.
         */
        public ArchiveMarkFile archiveMarkFile()
        {
            return markFile;
        }

        /**
         * Maximum number of catalog entries for the Archive.
         *
         * @param maxCatalogEntries for the archive.
         * @return this for a fluent API.
         */
        public Context maxCatalogEntries(final long maxCatalogEntries)
        {
            this.maxCatalogEntries = maxCatalogEntries;
            return this;
        }

        /**
         * Maximum number of catalog entries for the Archive.
         *
         * @return maximum number of catalog entries for the Archive.
         */
        public long maxCatalogEntries()
        {
            return maxCatalogEntries;
        }

        /**
         * Get the {@link AuthenticatorSupplier} that should be used for the Archive.
         *
         * @return the {@link AuthenticatorSupplier} to be used for the Archive.
         */
        public AuthenticatorSupplier authenticatorSupplier()
        {
            return authenticatorSupplier;
        }

        /**
         * Set the {@link AuthenticatorSupplier} that will be used for the Archive.
         *
         * @param authenticatorSupplier {@link AuthenticatorSupplier} to use for the Archive.
         * @return this for a fluent API.
         */
        public Context authenticatorSupplier(final AuthenticatorSupplier authenticatorSupplier)
        {
            this.authenticatorSupplier = authenticatorSupplier;
            return this;
        }

        CountDownLatch abortLatch()
        {
            return abortLatch;
        }

        /**
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            CloseHelper.close(catalog);
            CloseHelper.close(markFile);
            CloseHelper.close(archiveDirChannel);
            archiveDirChannel = null;

            CloseHelper.close(errorCounter);
            if (errorHandler instanceof AutoCloseable)
            {
                CloseHelper.close((AutoCloseable)errorHandler);
            }

            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }
        }
    }

    /**
     * The filename to be used for a segment file based on recording id and position the segment begins.
     *
     * @param recordingId         to identify the recorded stream.
     * @param segmentBasePosition at which the segment file begins.
     * @return the filename to be used for a segment file based on recording id and position the segment begins.
     */
    static String segmentFileName(final long recordingId, final long segmentBasePosition)
    {
        return recordingId + "-" + segmentBasePosition + Configuration.RECORDING_SEGMENT_SUFFIX;
    }

    /**
     * Get the {@link FileChannel} for the parent directory for the recordings and catalog so it can be sync'ed
     * to storage when new files are created.
     *
     * @param directory     which will store the files created by the archive.
     * @param fileSyncLevel to be applied for file updates, {@link Archive.Configuration#FILE_SYNC_LEVEL_PROP_NAME}.
     * @return the the {@link FileChannel} for the parent directory for the recordings and catalog if fileSyncLevel
     * greater than zero otherwise null.
     */
    static FileChannel channelForDirectorySync(final File directory, final int fileSyncLevel)
    {
        if (fileSyncLevel > 0)
        {
            try
            {
                return FileChannel.open(directory.toPath());
            }
            catch (final IOException ignore)
            {
            }
        }

        return null;
    }
}
