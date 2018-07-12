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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.StatusIndicator;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import static io.aeron.driver.status.SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MAX_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.agrona.SystemUtil.getSizeAsInt;
import static org.agrona.SystemUtil.loadPropertiesFiles;

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

        final Aeron aeron = ctx.aeron();

        final ArchiveConductor conductor = ArchiveThreadingMode.DEDICATED == ctx.threadingMode() ?
            new DedicatedModeArchiveConductor(aeron, ctx) :
            new SharedModeArchiveConductor(aeron, ctx);

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
        CloseHelper.close(ctx);
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
        public static final String ARCHIVE_DIR_PROP_NAME = "aeron.archive.dir";
        public static final String ARCHIVE_DIR_DEFAULT = "aeron-archive";

        public static final String SEGMENT_FILE_LENGTH_PROP_NAME = "aeron.archive.segment.file.length";
        public static final int SEGMENT_FILE_LENGTH_DEFAULT = 128 * 1024 * 1024;

        public static final String FILE_SYNC_LEVEL_PROP_NAME = "aeron.archive.file.sync.level";
        public static final int FILE_SYNC_LEVEL_DEFAULT = 0;

        public static final String THREADING_MODE_PROP_NAME = "aeron.archive.threading.mode";
        public static final String ARCHIVE_IDLE_STRATEGY_PROP_NAME = "aeron.archive.idle.strategy";
        public static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";

        public static final String MAX_CONCURRENT_RECORDINGS_PROP_NAME = "aeron.archive.max.concurrent.recordings";
        public static final int MAX_CONCURRENT_RECORDINGS_DEFAULT = 128;

        public static final String MAX_CONCURRENT_REPLAYS_PROP_NAME = "aeron.archive.max.concurrent.replays";
        public static final int MAX_CONCURRENT_REPLAYS_DEFAULT = 128;

        public static final String REPLAY_FRAGMENT_LIMIT_PROP_NAME = "aeron.archive.replay.fragment.limit";
        public static final int REPLAY_FRAGMENT_LIMIT_DEFAULT = 64;

        public static final String MAX_CATALOG_ENTRIES_PROP_NAME = "aeron.archive.max.catalog.entries";
        public static final long MAX_CATALOG_ENTRIES_DEFAULT = Catalog.DEFAULT_MAX_ENTRIES;

        static final String CATALOG_FILE_NAME = "archive.catalog";
        static final String RECORDING_SEGMENT_POSTFIX = ".rec";

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
         */
        public static int fileSyncLevel()
        {
            return Integer.getInteger(FILE_SYNC_LEVEL_PROP_NAME, FILE_SYNC_LEVEL_DEFAULT);
        }

        /**
         * The threading mode to be employed by the archive.
         *
         * @return the threading mode to be employed by the archive.
         */
        public static ArchiveThreadingMode threadingMode()
        {
            return ArchiveThreadingMode.valueOf(System.getProperty(
                THREADING_MODE_PROP_NAME, ArchiveThreadingMode.DEDICATED.name()));
        }

        /**
         * Create a supplier of {@link IdleStrategy}s that will use the system property.
         *
         * @param controllableStatus if a {@link org.agrona.concurrent.ControllableIdleStrategy} is required.
         * @return the new idle strategy
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
         * Limit for the number of fragments to be replayed per duty cycle on a replay.
         *
         * @return the limit for the number of fragments to be replayed per duty cycle on a replay.
         */
        public static int replayFragmentLimit()
        {
            return Integer.getInteger(REPLAY_FRAGMENT_LIMIT_PROP_NAME, REPLAY_FRAGMENT_LIMIT_DEFAULT);
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
    }

    /**
     * Overrides for the defaults and system properties.
     */
    public static class Context implements AutoCloseable, Cloneable
    {
        private boolean deleteArchiveOnStart = false;
        private boolean ownsAeronClient = false;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Aeron aeron;
        private File archiveDir;
        private String archiveDirectoryName = Configuration.archiveDirName();
        private FileChannel archiveDirChannel;
        private Catalog catalog;
        private ArchiveMarkFile markFile;

        private String controlChannel = AeronArchive.Configuration.controlChannel();
        private int controlStreamId = AeronArchive.Configuration.controlStreamId();
        private String localControlChannel = AeronArchive.Configuration.localControlChannel();
        private int localControlStreamId = AeronArchive.Configuration.localControlStreamId();

        private String recordingEventsChannel = AeronArchive.Configuration.recordingEventsChannel();
        private int recordingEventsStreamId = AeronArchive.Configuration.recordingEventsStreamId();

        private long maxCatalogEntries = Configuration.maxCatalogEntries();
        private int segmentFileLength = Configuration.segmentFileLength();
        private int fileSyncLevel = Configuration.fileSyncLevel();

        private ArchiveThreadingMode threadingMode = Configuration.threadingMode();
        private ThreadFactory threadFactory;

        private Supplier<IdleStrategy> idleStrategySupplier;
        private EpochClock epochClock;

        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;
        private CountedErrorHandler countedErrorHandler;

        private AgentInvoker mediaDriverAgentInvoker;
        private int maxConcurrentRecordings = Configuration.maxConcurrentRecordings();
        private int maxConcurrentReplays = Configuration.maxConcurrentReplays();

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
        public void conclude()
        {
            Objects.requireNonNull(errorHandler, "Error handler must be supplied");

            if (null == epochClock)
            {
                epochClock = new SystemEpochClock();
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
                        .clientLock(new NoOpLock()));

                if (null == errorCounter)
                {
                    errorCounter = aeron.addCounter(SYSTEM_COUNTER_TYPE_ID, "Archive errors");
                }
            }

            if (null == aeron.conductorAgentInvoker())
            {
                throw new ArchiveException("Aeron client must use conductor agent invoker");
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

            archiveDirChannel = channelForDirectorySync(archiveDir, fileSyncLevel);

            if (!BitUtil.isPowerOfTwo(segmentFileLength))
            {
                throw new ArchiveException("segment file length not a power of 2: " + segmentFileLength);
            }
            else if (segmentFileLength < TERM_MIN_LENGTH || segmentFileLength > TERM_MAX_LENGTH)
            {
                throw new ArchiveException("segment file length not in valid range: " + segmentFileLength);
            }

            if (null == markFile)
            {
                markFile = new ArchiveMarkFile(this);
            }

            if (null == catalog)
            {
                catalog = new Catalog(archiveDir, archiveDirChannel, fileSyncLevel, maxCatalogEntries, epochClock);
            }
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
         * Get the channel URI on which the control request subscription will listen.
         *
         * @return the channel URI on which the control request subscription will listen.
         * @see AeronArchive.Configuration#CONTROL_CHANNEL_PROP_NAME
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
         * @see AeronArchive.Configuration#CONTROL_CHANNEL_PROP_NAME
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
         * @see AeronArchive.Configuration#CONTROL_STREAM_ID_PROP_NAME
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
         * @see AeronArchive.Configuration#CONTROL_STREAM_ID_PROP_NAME
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
         * @see AeronArchive.Configuration#LOCAL_CONTROL_CHANNEL_PROP_NAME
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
         * @see AeronArchive.Configuration#LOCAL_CONTROL_CHANNEL_PROP_NAME
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
         * @see AeronArchive.Configuration#LOCAL_CONTROL_STREAM_ID_PROP_NAME
         */
        public int localControlStreamId()
        {
            return localControlStreamId;
        }

        /**
         * Set the local stream id on which the control request subscription will listen.
         *
         * @param controlStreamId stream id on which the control request subscription will listen.
         * @return this for a fluent API.
         * @see AeronArchive.Configuration#LOCAL_CONTROL_STREAM_ID_PROP_NAME
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
         * @see AeronArchive.Configuration#RECORDING_EVENTS_CHANNEL_PROP_NAME
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
         * @see AeronArchive.Configuration#RECORDING_EVENTS_CHANNEL_PROP_NAME
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
         * @see AeronArchive.Configuration#RECORDING_EVENTS_STREAM_ID_PROP_NAME
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
         * @see AeronArchive.Configuration#RECORDING_EVENTS_STREAM_ID_PROP_NAME
         */
        public Context recordingEventsStreamId(final int recordingEventsStreamId)
        {
            this.recordingEventsStreamId = recordingEventsStreamId;
            return this;
        }

        /**
         * Provides an {@link IdleStrategy} supplier for the thread responsible for publication/subscription backoff.
         *
         * @param idleStrategySupplier supplier of thread idle strategy for publication/subscription backoff.
         * @return this for a fluent API.
         */
        public Context idleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.idleStrategySupplier = idleStrategySupplier;
            return this;
        }

        /**
         * Get a new {@link IdleStrategy} based on configured supplier.
         *
         * @return a new {@link IdleStrategy} based on configured supplier.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
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
         */
        public Context fileSyncLevel(final int syncLevel)
        {
            this.fileSyncLevel = syncLevel;
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
            return this.maxConcurrentRecordings;
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
            return this.maxConcurrentReplays;
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
         * Close the context and free applicable resources.
         * <p>
         * If {@link #ownsAeronClient()} is true then the {@link #aeron()} client will be closed.
         */
        public void close()
        {
            if (ownsAeronClient)
            {
                CloseHelper.close(aeron);
            }

            CloseHelper.close(catalog);
            CloseHelper.close(markFile);
            CloseHelper.close(archiveDirChannel);
        }
    }

    static int segmentFileIndex(final long startPosition, final long position, final int segmentFileLength)
    {
        return (int)((position - startPosition) >> LogBufferDescriptor.positionBitsToShift(segmentFileLength));
    }

    static String segmentFileName(final long recordingId, final int segmentIndex)
    {
        return recordingId + "-" + segmentIndex + Configuration.RECORDING_SEGMENT_POSTFIX;
    }

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
