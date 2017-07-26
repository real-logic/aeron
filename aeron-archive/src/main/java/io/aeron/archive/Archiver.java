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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.StatusIndicator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * The Aeron Archiver is run with an embedded media driver and allows for the archival of 'local'
 * {@link io.aeron.Publication}s (ipc or spy subscriptions only).
 */
public final class Archiver implements AutoCloseable
{
    private final Context ctx;
    private final AgentRunner conductorRunner;
    private final AgentInvoker conductorInvoker;
    private final Aeron aeron;

    private Archiver(final Context ctx)
    {
        this.ctx = ctx;

        ctx.clientContext.driverAgentInvoker(ctx.mediaDriverAgentInvoker());
        ctx.clientContext.clientLock(new NoOpLock());
        aeron = Aeron.connect(ctx.clientContext);

        ctx.conclude();

        final ArchiveConductor archiveConductor;
        if (ctx.threadingMode() == ArchiverThreadingMode.DEDICATED)
        {
            archiveConductor = new DedicatedModeArchiveConductor(aeron, ctx);
        }
        else
        {
            archiveConductor = new SharedModeArchiveConductor(aeron, ctx);
        }

        switch (ctx.threadingMode())
        {
            case INVOKER:
                conductorInvoker = new AgentInvoker(ctx.errorHandler(), ctx.errorCounter(), archiveConductor);
                conductorRunner = null;
                break;

            case SHARED:
                conductorInvoker = null;
                conductorRunner = new AgentRunner(
                    ctx.idleStrategy(),
                    ctx.errorHandler(),
                    ctx.errorCounter(),
                    archiveConductor);
                break;

            default:
            case DEDICATED:
                conductorInvoker = null;
                conductorRunner = new AgentRunner(
                    ctx.idleStrategy(),
                    ctx.errorHandler(),
                    ctx.errorCounter(),
                    archiveConductor);
        }
    }

    /**
     * Get the {@link Archiver.Context} that is used by this {@link Archiver}.
     *
     * @return the {@link Archiver.Context} that is used by this {@link Archiver}.
     */
    public Context context()
    {
        return ctx;
    }

    public void close() throws Exception
    {
        CloseHelper.close(conductorInvoker);
        CloseHelper.close(conductorRunner);
        CloseHelper.close(aeron);
    }

    private Archiver start()
    {
        if (ctx.threadingMode() == ArchiverThreadingMode.SHARED ||
            ctx.threadingMode() == ArchiverThreadingMode.DEDICATED)
        {
            AgentRunner.startOnThread(conductorRunner, ctx.threadFactory());
        }

        if (null != conductorInvoker)
        {
            conductorInvoker.start();
        }

        return this;
    }

    public AgentInvoker invoker()
    {
        return conductorInvoker;
    }

    public static Archiver launch()
    {
        return launch(new Context());
    }

    public static Archiver launch(final Context ctx)
    {
        return new Archiver(ctx).start();
    }

    public static class Configuration
    {
        public static final String ARCHIVE_DIR_PROP_NAME = "aeron.archive.dir";
        public static final String ARCHIVE_DIR_DEFAULT = "archive";

        public static final String SEGMENT_FILE_LENGTH_PROP_NAME = "aeron.archive.segment.file.length";
        public static final int SEGMENT_FILE_LENGTH_DEFAULT = 128 * 1024 * 1024;

        public static final String FILE_SYNC_LEVEL_PROP_NAME = "aeron.archive.file.sync.level";
        public static final int FILE_SYNC_LEVEL_DEFAULT = 0;

        public static final String THREADING_MODE_PROP_NAME = "aeron.archive.threading.mode";
        public static final String ARCHIVER_IDLE_STRATEGY_PROP_NAME = "aeron.archive.idle.strategy";
        public static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";
        private static final String CONTROLLABLE_IDLE_STRATEGY = "org.agrona.concurrent.ControllableIdleStrategy";

        private static final long AGENT_IDLE_MAX_SPINS = 100;
        private static final long AGENT_IDLE_MAX_YIELDS = 100;
        private static final long AGENT_IDLE_MIN_PARK_NS = 1;
        private static final long AGENT_IDLE_MAX_PARK_NS = TimeUnit.MICROSECONDS.toNanos(100);

        public static final String MAX_CONCURRENT_RECORDINGS_PROP_NAME = "aeron.archive.max.concurrent.recordings";
        public static final int MAX_CONCURRENT_RECORDINGS_DEFAULT = 128;

        public static final String MAX_CONCURRENT_REPLAYS_PROP_NAME = "aeron.archive.max.concurrent.replays";
        public static final int MAX_CONCURRENT_REPLAYS_DEFAULT = 128;

        private static final String PRESET_RECORDINGS_FILE_PROP_NAME = "aeron.archive.preset.recordings.file";


        public static String archiveDirName()
        {
            return System.getProperty(ARCHIVE_DIR_PROP_NAME, ARCHIVE_DIR_DEFAULT);
        }

        private static int segmentFileLength()
        {
            return Integer.getInteger(SEGMENT_FILE_LENGTH_PROP_NAME, SEGMENT_FILE_LENGTH_DEFAULT);
        }

        private static int fileSyncLevel()
        {
            return Integer.getInteger(FILE_SYNC_LEVEL_PROP_NAME, FILE_SYNC_LEVEL_DEFAULT);
        }

        private static ArchiverThreadingMode threadingMode()
        {
            return ArchiverThreadingMode.valueOf(System.getProperty(
                THREADING_MODE_PROP_NAME, ArchiverThreadingMode.DEDICATED.name()));
        }

        private static Supplier<IdleStrategy> idleStrategySupplier(final StatusIndicator controllableStatus)
        {
            final String strategyName = System.getProperty(ARCHIVER_IDLE_STRATEGY_PROP_NAME, DEFAULT_IDLE_STRATEGY);
            return () ->
            {
                IdleStrategy idleStrategy = null;
                switch (strategyName)
                {
                    case DEFAULT_IDLE_STRATEGY:
                        idleStrategy = new BackoffIdleStrategy(
                            AGENT_IDLE_MAX_SPINS,
                            AGENT_IDLE_MAX_YIELDS,
                            AGENT_IDLE_MIN_PARK_NS,
                            AGENT_IDLE_MAX_PARK_NS);
                        break;

                    case CONTROLLABLE_IDLE_STRATEGY:
                        idleStrategy = new ControllableIdleStrategy(controllableStatus);
                        controllableStatus.setOrdered(ControllableIdleStrategy.PARK);
                        break;

                    default:
                        try
                        {
                            idleStrategy = (IdleStrategy)Class.forName(strategyName).newInstance();
                        }
                        catch (final Exception ex)
                        {
                            LangUtil.rethrowUnchecked(ex);
                        }
                        break;
                }

                return idleStrategy;
            };
        }

        private static int maxConcurrentRecordings()
        {
            return Integer.getInteger(MAX_CONCURRENT_RECORDINGS_PROP_NAME, MAX_CONCURRENT_RECORDINGS_DEFAULT);
        }

        private static int maxConcurrentReplays()
        {
            return Integer.getInteger(MAX_CONCURRENT_REPLAYS_PROP_NAME, MAX_CONCURRENT_REPLAYS_DEFAULT);
        }

        public static String presetRecordingFileName()
        {
            return System.getProperty(PRESET_RECORDINGS_FILE_PROP_NAME);
        }
    }

    public static class Context
    {
        private final Aeron.Context clientContext;
        private final List<String> presetRecordingChannels = new ArrayList<>();
        private final IntArrayList presetRecordingStreamIds = new IntArrayList();

        private File archiveDir;

        private String controlChannel;
        private int controlStreamId;

        private String recordingEventsChannel;
        private int recordingEventsStreamId;

        private int segmentFileLength;
        private int fileSyncLevel;

        private ArchiverThreadingMode threadingMode;
        private ThreadFactory threadFactory = Thread::new;

        private Supplier<IdleStrategy> idleStrategySupplier;
        private EpochClock epochClock;

        private ErrorHandler errorHandler;
        private CountersManager countersManager;

        private AtomicCounter errorCounter;

        private AgentInvoker mediaDriverAgentInvoker;
        private int maxConcurrentRecordings;
        private int maxConcurrentReplays;
        private String presetRecordingFileName;

        public Context()
        {
            this(new Aeron.Context());
        }

        public Context(final Aeron.Context clientContext)
        {
            this.clientContext = clientContext;
            clientContext.useConductorAgentInvoker(true);
            controlChannel(AeronArchive.Configuration.controlRequestChannel());
            controlStreamId(AeronArchive.Configuration.controlRequestStreamId());
            recordingEventsChannel(AeronArchive.Configuration.recordingEventsChannel());
            recordingEventsStreamId(AeronArchive.Configuration.recordingEventsStreamId());
            segmentFileLength(Configuration.segmentFileLength());
            fileSyncLevel(Configuration.fileSyncLevel());
            threadingMode(Configuration.threadingMode());
            maxConcurrentRecordings(Configuration.maxConcurrentRecordings());
            maxConcurrentReplays(Configuration.maxConcurrentReplays());
            presetRecordingFileName(Configuration.presetRecordingFileName());
        }

        void conclude()
        {
            if (null == errorHandler)
            {
                throw new IllegalStateException("Error handler must be externally supplied");
            }

            if (null == countersManager)
            {
                throw new IllegalStateException("Counter manager must be externally supplied");
            }
            errorCounter = countersManager.newCounter("archive-errors");

            if (null == archiveDir)
            {
                archiveDir = new File(Configuration.archiveDirName());
            }

            if (!archiveDir.exists() && !archiveDir.mkdirs())
            {
                throw new IllegalArgumentException(
                    "Failed to create archive dir: " + archiveDir.getAbsolutePath());
            }

            if (null == idleStrategySupplier)
            {
                idleStrategySupplier = Configuration.idleStrategySupplier(null);
            }

            if (null == epochClock)
            {
                epochClock = new SystemEpochClock();
            }

            if (presetRecordingFileName != null)
            {
                try (BufferedReader reader = new BufferedReader(new FileReader(new File(presetRecordingFileName))))
                {
                    String line;
                    while ((line = reader.readLine()) != null)
                    {
                        final int splitIndex = line.lastIndexOf(' ');
                        final String channel = line.substring(0, splitIndex);
                        final int streamId = Integer.valueOf(line.substring(splitIndex + 1));
                        presetRecordingChannels.add(channel);
                        presetRecordingStreamIds.add(streamId);
                    }
                }
                catch (final Exception e)
                {
                    LangUtil.rethrowUnchecked(e);
                }
            }
        }

        /**
         * Get the directory in which the Archiver will store recordings/index/counters etc.
         *
         * @return the directory in which the Archiver will store recordings/index/counters etc
         */
        public File archiveDir()
        {
            return archiveDir;
        }

        /**
         * Set the directory in which the Archiver will store recordings/index/counters etc.
         *
         * @param archiveDir the directory in which the Archiver will store recordings/index/counters etc
         * @return this for a fluent API.
         */
        public Context archiveDir(final File archiveDir)
        {
            this.archiveDir = archiveDir;
            return this;
        }

        /**
         * Get the Aeron client context used by the Archiver.
         *
         * @return Aeron client context used by the Archiver
         */
        public Aeron.Context clientContext()
        {
            return clientContext;
        }

        /**
         * Get the channel URI on which the control request subscription will listen.
         *
         * @return the channel URI on which the control request subscription will listen
         */
        public String controlChannel()
        {
            return controlChannel;
        }

        /**
         * Set the channel URI on which the control request subscription will listen.
         *
         * @param controlChannel channel URI on which the control request subscription will listen
         * @return this for a fluent API.
         */
        public Context controlChannel(final String controlChannel)
        {
            this.controlChannel = controlChannel;
            return this;
        }

        /**
         * Get the stream id on which the control request subscription will listen.
         *
         * @return the stream id on which the control request subscription will listen
         */
        public int controlStreamId()
        {
            return controlStreamId;
        }

        /**
         * Set the stream id on which the control request subscription will listen.
         *
         * @param controlStreamId stream id on which the control request subscription will listen
         * @return this for a fluent API.
         */
        public Context controlStreamId(final int controlStreamId)
        {
            this.controlStreamId = controlStreamId;
            return this;
        }

        /**
         * Get the channel URI on which the recording events publication will publish.
         *
         * @return the channel URI on which the recording events publication will publish
         */
        public String recordingEventsChannel()
        {
            return recordingEventsChannel;
        }

        /**
         * Set the channel URI on which the recording events publication will publish.
         *
         * @param recordingEventsChannel channel URI on which the recording events publication will publish
         * @return this for a fluent API.
         */
        public Context recordingEventsChannel(final String recordingEventsChannel)
        {
            this.recordingEventsChannel = recordingEventsChannel;
            return this;
        }

        /**
         * Get the stream id on which the recording events publication will publish.
         *
         * @return the stream id on which the recording events publication will publish
         */
        public int recordingEventsStreamId()
        {
            return recordingEventsStreamId;
        }

        /**
         * Set the stream id on which the recording events publication will publish.
         *
         * @param recordingEventsStreamId stream id on which the recording events publication will publish
         * @return this for a fluent API.
         */
        public Context recordingEventsStreamId(final int recordingEventsStreamId)
        {
            this.recordingEventsStreamId = recordingEventsStreamId;
            return this;
        }

        /**
         * Provides an {@link IdleStrategy} supplier for the thread responsible for publication/subscription backoff.
         *
         * @param idleStrategySupplier supplier of thread idle strategy for publication/subscription backoff
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
         * @return a new {@link IdleStrategy} based on configured supplier
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
        }

        /**
         * Set the {@link EpochClock} to be used for tracking wall clock time when interacting with the archive.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time when interacting with the archive
         * @return this for a fluent API.
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        /**
         * Get the {@link EpochClock} to used for tracking wall clock time within the archive.
         *
         * @return the {@link EpochClock} to used for tracking wall clock time within the archive.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * Get the file length used for recording data segment files.
         * @return the file length used for recording data segment files
         */
        int segmentFileLength()
        {
            return segmentFileLength;
        }

        /**
         * Set the file length to be used for recording data segment files.
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
         *     <li>0 - normal writes.</li>
         *     <li>1 - sync file data.</li>
         *     <li>2 - sync file data + metadata.</li>
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
         *     <li>0 - normal writes.</li>
         *     <li>1 - sync file data.</li>
         *     <li>2 - sync file data + metadata.</li>
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
         * Get the {@link ErrorHandler} to be used by the Archiver.
         *
         * @return the {@link ErrorHandler} to be used by the Archiver
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * Set the {@link ErrorHandler} to be used by the Archiver.
         *
         * @param errorHandler the error handler to be used by the Archiver
         * @return this for a fluent API
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the archive threading mode.
         *
         * @return the archive threading mode
         */
        public ArchiverThreadingMode threadingMode()
        {
            return threadingMode;
        }

        /**
         * Set the archive threading mode.
         *
         * @param threadingMode archive threading mode
         * @return this for a fluent API
         */
        public Context threadingMode(final ArchiverThreadingMode threadingMode)
        {
            this.threadingMode = threadingMode;
            return this;
        }

        /**
         * Get the thread factory used for creating threads in SHARED and DEDICATED threading modes.
         *
         * @return thread factory used for creating threads in SHARED and DEDICATED threading modes
         */
        public ThreadFactory threadFactory()
        {
            return threadFactory;
        }

        /**
         * Set the thread factory used for creating threads in SHARED and DEDICATED threading modes.
         *
         * @param threadFactory used for creating threads in SHARED and DEDICATED threading modes
         * @return this for a fluent API
         */
        public Context threadFactory(final ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        public AtomicCounter errorCounter()
        {
            return errorCounter;
        }

        /**
         * Get the max number of concurrent recordings.
         *
         * @return the max number of concurrent recordings
         */
        public int maxConcurrentRecordings()
        {
            return this.maxConcurrentRecordings;
        }

        /**
         * Set the max number of concurrent recordings.
         *
         * @param maxConcurrentRecordings the max number of concurrent recordings
         * @return this for a fluent API
         */
        public Context maxConcurrentRecordings(final int maxConcurrentRecordings)
        {
            this.maxConcurrentRecordings = maxConcurrentRecordings;
            return this;
        }

        /**
         * Get the max number of concurrent replays.
         *
         * @return the max number of concurrent replays
         */
        public int maxConcurrentReplays()
        {
            return this.maxConcurrentReplays;
        }

        /**
         * Set the max number of concurrent replays.
         *
         * @param maxConcurrentReplays the max number of concurrent replays
         * @return this for a fluent API
         */
        public Context maxConcurrentReplays(final int maxConcurrentReplays)
        {
            this.maxConcurrentReplays = maxConcurrentReplays;
            return this;
        }

        /**
         * The counters manager is a shared resource between the embedded media driver and the archive.
         *
         * @param countersManager shared counters manager to be used
         * @return this for a fluent API
         */
        public Context countersManager(final CountersManager countersManager)
        {
            this.countersManager = countersManager;
            return this;
        }

        /**
         * A file with recording presets to be subscribed to by the Archiver on startup. The file is expected to follow
         * the format:
         * <pre>
         *     channel streamId
         * </pre>
         *
         * @param presetRecordingFileName preset recordings file to be loaded
         * @return this for a fluent API
         */
        public Context presetRecordingFileName(final String presetRecordingFileName)
        {
            this.presetRecordingFileName = presetRecordingFileName;
            return this;
        }

        /**
         * Get the preset recording file name.
         *
         * @return the preset recording file name
         */
        public String presetRecordingFileName()
        {
            return presetRecordingFileName;
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

        List<String> presetRecordingChannels()
        {
            return presetRecordingChannels;
        }

        IntArrayList presetRecordingStreamIds()
        {
            return presetRecordingStreamIds;
        }
    }
}
