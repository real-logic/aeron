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
package io.aeron.archiver;

import io.aeron.Aeron;
import org.agrona.*;
import org.agrona.concurrent.*;
import org.agrona.concurrent.status.*;

import java.io.File;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

public final class Archiver implements AutoCloseable
{
    private final Context ctx;
    private final AgentRunner conductorRunner;
    private final AgentRunner replayRunner;
    private final AgentRunner recorderRunner;
    private final AgentInvoker invoker;
    private final Aeron aeron;

    private Archiver(final Context ctx)
    {
        this.ctx = ctx;

        ctx.clientContext.driverAgentInvoker(ctx.driverAgentInvoker());
        if (ctx.threadingMode() != ArchiverThreadingMode.DEDICATED)
        {
            ctx.clientContext.clientLock(new NoOpLock());
        }
        aeron = Aeron.connect(ctx.clientContext);

        ctx.conclude();

        final ErrorHandler errorHandler = ctx.errorHandler();
        final AtomicCounter errorCounter = ctx.errorCounter();

        final Replayer replayer;
        final Recorder recorder;
        if (ctx.threadingMode() == ArchiverThreadingMode.DEDICATED)
        {
            replayer = new ReplayerProxy(aeron, ctx);
            recorder = new RecorderProxy(aeron, ctx);
        }
        else
        {
            replayer = new Replayer(aeron, ctx);
            recorder = new Recorder(aeron, ctx);
            ctx.replayerInvoker(new AgentInvoker(errorHandler, errorCounter, replayer));
            ctx.recorderInvoker(new AgentInvoker(errorHandler, errorCounter, recorder));
        }
        ctx
            .replayer(replayer)
            .recorder(recorder);

        final ArchiveConductor archiveConductor = new ArchiveConductor(aeron, ctx);
        switch (ctx.threadingMode())
        {
            case INVOKER:
                invoker = new AgentInvoker(errorHandler, errorCounter, archiveConductor);
                conductorRunner = null;
                replayRunner = null;
                recorderRunner = null;
                break;

            case SHARED:
                invoker = null;
                conductorRunner = new AgentRunner(
                    ctx.idleStrategy(),
                    errorHandler,
                    errorCounter,
                    archiveConductor);
                replayRunner = null;
                recorderRunner = null;
                break;

            default:
            case DEDICATED:
                invoker = null;
                conductorRunner = new AgentRunner(
                    ctx.idleStrategy(),
                    errorHandler,
                    errorCounter,
                    archiveConductor);
                replayRunner = new AgentRunner(
                    ctx.idleStrategy(),
                    errorHandler,
                    errorCounter,
                    replayer);
                recorderRunner = new AgentRunner(
                    ctx.idleStrategy(),
                    errorHandler,
                    errorCounter,
                    recorder);
        }
    }

    public void close() throws Exception
    {
        CloseHelper.close(conductorRunner);
        CloseHelper.close(replayRunner);
        CloseHelper.close(recorderRunner);
        CloseHelper.close(aeron);
    }

    private Archiver start()
    {
        if (ctx.threadingMode() == ArchiverThreadingMode.SHARED)
        {
            AgentRunner.startOnThread(conductorRunner, ctx.threadFactory());
        }
        else if (ctx.threadingMode() == ArchiverThreadingMode.DEDICATED)
        {
            AgentRunner.startOnThread(conductorRunner, ctx.threadFactory());
            AgentRunner.startOnThread(replayRunner, ctx.threadFactory());
            AgentRunner.startOnThread(recorderRunner, ctx.threadFactory());
        }

        return this;
    }

    public AgentInvoker invoker()
    {
        return invoker;
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
        public static final String ARCHIVE_DIR_PROP_NAME = "aeron.archiver.dir";
        public static final String ARCHIVE_DIR_DEFAULT = "archive";

        public static final String CONTROL_CHANNEL_PROP_NAME = "aeron.archiver.control.channel";
        public static final String CONTROL_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8010";
        public static final String CONTROL_STREAM_ID_PROP_NAME = "aeron.archiver.control.stream.id";
        public static final int CONTROL_STREAM_ID_DEFAULT = 0;

        public static final String RECORDING_EVENTS_CHANNEL_PROP_NAME = "aeron.archiver.recording.events.channel";
        public static final String RECORDING_EVENTS_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:8011";
        public static final String RECORDING_EVENTS_STREAM_ID_PROP_NAME = "aeron.archiver.recording.events.stream.id";
        public static final int RECORDING_EVENTS_STREAM_ID_DEFAULT = 0;

        public static final String SEGMENT_FILE_LENGTH_PROP_NAME = "aeron.archiver.segment.file.length";
        public static final int SEGMENT_FILE_LENGTH_DEFAULT = 128 * 1024 * 1024;

        public static final String FORCE_WRITES_PROP_NAME = "aeron.archiver.force.writes";

        public static String archiveDirName()
        {
            return System.getProperty(ARCHIVE_DIR_PROP_NAME, ARCHIVE_DIR_DEFAULT);
        }

        public static String controlChannel()
        {
            return System.getProperty(CONTROL_CHANNEL_PROP_NAME, CONTROL_CHANNEL_DEFAULT);
        }

        public static int controlStreamId()
        {
            return Integer.getInteger(CONTROL_STREAM_ID_PROP_NAME, CONTROL_STREAM_ID_DEFAULT);
        }

        public static String recordingEventsChannel()
        {
            return System.getProperty(RECORDING_EVENTS_CHANNEL_PROP_NAME, RECORDING_EVENTS_CHANNEL_DEFAULT);
        }

        public static int recordingEventsStreamId()
        {
            return Integer.getInteger(RECORDING_EVENTS_STREAM_ID_PROP_NAME, RECORDING_EVENTS_STREAM_ID_DEFAULT);
        }

        private static int segmentFileLength()
        {
            return Integer.getInteger(SEGMENT_FILE_LENGTH_PROP_NAME, SEGMENT_FILE_LENGTH_DEFAULT);
        }

        private static boolean forceWrites()
        {
            return Boolean.valueOf(System.getProperty(FORCE_WRITES_PROP_NAME, "true"));
        }
    }

    public static class Context
    {
        private final Aeron.Context clientContext;
        private File archiveDir;

        private String controlChannel;
        private int controlStreamId;

        private String recordingEventsChannel;
        private int recordingEventsStreamId;

        private int segmentFileLength;
        private boolean forceWrites;

        private ArchiverThreadingMode threadingMode = ArchiverThreadingMode.SHARED;
        private ThreadFactory threadFactory = Thread::new;

        private Supplier<IdleStrategy> idleStrategySupplier;
        private EpochClock epochClock;
        private ErrorHandler errorHandler;
        private AtomicCounter errorCounter;

        private AgentInvoker driverAgentInvoker;
        private AgentInvoker replayerInvoker;
        private AgentInvoker recorderInvoker;
        private Replayer replayer;
        private Recorder recorder;

        public Context()
        {
            this(new Aeron.Context());
        }

        public Context(final Aeron.Context clientContext)
        {
            clientContext.useConductorAgentInvoker(true);
            this.clientContext = clientContext;
            controlChannel(Configuration.controlChannel());
            controlStreamId(Configuration.controlStreamId());
            recordingEventsChannel(Configuration.recordingEventsChannel());
            recordingEventsStreamId(Configuration.recordingEventsStreamId());
            segmentFileLength(Configuration.segmentFileLength());
            forceWrites(Configuration.forceWrites());
        }

        void conclude()
        {
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
                idleStrategySupplier = () -> new SleepingMillisIdleStrategy(Aeron.IDLE_SLEEP_MS);
            }

            if (null == epochClock)
            {
                epochClock = new SystemEpochClock();
            }

            if (null == errorHandler)
            {
                errorHandler = Throwable::printStackTrace;
            }

            if (null == errorCounter)
            {
                final CountersManager counters = new CountersManager(
                    clientContext.countersMetaDataBuffer(),
                    clientContext.countersValuesBuffer());
                errorCounter = counters.newCounter("archiver-errors");
            }
        }

        public File archiveDir()
        {
            return archiveDir;
        }

        public Context archiveDir(final File archiveDir)
        {
            this.archiveDir = archiveDir;
            return this;
        }

        public Aeron.Context clientContext()
        {
            return clientContext;
        }

        public String controlChannel()
        {
            return controlChannel;
        }

        public Context controlChannel(final String controlChannel)
        {
            this.controlChannel = controlChannel;
            return this;
        }

        public int controlStreamId()
        {
            return controlStreamId;
        }

        public Context controlStreamId(final int controlStreamId)
        {
            this.controlStreamId = controlStreamId;
            return this;
        }

        public String recordingEventsChannel()
        {
            return recordingEventsChannel;
        }

        public Context recordingEventsChannel(final String recordingEventsChannel)
        {
            this.recordingEventsChannel = recordingEventsChannel;
            return this;
        }

        public int recordingEventsStreamId()
        {
            return recordingEventsStreamId;
        }

        public Context recordingEventsStreamId(final int recordingEventsStreamId)
        {
            this.recordingEventsStreamId = recordingEventsStreamId;
            return this;
        }

        /**
         * Provides an IdleStrategy supplier for the thread responsible for publication/subscription backoff.
         *
         * @param idleStrategySupplier supplier of thread idle strategy for publication/subscription backoff.
         * @return this Context for method chaining.
         */
        public Context idleStrategySupplier(final Supplier<IdleStrategy> idleStrategySupplier)
        {
            this.idleStrategySupplier = idleStrategySupplier;
            return this;
        }

        public IdleStrategy idleStrategy()
        {
            return idleStrategySupplier.get();
        }

        /**
         * Set the {@link EpochClock} to be used for tracking wall clock time when interacting with the archiver.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time when interacting with the archiver.
         * @return this Context for method chaining
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        public EpochClock epochClock()
        {
            return epochClock;
        }

        int segmentFileLength()
        {
            return segmentFileLength;
        }

        public Context segmentFileLength(final int segmentFileLength)
        {
            this.segmentFileLength = segmentFileLength;
            return this;
        }

        boolean forceWrites()
        {
            return forceWrites;
        }

        public Context forceWrites(final boolean forceWrites)
        {
            this.forceWrites = forceWrites;
            return this;
        }

        /**
         * Get the {@link AgentInvoker} that should be used for the Media Driver if running in a lightweight mode.
         *
         * @return the {@link AgentInvoker} that should be used for the Media Driver if running in a lightweight mode.
         */
        AgentInvoker driverAgentInvoker()
        {
            return driverAgentInvoker;
        }

        /**
         * Set the {@link AgentInvoker} that should be used for the Media Driver if running in a lightweight mode.
         *
         * @param driverAgentInvoker that should be used for the Media Driver if running in a lightweight mode.
         * @return this for a fluent API.
         */
        Context driverAgentInvoker(final AgentInvoker driverAgentInvoker)
        {
            this.driverAgentInvoker = driverAgentInvoker;
            return this;
        }

        AgentInvoker replayerInvoker()
        {
            return replayerInvoker;
        }

        Context replayerInvoker(final AgentInvoker replayerInvoker)
        {
            this.replayerInvoker = replayerInvoker;
            return this;
        }

        AgentInvoker recorderInvoker()
        {
            return recorderInvoker;
        }

        Context recorderInvoker(final AgentInvoker recorderInvoker)
        {
            this.recorderInvoker = recorderInvoker;
            return this;
        }

        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        public AtomicCounter errorCounter()
        {
            return errorCounter;
        }

        public Context errorCounter(final AtomicCounter errorCounter)
        {
            this.errorCounter = errorCounter;
            return this;
        }

        public ArchiverThreadingMode threadingMode()
        {
            return threadingMode;
        }

        public Context threadingMode(final ArchiverThreadingMode threadingMode)
        {
            this.threadingMode = threadingMode;
            return this;
        }

        public ThreadFactory threadFactory()
        {
            return threadFactory;
        }

        public Context threadFactory(final ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        Context replayer(final Replayer replayer)
        {
            this.replayer = replayer;
            return this;
        }

        Context recorder(final Recorder recorder)
        {
            this.recorder = recorder;
            return this;
        }

        Replayer replayer()
        {
            return replayer;
        }

        Recorder recorder()
        {
            return recorder;
        }
    }
}
