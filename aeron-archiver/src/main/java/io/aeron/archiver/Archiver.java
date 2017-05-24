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
import org.agrona.CloseHelper;
import org.agrona.concurrent.*;

import java.io.File;

import static io.aeron.driver.MediaDriver.loadPropertiesFiles;

public class Archiver implements AutoCloseable
{
    private final Context ctx;
    private AgentRunner runner;
    private Aeron aeron;

    /**
     * Start an ArchiverConductor as a stand-alone process.
     *
     * @param args command line arguments
     * @throws Exception if an error occurs
     */
    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        try (Archiver ignore = Archiver.launch())
        {
            new ShutdownSignalBarrier().await();
            System.out.println("Shutdown Archiver...");
        }
    }

    public Archiver(final Context ctx)
    {
        this.ctx = ctx;
    }

    public void close() throws Exception
    {
        CloseHelper.close(runner);
        CloseHelper.close(aeron);
    }

    public Archiver start()
    {
        ctx.clientContext.driverAgentInvoker(ctx.driverAgentInvoker());
        aeron = Aeron.connect(ctx.clientContext);
        ctx.conclude();

        final ArchiveConductor archiveConductor = new ArchiveConductor(aeron, ctx);

        runner = new AgentRunner(
            ctx.clientContext.idleStrategy(),
            ctx.clientContext.errorHandler(),
            null,
            archiveConductor);
        AgentRunner.startOnThread(runner, ctx.clientContext.threadFactory());

        return this;
    }

    public static Archiver launch()
    {
        return launch(new Context());
    }

    public static Archiver launch(final Context ctx)
    {
        return new Archiver(ctx).start();
    }

    public static class Context
    {
        private Aeron.Context clientContext;
        private File archiveDir;
        private String controlRequestChannel;
        private int controlRequestStreamId;
        private String recordingEventsChannel;
        private int recordingEventsStreamId;
        private IdleStrategy idleStrategy;
        private EpochClock epochClock;
        private int segmentFileLength = 128 * 1024 * 1024;
        private boolean forceMetadataUpdates = true;
        private boolean forceWrites = true;
        private AgentInvoker driverAgentInvoker;

        public Context()
        {
            this(new Aeron.Context(), new File("archive"));
        }

        public Context(final Aeron.Context clientContext, final File archiveDir)
        {
            clientContext.useConductorAgentInvoker(true);
            clientContext.clientLock(new NoOpLock());
            this.clientContext = clientContext;
            this.archiveDir = archiveDir;
            controlRequestChannel = "aeron:udp?endpoint=localhost:8010";
            controlRequestStreamId = 0;
            recordingEventsChannel = "aeron:udp?endpoint=localhost:8011";
            recordingEventsStreamId = 0;
        }

        void conclude()
        {
            if (!archiveDir.exists() && !archiveDir.mkdirs())
            {
                throw new IllegalArgumentException(
                    "Failed to create archive dir: " + archiveDir.getAbsolutePath());
            }

            if (idleStrategy == null)
            {
                idleStrategy = new SleepingMillisIdleStrategy(Aeron.IDLE_SLEEP_MS);
            }

            if (epochClock == null)
            {
                epochClock = clientContext.epochClock();
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

        public Context clientContext(final Aeron.Context ctx)
        {
            this.clientContext = ctx;
            return this;
        }

        public String controlRequestChannel()
        {
            return controlRequestChannel;
        }

        public Context controlRequestChannel(final String controlRequestChannel)
        {
            this.controlRequestChannel = controlRequestChannel;
            return this;
        }

        public int controlRequestStreamId()
        {
            return controlRequestStreamId;
        }

        public Context controlRequestStreamId(final int controlRequestStreamId)
        {
            this.controlRequestStreamId = controlRequestStreamId;
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
         * Provides an IdleStrategy for the thread responsible for publication/subscription backoff.
         *
         * @param idleStrategy Thread idle strategy for publication/subscription backoff.
         * @return this Context for method chaining.
         */
        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
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

        boolean forceMetadataUpdates()
        {
            return forceMetadataUpdates;
        }

        public Context forceMetadataUpdates(final boolean forceMetadataUpdates)
        {
            this.forceMetadataUpdates = forceMetadataUpdates;
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
        public Context driverAgentInvoker(final AgentInvoker driverAgentInvoker)
        {
            this.driverAgentInvoker = driverAgentInvoker;
            return this;
        }
    }
}
