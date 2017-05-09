/*
 * Copyright 2014-2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
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

    public Archiver(final Context ctx)
    {
        this.ctx = ctx;
    }

    /**
     * Start an ArchiverConductor as a stand-alone process.
     *
     * @param args command line arguments
     * @throws Exception if an error occurs
     */
    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        setup();
    }

    static void setup() throws Exception
    {
        try (Archiver ignore = Archiver.launch())
        {
            new ShutdownSignalBarrier().await();
            System.out.println("Shutdown Archiver...");
        }
    }

    public void close() throws Exception
    {
        CloseHelper.close(runner);
        CloseHelper.close(aeron);
    }

    public void start()
    {
        aeron = Aeron.connect(ctx.clientContext);
        ctx.conclude();

        final ArchiveConductor archiveConductor = new ArchiveConductor(aeron, ctx);

        runner = new AgentRunner(
            ctx.clientContext.idleStrategy(),
            ctx.clientContext.errorHandler(),
            null,
            archiveConductor);
        AgentRunner.startOnThread(runner, ctx.clientContext.threadFactory());
    }

    public static Archiver launch()
    {
        return launch(new Context());
    }

    public static Archiver launch(final Context ctx)
    {
        final Archiver archiver = new Archiver(ctx);
        archiver.start();
        return archiver;
    }

    public static class Context
    {
        private Aeron.Context clientContext;
        private File archiveDir;
        private String serviceRequestChannel;
        private int serviceRequestStreamId;
        private String archiverNotificationsChannel;
        private int archiverNotificationsStreamId;
        private IdleStrategy idleStrategy;
        private EpochClock epochClock;
        private int segmentFileLength = 128 * 1024 * 1024;
        private boolean forceMetadataUpdates = true;
        private boolean forceWrites = true;

        public Context()
        {
            this(new Aeron.Context(), new File("archive"));
        }

        public Context(final Aeron.Context clientContext, final File archiveDir)
        {
            this.clientContext = clientContext;
            this.archiveDir = archiveDir;
            serviceRequestChannel = "aeron:udp?endpoint=localhost:8010";
            serviceRequestStreamId = 0;
            archiverNotificationsChannel = "aeron:udp?endpoint=localhost:8011";
            archiverNotificationsStreamId = 0;
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
                idleStrategy = new SleepingIdleStrategy(Aeron.IDLE_SLEEP_NS);
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

        public String serviceRequestChannel()
        {
            return serviceRequestChannel;
        }

        public Context serviceRequestChannel(final String serviceRequestChannel)
        {
            this.serviceRequestChannel = serviceRequestChannel;
            return this;
        }

        public int serviceRequestStreamId()
        {
            return serviceRequestStreamId;
        }

        public Context serviceRequestStreamId(final int serviceRequestStreamId)
        {
            this.serviceRequestStreamId = serviceRequestStreamId;
            return this;
        }

        public String archiverNotificationsChannel()
        {
            return archiverNotificationsChannel;
        }

        public Context archiverNotificationsChannel(final String archiverNotificationsChannel)
        {
            this.archiverNotificationsChannel = archiverNotificationsChannel;
            return this;
        }

        public int archiverNotificationsStreamId()
        {
            return archiverNotificationsStreamId;
        }

        public Context archiverNotificationsStreamId(final int archiverNotificationsStreamId)
        {
            this.archiverNotificationsStreamId = archiverNotificationsStreamId;
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

        public void segmentFileLength(final int segmentFileLength)
        {
            this.segmentFileLength = segmentFileLength;
        }

        boolean forceMetadataUpdates()
        {
            return forceMetadataUpdates;
        }

        public void forceMetadataUpdates(final boolean forceMetadataUpdates)
        {
            this.forceMetadataUpdates = forceMetadataUpdates;
        }

        boolean forceWrites()
        {
            return forceWrites;
        }

        public void forceWrites(final boolean forceWrites)
        {
            this.forceWrites = forceWrites;
        }
    }
}
