/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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
import io.aeron.Image;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.ShutdownSignalBarrier;

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
        try (Archiver archiver = Archiver.launch())
        {
            new ShutdownSignalBarrier().await();
            System.out.println("Shutdown Archiver...");
        }
    }


    public void close() throws Exception
    {
        runner.close();
        aeron.close();
    }

    public void start()
    {
        final ManyToOneConcurrentArrayQueue<Image> imageNotifications = new ManyToOneConcurrentArrayQueue<>(1024);

        // TODO: needs to give up on some isRunning flag
        ctx.clientContext.availableImageHandler(image ->
        {
            while (!imageNotifications.offer(image))
            {
                ctx.clientContext.idleStrategy().idle(0);
            }
        });

        aeron = Aeron.connect(ctx.clientContext);
        /* TODO: should we have replay and record on same thread?
           TODO: should we allow the allocation of more threads to these tasks assuming slow storage/sufficient
                 traffic/sufficient replay load?*/
        final ArchiverConductor archiverConductor = new ArchiverConductor(aeron, imageNotifications, ctx);

        runner = new AgentRunner(
            ctx.clientContext.idleStrategy(),
            ctx.clientContext.errorHandler(),
            null,
            archiverConductor);
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
        Aeron.Context clientContext;
        File archiveFolder;
        private String serviceRequestChannel;
        private int serviceRequestStreamId;
        private String archiverNotificationsChannel;
        private int archiverNotificationsStreamId;

        public Context()
        {
            // TODO: parametrize port/host/interface
            // TODO: Allow running with existing driver
            final File archiveDir = new File("archive");
            if (!archiveDir.exists())
            {
                archiveDir.mkdirs();
            }
            archiveFolder = archiveDir;
            clientContext = new Aeron.Context();
            serviceRequestChannel = "aeron:udp?endpoint=localhost:8010";
            serviceRequestStreamId = 0;
            archiverNotificationsChannel = "aeron:udp?endpoint=localhost:8011";
            archiverNotificationsStreamId = 0;
        }

        public Context(final Aeron.Context clientContext, final File archiveFolder)
        {
            this.clientContext = clientContext;
            this.archiveFolder = archiveFolder;
        }

        public File archiveFolder()
        {
            return archiveFolder;
        }

        public Context archiveFolder(final File f)
        {
            this.archiveFolder = f;
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
    }
}
