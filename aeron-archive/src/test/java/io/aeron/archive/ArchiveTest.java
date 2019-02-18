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
package io.aeron.archive;

import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.junit.Test;

import java.util.concurrent.*;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ArchiveTest
{
    @Test
    public void shouldGenerateRecordingName()
    {
        final long recordingId = 1L;
        final int segmentIndex = 2;
        final String expected = "1-2.rec";

        final String actual = Archive.segmentFileName(recordingId, segmentIndex);

        assertThat(actual, is(expected));
    }

    @Test
    public void shouldAllowMultipleConnectionsInParallel() throws InterruptedException
    {
        final int numberOfArchiveClients = 5;
        final CountDownLatch latch = new CountDownLatch(numberOfArchiveClients);
        final ExecutorService executorService = Executors.newFixedThreadPool(numberOfArchiveClients);
        final ManyToOneConcurrentLinkedQueue<AeronArchive> archiveClientQueue = new ManyToOneConcurrentLinkedQueue<>();
        final MediaDriver.Context driverCtx = new MediaDriver.Context();
        final Archive.Context archiveCtx = new Archive.Context();

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(driverCtx, archiveCtx))
        {
            for (int i = 0; i < numberOfArchiveClients; i++)
            {
                executorService.execute(
                    () ->
                    {
                        archiveClientQueue.add(AeronArchive.connect());
                        latch.countDown();
                    });
            }

            latch.await(10, TimeUnit.SECONDS);

            assertThat(latch.getCount(), is(0L));
        }
        finally
        {
            executorService.shutdownNow();

            AeronArchive archiveClient;
            while (null != (archiveClient = archiveClientQueue.poll()))
            {
                archiveClient.close();
            }

            archiveCtx.deleteArchiveDirectory();
            driverCtx.deleteAeronDirectory();
        }
    }
}
