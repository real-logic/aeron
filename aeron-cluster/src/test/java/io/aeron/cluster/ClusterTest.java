/*
 * Copyright 2018 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

@Ignore
public class ClusterTest
{
    private static final int MEMBER_COUNT = 3;

    private ClusteredMediaDriver[] drivers = new ClusteredMediaDriver[MEMBER_COUNT];
    private ClusteredServiceContainer[] containers = new ClusteredServiceContainer[MEMBER_COUNT];

    private AeronCluster client;

    @Before
    public void before()
    {
        final String aeronDirName = CommonContext.getAeronDirectoryName();

        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            final String baseDirName = aeronDirName + "-" + i;

            drivers[i] = ClusteredMediaDriver.launch(
                new MediaDriver.Context()
                    .aeronDirectoryName(baseDirName)
                    .threadingMode(ThreadingMode.SHARED)
                    .termBufferSparseFile(true)
                    .errorHandler(Throwable::printStackTrace)
                    .dirDeleteOnStart(true),
                new Archive.Context()
                    .aeronDirectoryName(baseDirName)
                    .archiveDir(new File(baseDirName, "-archive"))
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .deleteArchiveOnStart(true),
                new ConsensusModule.Context()
                    .aeronDirectoryName(baseDirName)
                    .clusterDir(new File(baseDirName, "-cm"))
                    .deleteDirOnStart(true));

            containers[i] = ClusteredServiceContainer.launch(
                new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(baseDirName)
                    //.clusteredService(echoScheduledService)
                    .errorHandler(Throwable::printStackTrace)
                    .deleteDirOnStart(true));

        }
    }

    @After
    public void after()
    {
    }

    @Test
    public void shouldConnectAndSendKeepAlive()
    {
    }
}
