/*
 * Copyright 2014-2023 Real Logic Limited.
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
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ArchiveResponseClientTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Archive archive;

    @BeforeEach
    void setUp()
    {
        final int termLength = 64 * 1024;

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .termBufferSparseFile(true)
            .publicationTermBufferLength(termLength)
            .sharedIdleStrategy(YieldingIdleStrategy.INSTANCE)
            .spiesSimulateConnection(true)
            .dirDeleteOnStart(true);

        final Archive.Context archiveContext = TestContexts.localhostArchive()
            .aeronDirectoryName(driverCtx.aeronDirectoryName())
            .controlChannel("aeron:udp?endpoint=localhost:10001")
            .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
            .fileSyncLevel(0)
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "archive-test"))
            .segmentFileLength(1024 * 1024)
            .idleStrategySupplier(YieldingIdleStrategy::new);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());

        archive = Archive.launch(archiveContext);
        systemTestWatcher.dataCollector().add(archiveContext.archiveDir());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.quietCloseAll(archive);
        CloseHelper.quietCloseAll(driver);
    }

    @Test
    @Disabled
    void shouldConnectUsingResponseChannel()
    {
        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
            .controlResponseChannel("aeron:udp?control-mode=response|control=localhost:10002");
        try (AeronArchive aeronArchive = AeronArchive.connect(aeronArchiveCtx))
        {
            Objects.requireNonNull(aeronArchive);
        }
    }

    @Test
    void shouldAsyncConnectUsingResponseChannel()
    {
        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
            .controlResponseChannel("aeron:udp?control-mode=response|control=localhost:10002");
        try (AeronArchive.AsyncConnect asyncConnect = AeronArchive.asyncConnect(aeronArchiveCtx))
        {
            AeronArchive aeronArchive;
            while (null == (aeronArchive = asyncConnect.poll()))
            {
                Tests.yield();
            }
            assertNotEquals(Aeron.NULL_VALUE, aeronArchive.controlSessionId());

            CloseHelper.close(aeronArchive);
        }
    }

    @Test
    void shouldAsyncConnectUsingChannel()
    {
        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
//            .controlResponseChannel("aeron:udp?control-mode=response|control=localhost:10002");
            .controlResponseChannel("aeron:udp?endpoint=localhost:10002");
        try (AeronArchive.AsyncConnect asyncConnect = AeronArchive.asyncConnect(aeronArchiveCtx))
        {
            AeronArchive aeronArchive;
            while (null == (aeronArchive = asyncConnect.poll()))
            {
                Tests.yield();
            }
            assertNotEquals(Aeron.NULL_VALUE, aeronArchive.controlSessionId());

            CloseHelper.close(aeronArchive);
        }
    }
}
