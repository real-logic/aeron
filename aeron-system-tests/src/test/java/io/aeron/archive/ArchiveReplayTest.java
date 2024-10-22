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
import io.aeron.ChannelUri;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ReplayParams;
import io.aeron.driver.MediaDriver;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;

import static io.aeron.CommonContext.IPC_CHANNEL;

public class ArchiveReplayTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher()
        .ignoreErrorsMatching(s -> s.contains("response publication is closed"));

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
        driverCtx.enableExperimentalFeatures(true);

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
    void shouldNotErrorOnReplayThatHasAlreadyStopped()
    {
        try (AeronArchive aeronArchive = AeronArchive.connect(TestContexts.ipcAeronArchive()))
        {
            final ArchiveSystemTests.RecordingResult recordingResult = ArchiveSystemTests.recordData(aeronArchive);

            final Aeron aeron = aeronArchive.context().aeron();
            final int replayStreamId = 10001;

            final long replaySessionId = aeronArchive.startReplay(
                recordingResult.recordingId, IPC_CHANNEL, replayStreamId, new ReplayParams());

            final String replayChannel = ChannelUri.addSessionId(IPC_CHANNEL, (int)replaySessionId);
            final Subscription replay = aeron.addSubscription(replayChannel, replayStreamId);

            final MutableLong replayPosition = new MutableLong();
            while (replayPosition.get() < recordingResult.position / 2)
            {
                if (0 == replay.poll((buffer, offset, length, header) -> replayPosition.set(header.position()), 10))
                {
                    Tests.yield();
                }
            }

            CloseHelper.quietClose(replay);
            aeronArchive.stopReplay(replaySessionId);
        }
    }
}
