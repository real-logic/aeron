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
import io.aeron.archive.client.ReplayParams;
import io.aeron.driver.MediaDriver;
import io.aeron.test.AdjustableClock;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.aeron.AeronCounters.ARCHIVE_CONTROL_SESSIONS_TYPE_ID;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(InterruptingTestCallback.class)
public class ArchiveResponseClientFailuresTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private final AdjustableClock adjustableClock = new AdjustableClock();
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
            .epochClock(adjustableClock)
            .nanoClock(adjustableClock)
            .aeronDirectoryName(driverCtx.aeronDirectoryName())
            .controlChannel("aeron:udp?endpoint=localhost:10001")
            .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
            .connectTimeoutNs(TimeUnit.SECONDS.toNanos(1))
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
    @InterruptAfter(15)
    void shouldTimeoutReplyTokenChannel()
    {
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("Unknown session or token timeout"));

        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
            .controlResponseChannel("aeron:udp?control-mode=response|control=localhost:10002");

        AeronArchive aeronArchive = null;
        try (AeronArchive.AsyncConnect asyncConnect = AeronArchive.asyncConnect(aeronArchiveCtx))
        {
            do
            {
                aeronArchive = asyncConnect.poll();
            }
            while (null == aeronArchive);

            final ArchiveSystemTests.RecordingResult recordingResult = ArchiveSystemTests.recordData(aeronArchive);

            final long tokenCorrelationId = aeronArchive.context().aeron().nextCorrelationId();
            aeronArchive.archiveProxy().requestReplayToken(
                tokenCorrelationId, aeronArchive.controlSessionId(), recordingResult.recordingId);
            while (0 == aeronArchive.controlResponsePoller().poll())
            {
                Tests.yield();
            }

            assertTrue(aeronArchive.controlResponsePoller().isPollComplete());
            final long replayToken = aeronArchive.controlResponsePoller().relevantId();

            final long timeoutNs = archive.context().connectTimeoutNs();
            final long incrementNs = TimeUnit.MILLISECONDS.toNanos(500);
            adjustableClock.slewTimeDelta(timeoutNs, incrementNs);

            final long replayCorrelationId = aeronArchive.context().aeron().nextCorrelationId();
            final ReplayParams replayParams = new ReplayParams();
            replayParams.replayToken(replayToken);

            assertTrue(aeronArchive.archiveProxy().replay(
                recordingResult.recordingId,
                "aeron:udp?control-mode=response|control=localhost:10002",
                10001,
                replayParams,
                replayCorrelationId,
                aeronArchive.controlSessionId()));

            final long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);
            while (System.currentTimeMillis() < deadlineMs && 0 == archive.context().errorCounter().get())
            {
                aeronArchive.controlResponsePoller().poll();
            }

            assertThat(archive.context().errorCounter().get(), greaterThan(0L));
        }
        finally
        {
            CloseHelper.quietClose(aeronArchive);
        }
    }

    @Test
    @InterruptAfter(15)
    void shouldRemoveTokenOnAeronArchiveClose()
    {
        systemTestWatcher.ignoreErrorsMatching(s -> s.contains("Unknown session or token timeout"));

        final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context()
            .controlRequestChannel(archive.context().controlChannel())
            .controlResponseChannel("aeron:udp?control-mode=response|control=localhost:10002");

        AeronArchive aeronArchive = null;
        try (AeronArchive.AsyncConnect asyncConnect = AeronArchive.asyncConnect(aeronArchiveCtx))
        {
            do
            {
                aeronArchive = asyncConnect.poll();
            }
            while (null == aeronArchive);

            final ArchiveSystemTests.RecordingResult recordingResult = ArchiveSystemTests.recordData(aeronArchive);

            final Aeron aeron = aeronArchive.context().aeron();
            final long tokenCorrelationId = aeron.nextCorrelationId();
            aeronArchive.archiveProxy().requestReplayToken(
                tokenCorrelationId, aeronArchive.controlSessionId(), recordingResult.recordingId);
            while (0 == aeronArchive.controlResponsePoller().poll())
            {
                Tests.yield();
            }

            assertTrue(aeronArchive.controlResponsePoller().isPollComplete());
            final long replayToken = aeronArchive.controlResponsePoller().relevantId();
            final long replayCorrelationId = aeron.nextCorrelationId();

            final int sessionCounterId = ArchiveCounters.find(
                aeron.countersReader(),
                ARCHIVE_CONTROL_SESSIONS_TYPE_ID,
                archive.context().archiveId());

            final long numSessions = aeron.countersReader().getCounterValue(sessionCounterId);

            aeronArchive.archiveProxy().closeSession(aeronArchive.controlSessionId());

            while (numSessions <= aeron.countersReader().getCounterValue(sessionCounterId))
            {
                Tests.yield();
            }

            final ReplayParams replayParams = new ReplayParams();
            replayParams.replayToken(replayToken);

            assertTrue(aeronArchive.archiveProxy().replay(
                recordingResult.recordingId,
                "aeron:udp?control-mode=response|control=localhost:10002",
                10001,
                replayParams,
                replayCorrelationId,
                aeronArchive.controlSessionId()));

            final long deadlineMs = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);
            while (System.currentTimeMillis() < deadlineMs && 0 == archive.context().errorCounter().get())
            {
                final int poll = aeronArchive.controlResponsePoller().poll();
                assertEquals(0, poll);
            }

            assertThat(archive.context().errorCounter().get(), greaterThan(0L));
        }
        finally
        {
            CloseHelper.close(aeronArchive);
        }
    }
}
