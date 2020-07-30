/*
 * Copyright 2014-2020 Real Logic Limited.
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
package io.aeron.agent;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.Tests;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static io.aeron.agent.ArchiveEventCode.*;
import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static java.util.Collections.synchronizedSet;
import static java.util.stream.Collectors.toSet;

public class ArchiveLoggingAgentTest
{
    private static final Set<Integer> LOGGED_EVENTS = synchronizedSet(new HashSet<>());
    private static final Set<Integer> WAIT_LIST = synchronizedSet(new HashSet<>());
    private static CountDownLatch latch;

    private File testDir;

    @AfterEach
    public void after()
    {
        AgentTests.afterAgent();

        LOGGED_EVENTS.clear();
        WAIT_LIST.clear();

        if (testDir != null && testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }
    }

    @Test
    @Timeout(10)
    public void logAll() throws InterruptedException
    {
        testArchiveLogging("all", EnumSet.of(CMD_OUT_RESPONSE, CMD_IN_AUTH_CONNECT, CMD_IN_KEEP_ALIVE));
    }

    @Test
    @Timeout(10)
    public void logControlSessionDemuxerOnFragment() throws InterruptedException
    {
        testArchiveLogging(CMD_IN_KEEP_ALIVE.name() + "," + CMD_IN_AUTH_CONNECT.id(),
            EnumSet.of(CMD_IN_AUTH_CONNECT, CMD_IN_KEEP_ALIVE));
    }

    @Test
    @Timeout(10)
    public void logControlResponseProxySendResponseHook() throws InterruptedException
    {
        testArchiveLogging(CMD_OUT_RESPONSE.name(), EnumSet.of(CMD_OUT_RESPONSE));
    }

    private void testArchiveLogging(final String enabledEvents, final EnumSet<ArchiveEventCode> expectedEvents)
        throws InterruptedException
    {
        before(enabledEvents, expectedEvents);

        final String aeronDirectoryName = testDir.toPath().resolve("media").toString();

        final MediaDriver.Context mediaDriverCtx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
            .aeronDirectoryName(aeronDirectoryName)
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .controlRequestChannel("aeron:udp?term-length=64k|endpoint=localhost:8010")
            .controlRequestStreamId(100)
            .controlResponseChannel("aeron:udp?term-length=64k|endpoint=localhost:8020")
            .controlResponseStreamId(101);

        final Archive.Context archiveCtx = new Archive.Context()
            .errorHandler(Tests::onError)
            .archiveDir(new File(testDir, "archive"))
            .deleteArchiveOnStart(true)
            .recordingEventsEnabled(false)
            .controlChannel(aeronArchiveContext.controlRequestChannel())
            .controlStreamId(aeronArchiveContext.controlRequestStreamId())
            .localControlStreamId(aeronArchiveContext.controlRequestStreamId())
            .recordingEventsChannel(aeronArchiveContext.recordingEventsChannel())
            .threadingMode(ArchiveThreadingMode.SHARED);

        try (ArchivingMediaDriver ignore1 = ArchivingMediaDriver.launch(mediaDriverCtx, archiveCtx))
        {
            try (AeronArchive ignore2 = AeronArchive.connect(aeronArchiveContext))
            {
                latch.await();
            }
        }
    }

    private void before(final String enabledEvents, final EnumSet<ArchiveEventCode> expectedEvents)
    {
        System.setProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME, StubEventLogReaderAgent.class.getName());
        System.setProperty(EventConfiguration.ENABLED_ARCHIVE_EVENT_CODES_PROP_NAME, enabledEvents);
        AgentTests.beforeAgent();

        latch = new CountDownLatch(expectedEvents.size());
        LOGGED_EVENTS.clear();
        WAIT_LIST.addAll(expectedEvents.stream().map(ArchiveEventLogger::toEventCodeId).collect(toSet()));

        testDir = Paths.get(IoUtil.tmpDirName(), "archive-test").toFile();
        if (testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }
    }

    static class StubEventLogReaderAgent implements Agent, MessageHandler
    {
        public String roleName()
        {
            return "event-log-reader";
        }

        public int doWork()
        {
            return EVENT_RING_BUFFER.read(this, EVENT_READER_FRAME_LIMIT);
        }

        public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
        {
            LOGGED_EVENTS.add(msgTypeId);

            if (WAIT_LIST.contains(msgTypeId) && WAIT_LIST.remove(msgTypeId))
            {
                latch.countDown();
            }
        }
    }
}
