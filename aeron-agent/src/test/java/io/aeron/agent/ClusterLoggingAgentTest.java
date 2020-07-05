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
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.*;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static io.aeron.agent.ClusterEventCode.*;
import static io.aeron.agent.ClusterEventLogger.toEventCodeId;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static java.util.Collections.synchronizedSet;
import static java.util.stream.Collectors.toSet;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class ClusterLoggingAgentTest
{
    private static final Set<Integer> LOGGED_EVENTS = synchronizedSet(new HashSet<>());
    private static CountDownLatch latch;

    private File testDir;
    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer container;

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(clusteredMediaDriver.consensusModule(), container, clusteredMediaDriver);
        AgentTests.afterAgent();

        if (testDir != null && testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }
    }

    @Test
    @Timeout(20)
    public void logAll() throws InterruptedException
    {
        testClusterEventsLogging("all", EnumSet.of(ROLE_CHANGE, STATE_CHANGE, ELECTION_STATE_CHANGE));
    }

    @Test
    @Timeout(20)
    public void logRoleChange() throws InterruptedException
    {
        testClusterEventsLogging(ROLE_CHANGE.name(), EnumSet.of(ROLE_CHANGE));
    }

    @Test
    @Timeout(20)
    public void logStateChange() throws InterruptedException
    {
        testClusterEventsLogging(STATE_CHANGE.name(), EnumSet.of(STATE_CHANGE));
    }

    @Test
    @Timeout(20)
    public void logElectionStateChange() throws InterruptedException
    {
        testClusterEventsLogging(ELECTION_STATE_CHANGE.name(), EnumSet.of(ELECTION_STATE_CHANGE));
    }

    private void testClusterEventsLogging(
        final String enabledEvents, final EnumSet<ClusterEventCode> expectedEvents) throws InterruptedException
    {
        before(enabledEvents, expectedEvents.size());

        final String aeronDirectoryName = testDir.toPath().resolve("media").toString();

        final Context mediaDriverCtx = new Context()
            .errorHandler(Tests::onError)
            .aeronDirectoryName(aeronDirectoryName)
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .controlRequestChannel("aeron:udp?term-length=64k|endpoint=localhost:8010")
            .controlRequestStreamId(100)
            .controlResponseChannel("aeron:udp?term-length=64k|endpoint=localhost:8020")
            .controlResponseStreamId(101)
            .recordingEventsChannel("aeron:udp?control-mode=dynamic|control=localhost:8030");

        final Archive.Context archiveCtx = new Archive.Context()
            .errorHandler(Tests::onError)
            .archiveDir(new File(testDir, "archive"))
            .deleteArchiveOnStart(true)
            .controlChannel(aeronArchiveContext.controlRequestChannel())
            .controlStreamId(aeronArchiveContext.controlRequestStreamId())
            .localControlStreamId(aeronArchiveContext.controlRequestStreamId())
            .recordingEventsChannel(aeronArchiveContext.recordingEventsChannel())
            .threadingMode(ArchiveThreadingMode.SHARED);

        final ConsensusModule.Context consensusModuleCtx = new ConsensusModule.Context()
            .errorHandler(Tests::onError)
            .clusterDir(new File(testDir, "consensus-module"))
            .archiveContext(aeronArchiveContext.clone())
            .clusterMemberId(0)
            .clusterMembers("0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010")
            .logChannel("aeron:udp?term-length=256k|control-mode=manual|control=localhost:20550");

        final ClusteredService clusteredService = mock(ClusteredService.class);
        final ClusteredServiceContainer.Context clusteredServiceCtx = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .errorHandler(Tests::onError)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(testDir, "service"))
            .clusteredService(clusteredService);

        clusteredMediaDriver = ClusteredMediaDriver.launch(mediaDriverCtx, archiveCtx, consensusModuleCtx);
        container = ClusteredServiceContainer.launch(clusteredServiceCtx);

        latch.await();
        verify(clusteredService, timeout(5000)).onRoleChange(eq(Cluster.Role.LEADER));

        final Set<Integer> expected = expectedEvents
            .stream()
            .map(ClusterEventLogger::toEventCodeId)
            .collect(toSet());

        assertEquals(expected, LOGGED_EVENTS);
    }

    private void before(final String enabledEvents, final int expectedEvents)
    {
        System.setProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME, StubEventLogReaderAgent.class.getName());
        System.setProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME, enabledEvents);
        AgentTests.beforeAgent();

        LOGGED_EVENTS.clear();
        latch = new CountDownLatch(expectedEvents);

        testDir = new File(IoUtil.tmpDirName(), "cluster-test");
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

            final int offset = LOG_HEADER_LENGTH + index + SIZE_OF_INT;
            if (toEventCodeId(ROLE_CHANGE) == msgTypeId)
            {
                final String roleChange = buffer.getStringAscii(offset);
                if (roleChange.contains("LEADER"))
                {
                    latch.countDown();
                }
            }
            else if (toEventCodeId(STATE_CHANGE) == msgTypeId)
            {
                final String stateChange = buffer.getStringAscii(offset);
                if (stateChange.contains("ACTIVE"))
                {
                    latch.countDown();
                }
            }
            else if (toEventCodeId(ELECTION_STATE_CHANGE) == msgTypeId)
            {
                final String stateChange = buffer.getStringAscii(offset);
                if (stateChange.contains("CLOSED"))
                {
                    latch.countDown();
                }
            }
        }
    }
}
