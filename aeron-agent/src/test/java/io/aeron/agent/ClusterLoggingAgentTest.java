/*
 * Copyright 2014-2021 Real Logic Limited.
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

import io.aeron.Counter;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.EnumSet;
import java.util.Set;

import static io.aeron.agent.ClusterEventCode.*;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static java.util.Collections.synchronizedSet;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.mockito.Mockito.mock;

public class ClusterLoggingAgentTest
{
    private static final Set<ClusterEventCode> WAIT_LIST = synchronizedSet(EnumSet.noneOf(ClusterEventCode.class));

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
    public void logAll()
    {
        testClusterEventsLogging("all", EnumSet.of(ROLE_CHANGE, STATE_CHANGE, ELECTION_STATE_CHANGE));
    }

    @Test
    @Timeout(20)
    public void logRoleChange()
    {
        testClusterEventsLogging(ROLE_CHANGE.name(), EnumSet.of(ROLE_CHANGE));
    }

    @Test
    @Timeout(20)
    public void logStateChange()
    {
        testClusterEventsLogging(STATE_CHANGE.name(), EnumSet.of(STATE_CHANGE));
    }

    @Test
    @Timeout(20)
    public void logElectionStateChange()
    {
        testClusterEventsLogging(ELECTION_STATE_CHANGE.name(), EnumSet.of(ELECTION_STATE_CHANGE));
    }

    private void testClusterEventsLogging(
        final String enabledEvents, final EnumSet<ClusterEventCode> expectedEvents)
    {
        before(enabledEvents, expectedEvents);

        final Context mediaDriverCtx = new Context()
            .errorHandler(Tests::onError)
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .controlRequestChannel("aeron:ipc?term-length=64k")
            .controlRequestStreamId(AeronArchive.Configuration.localControlStreamId())
            .controlResponseChannel("aeron:ipc?term-length=64k")
            .controlResponseStreamId(AeronArchive.Configuration.localControlStreamId() + 1)
            .controlResponseStreamId(101);

        final Archive.Context archiveCtx = new Archive.Context()
            .errorHandler(Tests::onError)
            .archiveDir(new File(testDir, "archive"))
            .deleteArchiveOnStart(true)
            .recordingEventsEnabled(false)
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
            .errorHandler(Tests::onError)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(testDir, "service"))
            .clusteredService(clusteredService);

        clusteredMediaDriver = ClusteredMediaDriver.launch(mediaDriverCtx, archiveCtx, consensusModuleCtx);
        container = ClusteredServiceContainer.launch(clusteredServiceCtx);

        Tests.await(WAIT_LIST::isEmpty);

        final Counter state = clusteredMediaDriver.consensusModule().context().electionStateCounter();
        while (ElectionState.CLOSED != ElectionState.get(state))
        {
            Tests.sleep(1);
        }
    }

    private void before(final String enabledEvents, final EnumSet<ClusterEventCode> expectedEvents)
    {
        System.setProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME, StubEventLogReaderAgent.class.getName());
        System.setProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME, enabledEvents);
        AgentTests.beforeAgent();

        WAIT_LIST.clear();
        WAIT_LIST.addAll(expectedEvents);

        testDir = new File(IoUtil.tmpDirName(), "cluster-test");
        if (testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }
    }

    static final class StubEventLogReaderAgent implements Agent, MessageHandler
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
            final ClusterEventCode eventCode = fromEventCodeId(msgTypeId);
            final int offset = LOG_HEADER_LENGTH + index + SIZE_OF_INT;
            switch (eventCode)
            {
                case ROLE_CHANGE:
                    final String roleChange = buffer.getStringAscii(offset);
                    if (roleChange.contains("LEADER"))
                    {
                        WAIT_LIST.remove(eventCode);
                    }
                    break;
                case STATE_CHANGE:
                {
                    final String stateChange = buffer.getStringAscii(offset);
                    if (stateChange.contains("ACTIVE"))
                    {
                        WAIT_LIST.remove(eventCode);
                    }
                    break;
                }
                case ELECTION_STATE_CHANGE:
                {
                    final String stateChange = buffer.getStringAscii(offset);
                    if (stateChange.contains("CLOSED"))
                    {
                        WAIT_LIST.remove(eventCode);
                    }
                    break;
                }
            }
        }
    }
}
