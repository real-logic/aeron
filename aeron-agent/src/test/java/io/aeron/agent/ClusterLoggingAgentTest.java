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
package io.aeron.agent;

import static io.aeron.agent.EventConfiguration.EVENT_READER_FRAME_LIMIT;
import static io.aeron.agent.EventConfiguration.EVENT_RING_BUFFER;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.mockito.Mockito.mock;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

import net.bytebuddy.agent.ByteBuddyAgent;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.junit.*;

public class ClusterLoggingAgentTest
{
    private static final CountDownLatch LATCH = new CountDownLatch(3);

    private String testDirName;
    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer clusteredServiceContainer;

    @Before
    public void before()
    {
        System.setProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME, "all");
        System.setProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME, StubEventLogReaderAgent.class.getName());
        EventLogAgent.agentmain("", ByteBuddyAgent.install());
    }

    @After
    public void after()
    {
        EventLogAgent.removeTransformer();
        System.clearProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME);
        System.clearProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME);

        CloseHelper.close(clusteredServiceContainer);
        CloseHelper.close(clusteredMediaDriver);

        if (testDirName != null)
        {
            IoUtil.delete(new File(testDirName), false);
        }
    }

    @Test(timeout = 10_000L)
    public void shouldLogMessages() throws Exception
    {
        testDirName = Paths.get(IoUtil.tmpDirName(), "cluster-test").toString();
        final File testDir = new File(testDirName);
        if (testDir.exists())
        {
            IoUtil.delete(testDir, false);
        }

        final String aeronDirectoryName = Paths.get(testDirName, "media").toString();

        final MediaDriver.Context mediaDriverCtx = new Context()
            .aeronDirectoryName(aeronDirectoryName)
            .threadingMode(ThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .controlRequestChannel("aeron:udp?term-length=64k|endpoint=localhost:8010")
            .controlRequestStreamId(100)
            .controlResponseChannel("aeron:udp?term-length=64k|endpoint=localhost:8020")
            .controlResponseStreamId(101)
            .recordingEventsChannel("aeron:udp?control-mode=dynamic|control=localhost:8030");

        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .archiveDir(new File(testDirName, "archive"))
            .controlChannel(aeronArchiveContext.controlRequestChannel())
            .controlStreamId(aeronArchiveContext.controlRequestStreamId())
            .localControlStreamId(aeronArchiveContext.controlRequestStreamId())
            .recordingEventsChannel(aeronArchiveContext.recordingEventsChannel())
            .threadingMode(ArchiveThreadingMode.SHARED);

        final ConsensusModule.Context consensusModuleCtx = new ConsensusModule.Context()
            .errorHandler(Throwable::printStackTrace)
            .aeronDirectoryName(aeronDirectoryName)
            .clusterDir(new File(testDirName, "consensus-module"))
            .archiveContext(aeronArchiveContext.clone())
            .clusterMemberId(0)
            .clusterMembers("0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010")
            .logChannel("aeron:udp?term-length=256k|control-mode=manual|control=localhost:20550");

        final ClusteredServiceContainer.Context clusteredServiceCtx = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(testDirName, "service"))
            .clusteredService(mock(ClusteredService.class));

        clusteredMediaDriver = ClusteredMediaDriver.launch(mediaDriverCtx, archiveCtx, consensusModuleCtx);
        clusteredServiceContainer = ClusteredServiceContainer.launch(clusteredServiceCtx);

        LATCH.await();
    }

    public static class StubEventLogReaderAgent implements Agent, MessageHandler
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
            if (ClusterEventLogger.toEventCodeId(ClusterEventCode.ROLE_CHANGE) == msgTypeId)
            {
                final String roleChange = buffer.getStringAscii(index + SIZE_OF_INT);
                if (roleChange.contains("LEADER"))
                {
                    LATCH.countDown();
                }
            }

            if (ClusterEventLogger.toEventCodeId(ClusterEventCode.STATE_CHANGE) == msgTypeId)
            {
                final String stateChange = buffer.getStringAscii(index + SIZE_OF_INT);
                if (stateChange.contains("ACTIVE"))
                {
                    LATCH.countDown();
                }
            }

            if (ClusterEventLogger.toEventCodeId(ClusterEventCode.ELECTION_STATE_CHANGE) == msgTypeId)
            {
                final String stateChange = buffer.getStringAscii(index + SIZE_OF_INT);
                if (stateChange.contains("CLOSE"))
                {
                    LATCH.countDown();
                }
            }
        }
    }
}
