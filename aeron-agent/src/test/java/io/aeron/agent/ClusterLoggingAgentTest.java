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

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.Cluster.Role;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MediaDriver.Context;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;

import net.bytebuddy.agent.ByteBuddyAgent;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.MessageHandler;
import org.junit.*;

public class ClusterLoggingAgentTest
{
    private static final CountDownLatch LATCH = new CountDownLatch(1);

    private ClusteredMediaDriver clusteredMediaDriver;
    private ClusteredServiceContainer clusteredServiceContainer;
    private String nodeDirName;

    @Before
    public void before()
    {
        nodeDirName = Paths.get(IoUtil.tmpDirName(), "cluster-test").toString();
        final String aeronDirectoryName = Paths.get(nodeDirName, "media").toString();

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
            .archiveDir(new File(nodeDirName, "archive"))
            .controlChannel(aeronArchiveContext.controlRequestChannel())
            .controlStreamId(aeronArchiveContext.controlRequestStreamId())
            .localControlStreamId(aeronArchiveContext.controlRequestStreamId())
            .recordingEventsChannel(aeronArchiveContext.recordingEventsChannel())
            .threadingMode(ArchiveThreadingMode.SHARED);

        final ConsensusModule.Context consensusModuleCtx = new ConsensusModule.Context()
            .errorHandler(Throwable::printStackTrace)
            .aeronDirectoryName(aeronDirectoryName)
            .clusterDir(new File(nodeDirName, "consensus-module"))
            .archiveContext(aeronArchiveContext.clone())
            .clusterMemberId(0)
            .clusterMembers("0,localhost:20110,localhost:20220,localhost:20330,localhost:20440,localhost:8010")
            .logChannel("aeron:udp?term-length=256k|control-mode=manual|control=localhost:20550");

        clusteredMediaDriver = ClusteredMediaDriver.launch(mediaDriverCtx, archiveCtx, consensusModuleCtx);

        final ClusteredService clusteredService = new ClusteredService()
        {
            public void onStart(final Cluster cluster, final Image snapshotImage)
            {
            }

            public void onSessionOpen(final ClientSession session, final long timestampMs)
            {
            }

            public void onSessionClose(
                final ClientSession session, final long timestampMs, final CloseReason closeReason)
            {
            }

            public void onSessionMessage(
                final ClientSession session,
                final long timestampMs,
                final DirectBuffer buffer,
                final int offset,
                final int length,
                final Header header)
            {
            }

            public void onTimerEvent(final long correlationId, final long timestampMs)
            {
            }

            public void onTakeSnapshot(final Publication snapshotPublication)
            {
            }

            public void onRoleChange(final Role newRole)
            {
            }

            public void onTerminate(final Cluster cluster)
            {
            }
        };

        System.setProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME, "all");
        System.setProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME, StubEventLogReaderAgent.class.getName());
        EventLogAgent.agentmain("", ByteBuddyAgent.install());

        final ClusteredServiceContainer.Context clusteredServiceCtx = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(nodeDirName, "service"))
            .clusteredService(clusteredService);

        clusteredServiceContainer = ClusteredServiceContainer.launch(clusteredServiceCtx);
    }

    @After
    public void after()
    {
        EventLogAgent.removeTransformer();
        System.clearProperty(EventConfiguration.ENABLED_CLUSTER_EVENT_CODES_PROP_NAME);
        System.clearProperty(EventLogAgent.READER_CLASSNAME_PROP_NAME);

        CloseHelper.quietClose(clusteredServiceContainer);
        CloseHelper.quietClose(clusteredMediaDriver);
        if (nodeDirName != null)
        {
            IoUtil.delete(new File(nodeDirName), false);
        }
    }

    @Test(timeout = 10_000L)
    public void shouldLogMessages() throws Exception
    {
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
            if (ClusterEventLogger.toEventCodeId(ClusterEventCode.STATE_CHANGE) == msgTypeId)
            {
                final String stateString = buffer.getStringAscii(index, length);
                if (stateString.contains("ACTIVE"))
                {
                    LATCH.countDown();
                }
            }
        }
    }
}
