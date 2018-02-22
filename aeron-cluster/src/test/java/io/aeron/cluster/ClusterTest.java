/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

@Ignore
public class ClusterTest
{
    private static final int MEMBER_COUNT = 3;
    private static final String CLUSTER_MEMBERS = clusterMembersString();

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

            final AeronArchive.Context archiveCtx = new AeronArchive.Context()
                .controlRequestChannel("aeron:ipc?term-length=64k")
                .controlRequestStreamId(100 + i)
                .controlResponseChannel("aeron:ipc?term-length=64k")
                .controlResponseStreamId(110 + i)
                .aeronDirectoryName(baseDirName);

            drivers[i] = ClusteredMediaDriver.launch(
                new MediaDriver.Context()
                    .aeronDirectoryName(baseDirName)
                    .threadingMode(ThreadingMode.SHARED)
                    .termBufferSparseFile(true)
                    .errorHandler(Throwable::printStackTrace)
                    .dirDeleteOnStart(true),
                new Archive.Context()
                    .aeronDirectoryName(baseDirName)
                    .archiveDir(new File(baseDirName, "archive-" + i))
                    .controlChannel(memberSpecificPort(AeronArchive.Configuration.controlChannel(), i))
                    .localControlStreamId(archiveCtx.controlRequestStreamId())
                    .localControlChannel(archiveCtx.controlRequestChannel())
                    .threadingMode(ArchiveThreadingMode.SHARED)
                    .deleteArchiveOnStart(true),
                new ConsensusModule.Context()
                    .clusterMemberId(i)
                    .clusterMembers(CLUSTER_MEMBERS)
                    .aeronDirectoryName(baseDirName)
                    .clusterDir(new File(baseDirName, "consensus-module-" + i))
                    .ingressChannel("aeron:udp?term-length=64k")
                    .archiveContext(archiveCtx.clone())
                    .deleteDirOnStart(true));

            containers[i] = ClusteredServiceContainer.launch(
                new ClusteredServiceContainer.Context()
                    .aeronDirectoryName(baseDirName)
                    .archiveContext(archiveCtx.clone())
                    .clusteredServiceDir(new File(baseDirName, "service-" + i))
                    .clusteredService(new EchoService())
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

    private static String memberSpecificPort(final String channel, final int memberId)
    {
        return channel.substring(0, channel.length() - 1) + memberId;
    }

    private static String clusterMembersString()
    {
        final StringBuilder builder = new StringBuilder();

        for (int i = 0; i < MEMBER_COUNT; i++)
        {
            builder
                .append(i).append(',')
                .append("localhost:11").append(i).append(',')
                .append("localhost:22").append(i).append(',')
                .append("localhost:33").append(i).append('|');
        }

        builder.setLength(builder.length() - 1);

        return builder.toString();
    }

    static class EchoService extends StubClusteredService
    {
        public void onSessionMessage(
            final long clusterSessionId,
            final long correlationId,
            final long timestampMs,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            final ClientSession session = cluster.getClientSession(clusterSessionId);

            while (session.offer(correlationId, buffer, offset, length) < 0)
            {
                TestUtil.checkInterruptedStatus();
                Thread.yield();
            }
        }
    }
}
