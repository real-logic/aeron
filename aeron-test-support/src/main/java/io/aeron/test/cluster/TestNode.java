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
package io.aeron.test.cluster;

import io.aeron.Counter;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.*;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.DataCollector;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.jupiter.api.Assertions.fail;

public class TestNode implements AutoCloseable
{
    private final ClusteredArchive clusteredArchive;
    private final ClusteredServiceContainer container;
    private final TestService service;
    private final Context context;
    private final TestMediaDriver mediaDriver;
    private boolean isClosed = false;

    TestNode(final Context context, final DataCollector dataCollector)
    {
        mediaDriver = TestMediaDriver.launch(context.mediaDriverContext, null);

        clusteredArchive = ClusteredArchive.launch(
            mediaDriver.aeronDirectoryName(),
            context.archiveContext,
            context.consensusModuleContext.terminationHook(ClusterTests.terminationHook(
                context.isTerminationExpected, context.hasMemberTerminated)));

        container = ClusteredServiceContainer.launch(
            context.serviceContainerContext
                .terminationHook(ClusterTests.terminationHook(
                    context.isTerminationExpected, context.hasServiceTerminated)));

        service = context.service;
        this.context = context;

        dataCollector.add(container.context().clusterDir().toPath());
        dataCollector.add(clusteredArchive.consensusModule().context().clusterDir().toPath());
        dataCollector.add(clusteredArchive.archive().context().archiveDir().toPath());
        dataCollector.add(mediaDriver.context().aeronDirectory().toPath());
    }

    public TestMediaDriver mediaDriver()
    {
        return mediaDriver;
    }

    public Archive archive()
    {
        return clusteredArchive.archive();
    }

    public ConsensusModule consensusModule()
    {
        return clusteredArchive.consensusModule();
    }

    public ClusteredServiceContainer container()
    {
        return container;
    }

    public TestService service()
    {
        return service;
    }

    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.closeAll(clusteredArchive.consensusModule(), container, clusteredArchive, mediaDriver);
        }
    }

    void closeAndDelete()
    {
        Throwable error = null;

        try
        {
            if (!isClosed)
            {
                close();
            }
        }
        catch (final Throwable t)
        {
            error = t;
        }

        try
        {
            if (null != container)
            {
                container.context().deleteDirectory();
            }
        }
        catch (final Throwable t)
        {
            if (error == null)
            {
                error = t;
            }
            else
            {
                error.addSuppressed(t);
            }
        }

        try
        {
            if (null != clusteredArchive)
            {
                clusteredArchive.consensusModule().context().deleteDirectory();
                clusteredArchive.archive().context().deleteDirectory();
                mediaDriver.context().deleteDirectory();
            }
        }
        catch (final Throwable t)
        {
            if (null == error)
            {
                error = t;
            }
            else
            {
                error.addSuppressed(t);
            }
        }

        if (null != error)
        {
            LangUtil.rethrowUnchecked(error);
        }
    }

    boolean isClosed()
    {
        return isClosed;
    }

    public Cluster.Role role()
    {
        final Counter counter = clusteredArchive.consensusModule().context().clusterNodeRoleCounter();
        if (counter.isClosed())
        {
            return Cluster.Role.FOLLOWER;
        }

        return Cluster.Role.get(counter);
    }

    ElectionState electionState()
    {
        final Counter counter = clusteredArchive.consensusModule().context().electionStateCounter();
        if (counter.isClosed())
        {
            return ElectionState.CLOSED;
        }

        return ElectionState.get(counter);
    }

    ConsensusModule.State moduleState()
    {
        final Counter counter = clusteredArchive.consensusModule().context().moduleStateCounter();
        if (counter.isClosed())
        {
            return ConsensusModule.State.CLOSED;
        }

        return ConsensusModule.State.get(counter);
    }

    public long commitPosition()
    {
        final Counter counter = clusteredArchive.consensusModule().context().commitPositionCounter();
        if (counter.isClosed())
        {
            return NULL_POSITION;
        }

        return counter.get();
    }

    public long appendPosition()
    {
        final long recordingId = consensusModule().context().recordingLog().findLastTermRecordingId();
        if (RecordingPos.NULL_RECORDING_ID == recordingId)
        {
            fail("no recording for last term");
        }

        final CountersReader countersReader = countersReader();
        final int counterId = RecordingPos.findCounterIdByRecording(countersReader, recordingId);
        if (NULL_VALUE == counterId)
        {
            fail("recording not active " + recordingId);
        }

        return countersReader.getCounterValue(counterId);
    }

    boolean isLeader()
    {
        return role() == Cluster.Role.LEADER && moduleState() != ConsensusModule.State.CLOSED;
    }

    boolean isFollower()
    {
        return role() == Cluster.Role.FOLLOWER;
    }

    public void isTerminationExpected(final boolean isTerminationExpected)
    {
        context.isTerminationExpected.set(isTerminationExpected);
    }

    boolean hasServiceTerminated()
    {
        return context.hasServiceTerminated.get();
    }

    public boolean hasMemberTerminated()
    {
        return context.hasMemberTerminated.get();
    }

    public int index()
    {
        return service.index();
    }

    CountersReader countersReader()
    {
        return mediaDriver.counters();
    }

    public long errors()
    {
        return countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());
    }

    public ClusterMembership clusterMembership()
    {
        final ClusterMembership clusterMembership = new ClusterMembership();
        final File clusterDir = clusteredArchive.consensusModule().context().clusterDir();

        if (!ClusterTool.listMembers(clusterMembership, clusterDir, TimeUnit.SECONDS.toMillis(3)))
        {
            throw new IllegalStateException("timeout waiting for cluster members info");
        }

        return clusterMembership;
    }

    public void removeMember(final int followerMemberId, final boolean isPassive)
    {
        final File clusterDir = clusteredArchive.consensusModule().context().clusterDir();

        if (!ClusterTool.removeMember(clusterDir, followerMemberId, isPassive))
        {
            throw new IllegalStateException("could not remove member");
        }
    }

    public static class TestService extends StubClusteredService
    {
        static final int SNAPSHOT_FRAGMENT_COUNT = 500;
        static final int SNAPSHOT_MSG_LENGTH = 1000;

        private int index;
        private volatile boolean wasSnapshotTaken = false;
        private volatile boolean wasSnapshotLoaded = false;
        private volatile boolean hasReceivedUnexpectedMessage = false;
        private volatile Cluster.Role roleChangedTo = null;
        private final AtomicInteger activeSessionCount = new AtomicInteger();
        private final AtomicInteger messageCount = new AtomicInteger();

        TestService index(final int index)
        {
            this.index = index;
            return this;
        }

        int index()
        {
            return index;
        }

        int activeSessionCount()
        {
            return activeSessionCount.get();
        }

        public int messageCount()
        {
            return messageCount.get();
        }

        public boolean wasSnapshotTaken()
        {
            return wasSnapshotTaken;
        }

        public void resetSnapshotTaken()
        {
            wasSnapshotTaken = false;
        }

        public boolean wasSnapshotLoaded()
        {
            return wasSnapshotLoaded;
        }

        public Cluster.Role roleChangedTo()
        {
            return roleChangedTo;
        }

        public Cluster cluster()
        {
            return cluster;
        }

        boolean hasReceivedUnexpectedMessage()
        {
            return hasReceivedUnexpectedMessage;
        }

        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            super.onStart(cluster, snapshotImage);

            if (null != snapshotImage)
            {
                activeSessionCount.set(cluster.clientSessions().size());

                final FragmentHandler handler =
                    (buffer, offset, length, header) -> messageCount.set(buffer.getInt(offset));

                int fragmentCount = 0;
                while (true)
                {
                    final int fragments = snapshotImage.poll(handler, 10);
                    fragmentCount += fragments;

                    if (snapshotImage.isClosed() || snapshotImage.isEndOfStream())
                    {
                        break;
                    }

                    idleStrategy.idle(fragments);
                }

                if (fragmentCount != SNAPSHOT_FRAGMENT_COUNT)
                {
                    throw new AgentTerminationException(
                        "unexpected snapshot length: expected=" + SNAPSHOT_FRAGMENT_COUNT + " actual=" + fragmentCount);
                }

                wasSnapshotLoaded = true;
            }
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            final String message = buffer.getStringWithoutLengthAscii(offset, length);
            if (message.equals(ClusterTests.REGISTER_TIMER_MSG))
            {
                while (!cluster.scheduleTimer(1, cluster.time() + 1_000))
                {
                    idleStrategy.idle();
                }
            }

            if (message.equals(ClusterTests.UNEXPECTED_MSG))
            {
                hasReceivedUnexpectedMessage = true;
                throw new IllegalStateException("unexpected message received");
            }

            if (message.equals(ClusterTests.ECHO_IPC_INGRESS_MSG))
            {
                if (null != session)
                {
                    while (cluster.offer(buffer, offset, length) < 0)
                    {
                        idleStrategy.idle();
                    }
                }
                else
                {
                    for (final ClientSession clientSession : cluster.clientSessions())
                    {
                        while (clientSession.offer(buffer, offset, length) < 0)
                        {
                            idleStrategy.idle();
                        }
                    }
                }
            }
            else
            {
                if (null != session)
                {
                    while (session.offer(buffer, offset, length) < 0)
                    {
                        idleStrategy.idle();
                    }
                }
            }

            messageCount.incrementAndGet();
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[SNAPSHOT_MSG_LENGTH]);
            buffer.putInt(0, messageCount.get());
            buffer.putInt(SNAPSHOT_MSG_LENGTH - SIZE_OF_INT, messageCount.get());

            for (int i = 0; i < SNAPSHOT_FRAGMENT_COUNT; i++)
            {
                idleStrategy.reset();
                while (snapshotPublication.offer(buffer, 0, SNAPSHOT_MSG_LENGTH) < 0)
                {
                    idleStrategy.idle();
                }
            }

            wasSnapshotTaken = true;
        }

        public void onSessionOpen(final ClientSession session, final long timestamp)
        {
            super.onSessionOpen(session, timestamp);
            activeSessionCount.incrementAndGet();
        }

        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
        {
            super.onSessionClose(session, timestamp, closeReason);
            activeSessionCount.decrementAndGet();
        }

        public void onRoleChange(final Cluster.Role newRole)
        {
            roleChangedTo = newRole;
        }
    }

    static class Context
    {
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context();
        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context();
        final ClusteredServiceContainer.Context serviceContainerContext = new ClusteredServiceContainer.Context();
        final AtomicBoolean isTerminationExpected = new AtomicBoolean();
        final AtomicBoolean hasMemberTerminated = new AtomicBoolean();
        final AtomicBoolean hasServiceTerminated = new AtomicBoolean();
        final TestService service;

        Context(final TestService service)
        {
            this.service = service;
        }
    }
}
