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
package io.aeron.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.cluster.service.CommitPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.*;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.aeron.Aeron.NULL_VALUE;

class TestNode implements AutoCloseable
{
    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer container;
    private final TestService service;
    private final Context context;
    private boolean isClosed = false;

    TestNode(final Context context)
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            context.mediaDriverContext,
            context.archiveContext,
            context.consensusModuleContext
                .terminationHook(ClusterTests.dynamicTerminationHook(
                    context.terminationExpected, context.memberWasTerminated)));

        container = ClusteredServiceContainer.launch(
            context.serviceContainerContext
                .terminationHook(ClusterTests.dynamicTerminationHook(
                    context.terminationExpected, context.serviceWasTerminated)));

        service = context.service;
        this.context = context;
    }

    ConsensusModule consensusModule()
    {
        return clusteredMediaDriver.consensusModule();
    }

    TestService service()
    {
        return service;
    }

    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.closeAll(container, clusteredMediaDriver);
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
            if (null != clusteredMediaDriver)
            {
                clusteredMediaDriver.consensusModule().context().deleteDirectory();
                clusteredMediaDriver.archive().context().deleteDirectory();
                clusteredMediaDriver.mediaDriver().context().deleteDirectory();
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

        if (null != error)
        {
            LangUtil.rethrowUnchecked(error);
        }
    }

    Cluster.Role role()
    {
        return Cluster.Role.get((int)clusteredMediaDriver.consensusModule().context().clusterNodeRoleCounter().get());
    }

    boolean isClosed()
    {
        return isClosed;
    }

    Election.State electionState()
    {
        return Election.State.get((int)clusteredMediaDriver.consensusModule().context().electionStateCounter().get());
    }

    long commitPosition()
    {
        final MutableLong commitPosition = new MutableLong(NULL_VALUE);

        countersReader().forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                if (typeId == CommitPos.COMMIT_POSITION_TYPE_ID)
                {
                    commitPosition.value = countersReader().getCounterValue(counterId);
                }
            });

        return commitPosition.value;
    }

    long appendPosition()
    {
        final MutableLong appendPosition = new MutableLong(NULL_VALUE);

        countersReader().forEach(
            (counterId, typeId, keyBuffer, label) ->
            {
                // Doesn't work if there with multiple recordings
                if (typeId == RecordingPos.RECORDING_POSITION_TYPE_ID)
                {
                    appendPosition.value = countersReader().getCounterValue(counterId);
                }
            });

        return appendPosition.value;
    }

    boolean isLeader()
    {
        return role() == Cluster.Role.LEADER;
    }

    boolean isFollower()
    {
        return role() == Cluster.Role.FOLLOWER;
    }

    void terminationExpected(final boolean terminationExpected)
    {
        context.terminationExpected.set(terminationExpected);
    }

    boolean hasServiceTerminated()
    {
        return context.serviceWasTerminated.get();
    }

    boolean hasMemberTerminated()
    {
        return context.memberWasTerminated.get();
    }

    int index()
    {
        return service.index();
    }

    CountersReader countersReader()
    {
        return clusteredMediaDriver.mediaDriver().context().countersManager();
    }

    long errors()
    {
        return countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());
    }

    ClusterTool.ClusterMembership clusterMembership()
    {
        final ClusterTool.ClusterMembership clusterMembership = new ClusterTool.ClusterMembership();
        final File clusterDir = clusteredMediaDriver.consensusModule().context().clusterDir();

        if (!ClusterTool.listMembers(clusterMembership, clusterDir, TimeUnit.SECONDS.toMillis(3)))
        {
            throw new IllegalStateException("timeout waiting for cluster members info");
        }

        return clusterMembership;
    }

    void removeMember(final int followerMemberId, final boolean isPassive)
    {
        final File clusterDir = clusteredMediaDriver.consensusModule().context().clusterDir();

        if (!ClusterTool.removeMember(clusterDir, followerMemberId, isPassive))
        {
            throw new IllegalStateException("could not remove member");
        }
    }

    static class TestService extends StubClusteredService
    {
        public static final int SNAPSHOT_FRAGMENT_COUNT = 500;
        public static final int SNAPSHOT_MSG_LENGTH = 1000;
        private int index;
        private volatile int messageCount;
        private volatile boolean wasSnapshotTaken = false;
        private volatile boolean wasSnapshotLoaded = false;
        private volatile boolean wasOnStartCalled = false;
        private volatile Cluster.Role roleChangedTo = null;
        private volatile boolean hasReceivedUnexpectedMessage = false;

        TestService index(final int index)
        {
            this.index = index;
            return this;
        }

        int index()
        {
            return index;
        }

        int messageCount()
        {
            return messageCount;
        }

        boolean wasSnapshotTaken()
        {
            return wasSnapshotTaken;
        }

        void resetSnapshotTaken()
        {
            wasSnapshotTaken = false;
        }

        boolean wasSnapshotLoaded()
        {
            return wasSnapshotLoaded;
        }

        boolean wasOnStartCalled()
        {
            return wasOnStartCalled;
        }

        Cluster.Role roleChangedTo()
        {
            return roleChangedTo;
        }

        Cluster cluster()
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
                final MutableInteger snapshotFragmentCount = new MutableInteger();
                final FragmentHandler handler =
                    (buffer, offset, length, header) ->
                    {
                        messageCount = buffer.getInt(offset);
                        snapshotFragmentCount.value += 1;
                    };

                while (true)
                {
                    final int fragments = snapshotImage.poll(handler, 1);

                    if (snapshotImage.isClosed() || snapshotImage.isEndOfStream())
                    {
                        break;
                    }

                    idleStrategy.idle(fragments);
                }

                if (snapshotFragmentCount.get() != SNAPSHOT_FRAGMENT_COUNT)
                {
                    throw new AgentTerminationException(
                        "Unexpected snapshot length: expected=" + SNAPSHOT_FRAGMENT_COUNT +
                        " actual=" + snapshotFragmentCount);
                }

                wasSnapshotLoaded = true;
            }

            wasOnStartCalled = true;
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
            if (message.equals(TestMessages.REGISTER_TIMER))
            {
                while (!cluster.scheduleTimer(1, cluster.time() + 1_000))
                {
                    idleStrategy.idle();
                }
            }

            if (message.equals(TestMessages.POISON_MESSAGE))
            {
                hasReceivedUnexpectedMessage = true;
                throw new IllegalStateException("Poison message received.");
            }

            if (message.equals(TestMessages.ECHO_IPC_INGRESS))
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

            //noinspection NonAtomicOperationOnVolatileField
            ++messageCount;
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(SNAPSHOT_MSG_LENGTH);
            buffer.putInt(0, messageCount);

            for (int i = 0; i < SNAPSHOT_FRAGMENT_COUNT; i++)
            {
                idleStrategy.reset();
                while (snapshotPublication.offer(buffer, 0, SNAPSHOT_MSG_LENGTH) <= 0)
                {
                    idleStrategy.idle();
                }
            }

            wasSnapshotTaken = true;
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
        final AtomicBoolean terminationExpected = new AtomicBoolean(false);
        final AtomicBoolean memberWasTerminated = new AtomicBoolean(false);
        final AtomicBoolean serviceWasTerminated = new AtomicBoolean(false);
        final TestService service;

        Context(final TestService service)
        {
            this.service = service;
        }
    }
}
