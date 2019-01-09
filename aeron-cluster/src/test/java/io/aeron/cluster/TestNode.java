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
package io.aeron.cluster;

import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.agrona.BitUtil.SIZE_OF_INT;

class TestNode implements AutoCloseable
{
    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer container;
    private final TestService service;
    private final TestNodeContext context;
    private boolean isClosed = false;

    TestNode(final TestNodeContext context)
    {
        clusteredMediaDriver = ClusteredMediaDriver.launch(
            context.mediaDriverContext,
            context.archiveContext,
            context.consensusModuleContext
                .terminationHook(TestUtil.dynamicTerminationHook(
                    context.terminationExpected, context.memberWasTerminated)));

        container = ClusteredServiceContainer.launch(
            context.serviceContainerContext
                .terminationHook(TestUtil.dynamicTerminationHook(
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
            CloseHelper.close(clusteredMediaDriver);
            CloseHelper.close(container);

            if (null != clusteredMediaDriver)
            {
                clusteredMediaDriver.mediaDriver().context().deleteAeronDirectory();
            }

            isClosed = true;
        }
    }

    void cleanUp()
    {
        if (null != clusteredMediaDriver)
        {
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
            clusteredMediaDriver.archive().context().deleteArchiveDirectory();
        }

        if (null != container)
        {
            container.context().deleteDirectory();
        }
    }

    Cluster.Role role()
    {
        return Cluster.Role.get((int)clusteredMediaDriver.consensusModule().context().clusterNodeCounter().get());
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
        context.terminationExpected.lazySet(terminationExpected);
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

    static class TestService extends StubClusteredService
    {
        private volatile int messageCount;
        private volatile boolean wasSnapshotTaken = false;
        private volatile boolean wasSnapshotLoaded = false;
        private final int index;

        TestService(final int index)
        {
            this.index = index;
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

        boolean wasSnapshotLoaded()
        {
            return wasSnapshotLoaded;
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestampMs,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            while (session.offer(buffer, offset, length) < 0)
            {
                cluster.idle();
            }

            ++messageCount;
        }

        public void onTakeSnapshot(final Publication snapshotPublication)
        {
            final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

            int length = 0;
            buffer.putInt(length, messageCount);
            length += SIZE_OF_INT;

            snapshotPublication.offer(buffer, 0, length);
            wasSnapshotTaken = true;
        }

        public void onLoadSnapshot(final Image snapshotImage)
        {
            while (true)
            {
                final int fragments = snapshotImage.poll(
                    (buffer, offset, length, header) ->
                    {
                        messageCount = buffer.getInt(offset);
                    },
                    1);

                if (fragments == 1)
                {
                    break;
                }

                cluster.idle();
            }

            wasSnapshotLoaded = true;
        }
    }

    static class TestNodeContext
    {
        public final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        public final Archive.Context archiveContext = new Archive.Context();
        public final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        public final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context();
        public final ClusteredServiceContainer.Context serviceContainerContext =
            new ClusteredServiceContainer.Context();
        public final AtomicBoolean terminationExpected = new AtomicBoolean(false);
        public final AtomicBoolean memberWasTerminated = new AtomicBoolean(false);
        public final AtomicBoolean serviceWasTerminated = new AtomicBoolean(false);
        public TestService service = null;
    }
}
