/*
 * Copyright 2014-2022 Real Logic Limited.
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
import io.aeron.FragmentAssembler;
import io.aeron.Image;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.ClusterMembership;
import io.aeron.cluster.ClusterTool;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.ElectionState;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.test.DataCollector;
import io.aeron.test.driver.RedirectingNameResolver;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.Hashing;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.AgentTerminationException;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.junit.jupiter.api.Assertions.fail;

public final class TestNode implements AutoCloseable
{
    private final Archive archive;
    private final ConsensusModule consensusModule;
    private final ClusteredServiceContainer[] containers;
    private final TestService[] services;
    private final Context context;
    private final TestMediaDriver mediaDriver;
    private boolean isClosed = false;

    TestNode(final Context context, final DataCollector dataCollector)
    {
        this.context = context;

        try
        {
            mediaDriver = TestMediaDriver.launch(
                context.mediaDriverContext, TestCluster.clientDriverOutputConsumer(dataCollector));

            final String aeronDirectoryName = mediaDriver.context().aeronDirectoryName();
            archive = Archive.launch(context.archiveContext.aeronDirectoryName(aeronDirectoryName));

            services = context.services;

            context.consensusModuleContext
                .serviceCount(services.length)
                .aeronDirectoryName(aeronDirectoryName)
                .isIpcIngressAllowed(true)
                .terminationHook(ClusterTests.terminationHook(
                context.isTerminationExpected, context.hasMemberTerminated));
            consensusModule = ConsensusModule.launch(context.consensusModuleContext);

            containers = new ClusteredServiceContainer[services.length];
            final File baseDir = context.consensusModuleContext.clusterDir().getParentFile();
            for (int i = 0; i < services.length; i++)
            {
                final File clusterDir = new File(baseDir, "service" + i);
                final ClusteredServiceContainer.Context ctx = context.serviceContainerContext.clone();
                ctx.aeronDirectoryName(aeronDirectoryName)
                    .archiveContext(context.aeronArchiveContext.clone()
                        .controlRequestChannel("aeron:ipc")
                        .controlResponseChannel("aeron:ipc"))
                    .terminationHook(ClusterTests.terminationHook(
                        context.isTerminationExpected, context.hasServiceTerminated[i]))
                    .clusterDir(clusterDir)
                    .clusteredService(services[i])
                    .serviceId(i);
                containers[i] = ClusteredServiceContainer.launch(ctx);
                dataCollector.add(clusterDir.toPath());
            }

            dataCollector.add(consensusModule.context().clusterDir().toPath());
            dataCollector.add(archive.context().archiveDir().toPath());
            dataCollector.add(mediaDriver.context().aeronDirectory().toPath());
        }
        catch (final RuntimeException ex)
        {
            try
            {
                close();
            }
            catch (final Exception e)
            {
                ex.addSuppressed(e);
            }
            throw ex;
        }
    }

    public void stopServiceContainers()
    {
        CloseHelper.closeAll(containers);
        for (final AtomicBoolean terminationFlag : context.hasServiceTerminated)
        {
            terminationFlag.set(true);
        }
    }

    public TestMediaDriver mediaDriver()
    {
        return mediaDriver;
    }

    public Archive archive()
    {
        return archive;
    }

    public ConsensusModule consensusModule()
    {
        return consensusModule;
    }

    public ClusteredServiceContainer container()
    {
        if (1 != containers.length)
        {
            throw new IllegalStateException("container count expected=1 actual=" + containers.length);
        }

        return containers[0];
    }

    public TestService service()
    {
        if (1 != services.length)
        {
            throw new IllegalStateException("111 service count expected=1 actual=" + services.length);
        }

        return services[0];
    }

    public TestService[] services()
    {
        return services;
    }

    public void close()
    {
        if (!isClosed)
        {
            isClosed = true;
            CloseHelper.closeAll(consensusModule, () -> CloseHelper.closeAll(containers), archive, mediaDriver);
        }
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    public Cluster.Role role()
    {
        final Counter roleCounter = consensusModule.context().clusterNodeRoleCounter();
        if (!roleCounter.isClosed())
        {
            return Cluster.Role.get(roleCounter);
        }
        return Cluster.Role.FOLLOWER;
    }

    ElectionState electionState()
    {
        return ElectionState.get(consensusModule.context().electionStateCounter());
    }

    ConsensusModule.State moduleState()
    {
        return ConsensusModule.State.get(consensusModule.context().moduleStateCounter());
    }

    public long commitPosition()
    {
        final Counter counter = consensusModule.context().commitPositionCounter();
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
        if (1 != services.length)
        {
            throw new IllegalStateException("service count expected=1 actual=" + services.length);
        }

        return context.hasServiceTerminated[0].get();
    }

    public boolean hasMemberTerminated()
    {
        return context.hasMemberTerminated.get();
    }

    public int index()
    {
        return services[0].index();
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
        final File clusterDir = consensusModule.context().clusterDir();

        if (!ClusterTool.listMembers(clusterMembership, clusterDir, TimeUnit.SECONDS.toMillis(3)))
        {
            throw new IllegalStateException("timeout waiting for cluster members info");
        }

        return clusterMembership;
    }

    public void removeMember(final int followerMemberId, final boolean isPassive)
    {
        final File clusterDir = consensusModule.context().clusterDir();

        if (!ClusterTool.removeMember(clusterDir, followerMemberId, isPassive))
        {
            throw new IllegalStateException("could not remove member");
        }
    }

    public String hostname()
    {
        return TestCluster.hostname(index());
    }

    public boolean allSnapshotsLoaded()
    {
        for (final TestService service : services)
        {
            if (!service.wasSnapshotLoaded())
            {
                return false;
            }
        }

        return true;
    }

    public static class TestService extends StubClusteredService
    {
        static final int SNAPSHOT_FRAGMENT_COUNT = 500;
        static final int SNAPSHOT_MSG_LENGTH = 1000;

        volatile boolean wasSnapshotTaken = false;
        volatile boolean wasSnapshotLoaded = false;
        private int index;
        private volatile boolean hasReceivedUnexpectedMessage = false;
        private volatile Cluster.Role roleChangedTo = null;
        private final AtomicInteger activeSessionCount = new AtomicInteger();
        final AtomicInteger messageCount = new AtomicInteger();
        final AtomicInteger timerCount = new AtomicInteger();

        public TestService index(final int index)
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

        public int timerCount()
        {
            return timerCount.get();
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

            if (message.equals(ClusterTests.TERMINATE_MSG))
            {
                throw new ClusterTerminationException(false);
            }

            if (message.equals(ClusterTests.ECHO_SERVICE_IPC_INGRESS_MSG))
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

        public void onTimerEvent(final long correlationId, final long timestamp)
        {
            timerCount.incrementAndGet();
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

    public static class ChecksumService extends TestNode.TestService
    {
        private final BufferClaim bufferClaim = new BufferClaim();
        private final CRC32 crc32 = new CRC32();
        private long checksum;

        public long checksum()
        {
            return checksum;
        }

        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            checksum = 0;
            wasSnapshotLoaded = false;
            this.cluster = cluster;
            this.idleStrategy = cluster.idleStrategy();

            if (null != snapshotImage)
            {
                final FragmentHandler handler =
                    (buffer, offset, length, header) -> checksum = buffer.getLong(offset, LITTLE_ENDIAN);
                while (true)
                {
                    final int fragments = snapshotImage.poll(handler, 1);

                    if (snapshotImage.isClosed() || snapshotImage.isEndOfStream())
                    {
                        break;
                    }

                    idleStrategy.idle(fragments);
                }
                wasSnapshotLoaded = true;
            }
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            idleStrategy.reset();
            while (true)
            {
                if (snapshotPublication.tryClaim(SIZE_OF_LONG, bufferClaim) > 0)
                {
                    bufferClaim.buffer().putLong(bufferClaim.offset(), checksum, LITTLE_ENDIAN);
                    bufferClaim.commit();
                    break;
                }
                idleStrategy.idle();
            }
            wasSnapshotTaken = true;
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            final int payloadLength = length - SIZE_OF_INT;
            final int msgChecksum = buffer.getInt(offset + payloadLength, LITTLE_ENDIAN);
            crc32.reset();
            crc32.update(buffer.byteArray(), offset, payloadLength);
            final int computedChecksum = (int)crc32.getValue();
            if (computedChecksum != msgChecksum)
            {
                throw new ClusterException("checksum mismatch");
            }

            checksum = Hashing.hash(checksum ^ msgChecksum);
        }
    }

    public static class MessageTrackingService extends TestNode.TestService
    {
        private static final byte SNAPSHOT_COUNTERS = (byte)1;
        private static final byte SNAPSHOT_CLIENT_MESSAGES = (byte)2;
        private static final byte SNAPSHOT_SERVICE_MESSAGES = (byte)3;
        private static final byte SNAPSHOT_TIMERS = (byte)4;
        private final int serviceId;
        private final ExpandableArrayBuffer messageBuffer = new ExpandableArrayBuffer();
        private final IntHashSet clientMessages = new IntHashSet();
        private final IntHashSet serviceMessages = new IntHashSet();
        private final LongHashSet timers = new LongHashSet();
        private int nextServiceMessageNumber;
        private long nextTimerCorrelationId;

        public MessageTrackingService(final int serviceId, final int index)
        {
            this.serviceId = serviceId;
            index(index);
        }

        public int clientMessages()
        {
            return clientMessages.size();
        }

        public int serviceMessages()
        {
            return serviceMessages.size();
        }

        public int timers()
        {
            return timers.size();
        }

        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            nextServiceMessageNumber = 1_000_000 * serviceId;
            nextTimerCorrelationId = -1L * 1_000_000 * serviceId;
            clientMessages.clear();
            serviceMessages.clear();
            timers.clear();
            wasSnapshotLoaded = false;
            this.cluster = cluster;
            this.idleStrategy = cluster.idleStrategy();

            if (null != snapshotImage)
            {
                final FragmentHandler handler =
                    new FragmentAssembler((buffer, offset, length, header) ->
                    {
                        int index = offset;
                        final byte snapshotType = buffer.getByte(index);
                        index++;
                        if (SNAPSHOT_COUNTERS == snapshotType)
                        {
                            final int storedServiceId = buffer.getInt(index, LITTLE_ENDIAN);
                            if (serviceId != storedServiceId)
                            {
                                throw new IllegalStateException("Invalid snapshot!");
                            }
                            index += SIZE_OF_INT;
                            messageCount.set(buffer.getInt(index, LITTLE_ENDIAN));
                            index += SIZE_OF_INT;
                            timerCount.set(buffer.getInt(index, LITTLE_ENDIAN));
                            index += SIZE_OF_INT;
                            nextServiceMessageNumber = buffer.getInt(index, LITTLE_ENDIAN);
                            index += SIZE_OF_INT;
                            nextTimerCorrelationId = buffer.getLong(index, LITTLE_ENDIAN);
                        }
                        else if (SNAPSHOT_CLIENT_MESSAGES == snapshotType) // client messages
                        {
                            restoreMessages(buffer, index, clientMessages);
                        }
                        else if (SNAPSHOT_SERVICE_MESSAGES == snapshotType) // service messages
                        {
                            restoreMessages(buffer, index, serviceMessages);
                        }
                        else if (SNAPSHOT_TIMERS == snapshotType) // timers
                        {
                            restoreTimers(buffer, index);
                        }
                        else
                        {
                            throw new IllegalStateException("Unknown snapshot type: " + snapshotType);
                        }
                    });

                while (true)
                {
                    final int fragments = snapshotImage.poll(handler, 1);

                    if (snapshotImage.isClosed() || snapshotImage.isEndOfStream())
                    {
                        break;
                    }

                    idleStrategy.idle(fragments);
                }
                wasSnapshotLoaded = true;
            }
        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            wasSnapshotTaken = false;

            int offset = 0;

            messageBuffer.putByte(offset, SNAPSHOT_COUNTERS);
            offset++;
            messageBuffer.putInt(offset, serviceId, LITTLE_ENDIAN);
            offset += SIZE_OF_INT;
            messageBuffer.putInt(offset, messageCount(), LITTLE_ENDIAN);
            offset += SIZE_OF_INT;
            messageBuffer.putInt(offset, timerCount(), LITTLE_ENDIAN);
            offset += SIZE_OF_INT;
            messageBuffer.putInt(offset, nextServiceMessageNumber, LITTLE_ENDIAN);
            offset += SIZE_OF_INT;
            messageBuffer.putLong(offset, nextTimerCorrelationId, LITTLE_ENDIAN);
            offset += SIZE_OF_LONG;
            idleStrategy.reset();
            while (snapshotPublication.offer(messageBuffer, 0, offset) < 0)
            {
                idleStrategy.idle();
            }

            snapshotMessages(snapshotPublication, SNAPSHOT_CLIENT_MESSAGES, clientMessages);
            snapshotMessages(snapshotPublication, SNAPSHOT_SERVICE_MESSAGES, serviceMessages);
            snapshotTimers(snapshotPublication);

            wasSnapshotTaken = true;
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            if (null != session)
            {
                final int messageNumber = buffer.getInt(offset, LITTLE_ENDIAN);
                if (!clientMessages.add(messageNumber))
                {
                    throw new IllegalStateException("memberId=" + index() + ", serviceId=" + serviceId +
                        " Duplicate client message: messageNumber=" + messageNumber + ", clientMessages=" +
                        clientMessages);
                }

                // Send 3 service messages
                for (int i = 0; i < 3; i++)
                {
                    messageBuffer.putInt(0, ++nextServiceMessageNumber, LITTLE_ENDIAN);

                    idleStrategy.reset();
                    while (cluster.offer(messageBuffer, 0, SIZE_OF_INT) < 0)
                    {
                        idleStrategy.idle();
                    }
                }

                // Schedule two timers
                for (int i = 0; i < 2; i++)
                {
                    final long timerId = --nextTimerCorrelationId;
                    idleStrategy.reset();
                    while (!cluster.scheduleTimer(timerId, cluster.time() - 1))
                    {
                        idleStrategy.idle();
                    }
                }

                // Echo input message back to the client
                while (session.offer(buffer, offset, length) < 0)
                {
                    idleStrategy.idle();
                }
            }
            else
            {
                final int messageNumber = buffer.getInt(offset, LITTLE_ENDIAN);
                if (!serviceMessages.add(messageNumber))
                {
                    throw new IllegalStateException("memberId=" + index() + ", serviceId=" + serviceId +
                        " Duplicate service message: messageNumber=" + messageNumber + ", serviceMessages=" +
                        serviceMessages);
                }
            }
            messageCount.incrementAndGet(); // count all messages
        }

        public void onTimerEvent(final long correlationId, final long timestamp)
        {
            if (!timers.add(correlationId))
            {
                throw new IllegalStateException("memberId=" + index() + ", serviceId=" + serviceId +
                    " Duplicate timer event: correlationId=" + correlationId + ", timers=" + timers);
            }
            super.onTimerEvent(correlationId, timestamp);
        }

        public String toString()
        {
            return "MessageTrackingService{" +
                "serviceId=" + serviceId +
                ", messageCount=" + messageCount() +
                ", timerCount=" + timerCount() +
                ", nextServiceMessageNumber=" + nextServiceMessageNumber +
                ", nextTimerCorrelationId=" + nextTimerCorrelationId +
                ", clientMessages=" + clientMessages.stream().sorted().collect(Collectors.toList()) +
                ", serviceMessages=" + serviceMessages.stream().sorted().collect(Collectors.toList()) +
                ", timers=" + timers.stream().sorted().collect(Collectors.toList()) +
                '}';
        }

        private void snapshotMessages(
            final ExclusivePublication snapshotPublication, final byte snapshotType, final IntHashSet messages)
        {
            int offset = 0;
            messageBuffer.putByte(offset, snapshotType);
            offset++;
            messageBuffer.putInt(offset, messages.size(), LITTLE_ENDIAN);
            offset += SIZE_OF_INT;

            final IntHashSet.IntIterator iterator = messages.iterator();
            while (iterator.hasNext())
            {
                final int messageId = iterator.nextValue();
                messageBuffer.putInt(offset, messageId);
                offset += SIZE_OF_INT;
            }

            idleStrategy.reset();
            while (snapshotPublication.offer(messageBuffer, 0, offset) < 0)
            {
                idleStrategy.idle();
            }
        }

        private void snapshotTimers(final ExclusivePublication snapshotPublication)
        {
            int offset = 0;
            messageBuffer.putByte(offset, SNAPSHOT_TIMERS);
            offset++;
            messageBuffer.putInt(offset, timers.size(), LITTLE_ENDIAN);
            offset += SIZE_OF_INT;

            final LongHashSet.LongIterator iterator = timers.iterator();
            while (iterator.hasNext())
            {
                final long correlationId = iterator.nextValue();
                messageBuffer.putLong(offset, correlationId);
                offset += SIZE_OF_LONG;
            }

            idleStrategy.reset();
            while (snapshotPublication.offer(messageBuffer, 0, offset) < 0)
            {
                idleStrategy.idle();
            }
        }

        private void restoreMessages(final DirectBuffer buffer, final int offset, final IntHashSet messages)
        {
            int absoluteOffset = offset;
            final int count = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
            absoluteOffset += SIZE_OF_INT;
            for (int i = 0; i < count; i++)
            {
                final int messageId = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
                absoluteOffset += SIZE_OF_INT;
                if (!messages.add(messageId))
                {
                    throw new IllegalStateException("memberId=" + index() + " Duplicate message in a snapshot: " +
                        messageId);
                }
            }
        }

        private void restoreTimers(final DirectBuffer buffer, final int offset)
        {
            int absoluteOffset = offset;
            final int count = buffer.getInt(absoluteOffset, LITTLE_ENDIAN);
            absoluteOffset += SIZE_OF_INT;
            for (int i = 0; i < count; i++)
            {
                final long correlationId = buffer.getLong(absoluteOffset, LITTLE_ENDIAN);
                absoluteOffset += SIZE_OF_LONG;
                if (!timers.add(correlationId))
                {
                    throw new IllegalStateException("memberId=" + index() + " Duplicate timer event in a snapshot: " +
                        correlationId);
                }
            }
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
        final AtomicBoolean[] hasServiceTerminated;
        final TestService[] services;

        Context(final TestService[] services, final String nodeMappings)
        {
            mediaDriverContext.nameResolver(new RedirectingNameResolver(nodeMappings));
            this.services = services;
            hasServiceTerminated = new AtomicBoolean[services.length];
            for (int i = 0; i < services.length; i++)
            {
                hasServiceTerminated[i] = new AtomicBoolean();
            }
        }
    }

    public String toString()
    {
        return "TestNode{" +
            "memberId=" + index() +
            ", role=" + role() +
            ", services=" + Arrays.toString(services) +
            '}';
    }
}
