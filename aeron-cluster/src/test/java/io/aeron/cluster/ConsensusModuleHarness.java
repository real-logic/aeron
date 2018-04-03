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

import io.aeron.*;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.SessionDecorator;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.io.File;
import java.io.PrintStream;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;

public class ConsensusModuleHarness implements AutoCloseable, ClusteredService
{
    public static final String DRIVER_DIRECTORY = "driver";
    public static final String CONSENSUS_MODULE_DIRECTORY = ConsensusModule.Configuration.clusterDirName();
    public static final String ARCHIVE_DIRECTORY = Archive.Configuration.archiveDirName();
    public static final String SERVICE_DIRECTORY = ClusteredServiceContainer.Configuration.clusteredServiceDirName();

    private static final long MAX_CATALOG_ENTRIES = 1024;

    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer clusteredServiceContainer;
    private final AtomicBoolean isTerminated = new AtomicBoolean();
    private final Aeron aeron;
    private final ClusteredService service;
    private final AtomicBoolean serviceOnStart = new AtomicBoolean();
    private final AtomicInteger serviceOnMessageCounter = new AtomicInteger(0);
    private final IdleStrategy idleStrategy = new SleepingMillisIdleStrategy(1);
    private final ClusterMember[] members;
    private final Subscription[] memberStatusSubscriptions;
    private final MemberStatusAdapter[] memberStatusAdapters;
    private final Publication[] memberStatusPublications;
    private final MemberStatusPublisher memberStatusPublisher = new MemberStatusPublisher();
    private final boolean cleanOnClose;
    private final File harnessDir;

    private int thisMemberIndex = -1;
    private int leaderIndex = -1;

    ConsensusModuleHarness(
        final ConsensusModule.Context consenusModuleContext,
        final ClusteredService service,
        final MemberStatusListener[] memberStatusListeners,
        final boolean isCleanStart,
        final boolean cleanOnClose)
    {
        this.service = service;
        this.members = ClusterMember.parse(consenusModuleContext.clusterMembers());
        this.leaderIndex = consenusModuleContext.appointedLeaderId();
        this.thisMemberIndex = consenusModuleContext.clusterMemberId();

        harnessDir = harnessDirectory(consenusModuleContext.clusterMemberId());
        final String mediaDriverPath = new File(harnessDir, DRIVER_DIRECTORY).getPath();
        final File clusterDir = new File(harnessDir, CONSENSUS_MODULE_DIRECTORY);
        final File archiveDir = new File(harnessDir, ARCHIVE_DIRECTORY);
        final File serviceDir = new File(harnessDir, SERVICE_DIRECTORY);
        final ClusterMember thisMember = this.members[thisMemberIndex];
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context();
        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        final ClusteredServiceContainer.Context serviceContext = new ClusteredServiceContainer.Context();

        if (members.length > 1)
        {
            ChannelUri channelUri;

            channelUri = ChannelUri.parse(consenusModuleContext.memberStatusChannel());
            channelUri.put(ENDPOINT_PARAM_NAME, thisMember.memberFacingEndpoint());

            consenusModuleContext.memberStatusChannel(channelUri.toString());

            channelUri = ChannelUri.parse(archiveContext.controlChannel());
            channelUri.put(ENDPOINT_PARAM_NAME, thisMember.archiveEndpoint());
            final String archiveControlChannel = channelUri.toString();

            aeronArchiveContext.controlRequestChannel(archiveControlChannel);
            archiveContext.controlChannel(archiveControlChannel);

            channelUri = ChannelUri.parse(aeronArchiveContext.controlResponseChannel());
            final char[] endpointAsChars = channelUri.get(ENDPOINT_PARAM_NAME).toCharArray();
            endpointAsChars[endpointAsChars.length - 1] += thisMemberIndex;
            channelUri.put(ENDPOINT_PARAM_NAME, new String(endpointAsChars));
            final String controlResponseChannel = channelUri.toString();

            aeronArchiveContext.controlResponseChannel(controlResponseChannel);

            channelUri = ChannelUri.parse(consenusModuleContext.ingressChannel());
            channelUri.put(ENDPOINT_PARAM_NAME, thisMember.clientFacingEndpoint());

            consenusModuleContext.ingressChannel(channelUri.toString());

            channelUri = ChannelUri.parse(consenusModuleContext.logChannel());
            channelUri.put(ENDPOINT_PARAM_NAME, thisMember.logEndpoint());

            consenusModuleContext.logChannel(channelUri.toString());
        }

        clusteredMediaDriver = ClusteredMediaDriver.launch(
            mediaDriverContext
                .aeronDirectoryName(mediaDriverPath)
                .warnIfDirectoryExists(isCleanStart)
                .threadingMode(ThreadingMode.SHARED)
                .termBufferSparseFile(true)
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true),
            archiveContext
                .aeronDirectoryName(mediaDriverPath)
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .archiveDir(archiveDir)
                .deleteArchiveOnStart(isCleanStart),
            consenusModuleContext
                .aeronDirectoryName(mediaDriverPath)
                .clusterDir(clusterDir)
                .archiveContext(aeronArchiveContext.clone())
                .terminationHook(() -> isTerminated.set(true))
                .deleteDirOnStart(isCleanStart));

        clusteredServiceContainer = ClusteredServiceContainer.launch(
            serviceContext
                .aeronDirectoryName(mediaDriverPath)
                .clusteredServiceDir(serviceDir)
                .idleStrategySupplier(() -> new SleepingMillisIdleStrategy(1))
                .clusteredService(this)
                .terminationHook(() -> {})
                .archiveContext(aeronArchiveContext.clone())
                .errorHandler(Throwable::printStackTrace)
                .deleteDirOnStart(isCleanStart));

        this.cleanOnClose = cleanOnClose;
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriverPath));

        memberStatusSubscriptions = new Subscription[members.length];
        memberStatusAdapters = new MemberStatusAdapter[members.length];
        memberStatusPublications = new Publication[members.length];

        for (int i = 0; i < members.length; i++)
        {
            if (consenusModuleContext.clusterMemberId() != members[i].id() && null != memberStatusListeners[i])
            {
                final ChannelUri memberStatusUri = ChannelUri.parse(consenusModuleContext.memberStatusChannel());
                memberStatusUri.put(ENDPOINT_PARAM_NAME, members[i].memberFacingEndpoint());

                final int statusStreamId = consenusModuleContext.memberStatusStreamId();

                memberStatusSubscriptions[i] =
                    aeron.addSubscription(memberStatusUri.toString(), statusStreamId);

                memberStatusAdapters[i] = new MemberStatusAdapter(
                    memberStatusSubscriptions[i], memberStatusListeners[i]);
                memberStatusPublications[i] = aeron.addExclusivePublication(
                    consenusModuleContext.memberStatusChannel(), consenusModuleContext.memberStatusStreamId());

                idleStrategy.reset();
                while (!memberStatusSubscriptions[i].isConnected())
                {
                    idleStrategy.idle();
                }
            }
        }
    }

    public void close()
    {
        CloseHelper.close(clusteredServiceContainer);
        CloseHelper.close(clusteredMediaDriver);
        CloseHelper.close(aeron);

        if (cleanOnClose)
        {
            deleteDirectories();
        }
    }

    public void deleteDirectories()
    {
        if (null != clusteredServiceContainer)
        {
            clusteredServiceContainer.context().deleteDirectory();
        }

        if (null != clusteredMediaDriver)
        {
            clusteredMediaDriver.mediaDriver().context().deleteAeronDirectory();
            clusteredMediaDriver.archive().context().deleteArchiveDirectory();
            clusteredMediaDriver.consensusModule().context().deleteDirectory();
        }

        IoUtil.delete(harnessDir, true);
    }

    public Aeron aeron()
    {
        return aeron;
    }

    public ClusterMember member(final int index)
    {
        return members[index];
    }

    public int pollMemberStatusAdapters(final int index)
    {
        if (null != memberStatusAdapters[index])
        {
            return memberStatusAdapters[index].poll();
        }

        return 0;
    }

    public void awaitMemberStatusMessage(final int index)
    {
        idleStrategy.reset();
        while (memberStatusAdapters[index].poll() == 0)
        {
            idleStrategy.idle();
        }
    }

    public Publication memberStatusPublication(final int index)
    {
        return memberStatusPublications[index];
    }

    public MemberStatusPublisher memberStatusPublisher()
    {
        return memberStatusPublisher;
    }

    public void awaitServiceOnStart()
    {
        idleStrategy.reset();
        while (!serviceOnStart.get())
        {
            idleStrategy.idle();
        }
    }

    public void awaitServiceOnMessageCounter(final int value)
    {
        idleStrategy.reset();
        while (serviceOnMessageCounter.get() < value)
        {
            idleStrategy.idle();
        }
    }

    public void onStart(final Cluster cluster)
    {
        service.onStart(cluster);
        serviceOnStart.lazySet(true);
    }

    public void onSessionOpen(final ClientSession session, final long timestampMs)
    {
        service.onSessionOpen(session, timestampMs);
    }

    public void onSessionClose(final ClientSession session, final long timestampMs, final CloseReason closeReason)
    {
        service.onSessionClose(session, timestampMs, closeReason);
    }

    public void onSessionMessage(
        final long clusterSessionId,
        final long correlationId,
        final long timestampMs,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        service.onSessionMessage(clusterSessionId, correlationId, timestampMs, buffer, offset, length, header);
        serviceOnMessageCounter.getAndIncrement();
    }

    public void onTimerEvent(final long correlationId, final long timestampMs)
    {
        service.onTimerEvent(correlationId, timestampMs);
    }

    public void onTakeSnapshot(final Publication snapshotPublication)
    {
        service.onTakeSnapshot(snapshotPublication);
    }

    public void onLoadSnapshot(final Image snapshotImage)
    {
        service.onLoadSnapshot(snapshotImage);
    }

    public void onReplayBegin()
    {
        service.onReplayBegin();
    }

    public void onReplayEnd()
    {
        service.onReplayEnd();
    }

    public void onRoleChange(final Cluster.Role newRole)
    {
        service.onRoleChange(newRole);
    }

    public void onReady()
    {
        service.onReady();
    }

    public static File harnessDirectory(final int clusterMemberId)
    {
        return new File(IoUtil.tmpDirName(), "aeron-cluster-" + clusterMemberId);
    }

    public static long makeRecordingLog(
        final int numMessages,
        final int maxMessageLength,
        final Random random,
        final Long2LongHashMap positionMap,
        final ConsensusModule.Context context)
    {
        try (ConsensusModuleHarness harness = new ConsensusModuleHarness(
            context,
            new StubClusteredService(),
            null,
            true,
            false))
        {
            harness.awaitServiceOnStart();

            final AeronCluster.Context clusterContext = new AeronCluster.Context()
                .aeronDirectoryName(harness.aeron().context().aeronDirectoryName())
                .lock(new NoOpLock());

            try (AeronCluster aeronCluster = AeronCluster.connect(clusterContext))
            {
                final SessionDecorator sessionDecorator = new SessionDecorator(aeronCluster.clusterSessionId());
                final Publication publication = aeronCluster.ingressPublication();
                final ExpandableArrayBuffer msgBuffer = new ExpandableArrayBuffer(maxMessageLength);

                for (int i = 0; i < numMessages; i++)
                {
                    final long messageCorrelationId = aeronCluster.context().aeron().nextCorrelationId();
                    final int length = null == random ? maxMessageLength : random.nextInt(maxMessageLength);
                    msgBuffer.putInt(0, i);

                    while (true)
                    {
                        final long result = sessionDecorator.offer(
                            publication, messageCorrelationId, msgBuffer, 0, length);
                        if (result > 0)
                        {
                            if (null != positionMap)
                            {
                                positionMap.put(i, result);
                            }

                            break;
                        }

                        checkOfferResult(result);
                        TestUtil.checkInterruptedStatus();

                        Thread.yield();
                    }
                }

                harness.awaitServiceOnMessageCounter(numMessages);

                return publication.position() + 96; // 96 is for the close session appended at the end.
            }
        }
    }

    public static long makeTruncatedRecordingLog(
        final int numTotalMessages,
        final int truncateAtNumMessage,
        final int maxMessageLength,
        final Random random,
        final ConsensusModule.Context context)
    {
        final Long2LongHashMap positionMap = new Long2LongHashMap(-1);
        final File harnessDir = harnessDirectory(context.clusterMemberId());

        makeRecordingLog(numTotalMessages, maxMessageLength, random, positionMap, context);

        final long truncatePosition = positionMap.get(truncateAtNumMessage);

        final Archive.Context archiveContext = new Archive.Context()
            .archiveDir(new File(harnessDir, ARCHIVE_DIRECTORY));

        try (ArchivingMediaDriver archivingMediaDriver =
            ArchivingMediaDriver.launch(new MediaDriver.Context(), archiveContext);
            AeronArchive archive = AeronArchive.connect())
        {
            final RecordingDescriptorConsumer consumer =
                (controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity) ->
                {
                    System.out.println("id " + recordingId + " " + stopPosition);
                };

            archive.listRecording(0, consumer);
            archive.truncateRecording(0, truncatePosition);
            archive.listRecording(0, consumer);
        }

        return truncatePosition;
    }

    public static MemberStatusListener printMemberStatusMixIn(
        final PrintStream stream, final MemberStatusListener nextListener)
    {
        return new MemberStatusListener()
        {
            public void onRequestVote(
                final long candidateTermId,
                final long lastBaseLogPosition,
                final long lastTermPosition,
                final int candidateId)
            {
                stream.format(
                    "onRequestVote %d %d %d %d%n", candidateTermId, lastBaseLogPosition, lastTermPosition, candidateId);

                nextListener.onRequestVote(candidateTermId, lastBaseLogPosition, lastTermPosition, candidateId);
            }

            public void onVote(
                final long candidateTermId,
                final int candidateMemberId,
                final int followerMemberId,
                final boolean vote)
            {
                stream.format("onVote %d %d %d %s%n", candidateTermId, candidateMemberId, followerMemberId, vote);

                nextListener.onVote(candidateTermId, candidateMemberId, followerMemberId, vote);
            }

            public void onAppendedPosition(
                final long termPosition, final long leadershipTermId, final int followerMemberId)
            {
                stream.format("onAppendedPosition %d %d %d%n", termPosition, leadershipTermId, followerMemberId);
                nextListener.onAppendedPosition(termPosition, leadershipTermId, followerMemberId);
            }

            public void onCommitPosition(
                final long termPosition, final long leadershipTermId, final int leaderMemberId, final int logSessionId)
            {
                stream.format(
                    "onCommitPosition %d %d %d %d%n", termPosition, leadershipTermId, leaderMemberId, logSessionId);
                nextListener.onCommitPosition(termPosition, leadershipTermId, leaderMemberId, logSessionId);
            }
        };
    }

    public static MemberStatusListener[] printMemberStatusMixIn(
        final PrintStream stream, final MemberStatusListener[] listeners)
    {
        final MemberStatusListener[] printMixIns = new MemberStatusListener[listeners.length];

        for (int i = 0; i < listeners.length; i++)
        {
            if (null != listeners[i])
            {
                printMixIns[i] = printMemberStatusMixIn(stream, listeners[i]);
            }
        }

        return printMixIns;
    }

    private static void checkOfferResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("Unexpected publication state: " + result);
        }
    }
}
