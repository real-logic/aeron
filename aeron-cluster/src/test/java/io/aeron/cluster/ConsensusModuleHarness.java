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
import org.agrona.*;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static java.util.stream.Collectors.toList;

public class ConsensusModuleHarness implements AutoCloseable, ClusteredService
{
    public static final String DRIVER_DIRECTORY = "driver";
    public static final String CONSENSUS_MODULE_DIRECTORY = ConsensusModule.Configuration.clusterDirName();
    public static final String ARCHIVE_DIRECTORY = Archive.Configuration.archiveDirName();
    public static final String SERVICE_DIRECTORY = ClusteredServiceContainer.Configuration.clusteredServiceDirName();
    private static final String LOG_CHANNEL =
        "aeron:udp?term-length=64k|control-mode=manual|control=localhost:55550";

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
        final ConsensusModule.Context consensusModuleContext,
        final ClusteredService service,
        final MemberStatusListener[] memberStatusListeners,
        final boolean isCleanStart,
        final boolean cleanOnClose)
    {
        this.service = service;
        this.members = ClusterMember.parse(consensusModuleContext.clusterMembers());
        this.leaderIndex = consensusModuleContext.appointedLeaderId();
        this.thisMemberIndex = consensusModuleContext.clusterMemberId();

        harnessDir = harnessDirectory(consensusModuleContext.clusterMemberId());
        final String mediaDriverPath = new File(harnessDir, DRIVER_DIRECTORY).getPath();
        final File clusterDir = new File(harnessDir, CONSENSUS_MODULE_DIRECTORY);
        final File archiveDir = new File(harnessDir, ARCHIVE_DIRECTORY);
        final File serviceDir = new File(harnessDir, SERVICE_DIRECTORY);
        final ClusterMember thisMember = this.members[thisMemberIndex];
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();
        final Archive.Context archiveContext = new Archive.Context();
        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context();
        final ClusteredServiceContainer.Context serviceContext = new ClusteredServiceContainer.Context();

        aeronArchiveContext.aeronDirectoryName(mediaDriverPath);

        if (members.length > 1)
        {
            ChannelUri channelUri;

            channelUri = ChannelUri.parse(consensusModuleContext.memberStatusChannel());
            channelUri.put(ENDPOINT_PARAM_NAME, thisMember.memberFacingEndpoint());

            consensusModuleContext.memberStatusChannel(channelUri.toString());

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

            channelUri = ChannelUri.parse(consensusModuleContext.ingressChannel());
            channelUri.put(ENDPOINT_PARAM_NAME, thisMember.clientFacingEndpoint());

            consensusModuleContext.ingressChannel(channelUri.toString());

            channelUri = ChannelUri.parse(LOG_CHANNEL);
            String logControl = channelUri.get(CommonContext.MDC_CONTROL_PARAM_NAME);
            logControl = logControl.substring(0, logControl.length() - 1) + thisMemberIndex;
            channelUri.put(CommonContext.MDC_CONTROL_PARAM_NAME, logControl);

            consensusModuleContext.logChannel(channelUri.toString());
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
            consensusModuleContext
                .aeronDirectoryName(mediaDriverPath)
                .clusterDir(clusterDir)
                .archiveContext(aeronArchiveContext.clone())
                .terminationHook(() -> isTerminated.set(true))
                .errorHandler(Throwable::printStackTrace)
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
            if (consensusModuleContext.clusterMemberId() != members[i].id() && null != memberStatusListeners[i])
            {
                final ChannelUri memberStatusUri = ChannelUri.parse(consensusModuleContext.memberStatusChannel());
                memberStatusUri.put(ENDPOINT_PARAM_NAME, members[i].memberFacingEndpoint());

                final int statusStreamId = consensusModuleContext.memberStatusStreamId();

                memberStatusSubscriptions[i] =
                    aeron.addSubscription(memberStatusUri.toString(), statusStreamId);

                memberStatusAdapters[i] = new MemberStatusAdapter(
                    memberStatusSubscriptions[i], memberStatusListeners[i]);
                memberStatusPublications[i] = aeron.addExclusivePublication(
                    consensusModuleContext.memberStatusChannel(), consensusModuleContext.memberStatusStreamId());

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

        return truncateRecordingLog(harnessDir, 0, truncatePosition);
    }

    public static long truncateRecordingLog(
        final File harnessDir,
        final long truncateRecordingId,
        final long truncatePosition)
    {
        final Archive.Context archiveContext = new Archive.Context()
            .archiveDir(new File(harnessDir, ARCHIVE_DIRECTORY));

        try (ArchivingMediaDriver ignore =
            ArchivingMediaDriver.launch(new MediaDriver.Context(), archiveContext);
            AeronArchive archive = AeronArchive.connect())
        {
            archive.truncateRecording(truncateRecordingId, truncatePosition);
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

            public void onNewLeadershipTerm(
                final long lastBaseLogPosition,
                final long lastTermPosition,
                final long leadershipTermId,
                final int leaderMemberId,
                final int logSessionId)
            {
                stream.format("onNewLeadershipTerm %d %d %d %d %d%n",
                    lastBaseLogPosition, lastTermPosition, leadershipTermId, leaderMemberId, logSessionId);
                nextListener.onNewLeadershipTerm(
                    lastBaseLogPosition, lastTermPosition, leadershipTermId, leaderMemberId, logSessionId);
            }

            public void onAppendedPosition(
                final long termPosition, final long leadershipTermId, final int followerMemberId)
            {
                stream.format("onAppendedPosition %d %d %d%n", termPosition, leadershipTermId, followerMemberId);
                nextListener.onAppendedPosition(termPosition, leadershipTermId, followerMemberId);
            }

            public void onCommitPosition(final long termPosition, final long leadershipTermId, final int leaderMemberId)
            {
                stream.format(
                    "onCommitPosition %d %d %d%n", termPosition, leadershipTermId, leaderMemberId);
                nextListener.onCommitPosition(termPosition, leadershipTermId, leaderMemberId);
            }

            public void onQueryResponse(
                final long correlationId,
                final int requestMemberId,
                final int responseMemberId,
                final DirectBuffer data,
                final int offset,
                final int length)
            {
                stream.format(
                    "onQueryResponse %d %d %d%n", correlationId, requestMemberId, responseMemberId);
                nextListener.onQueryResponse(correlationId, requestMemberId, responseMemberId, data, offset, length);
            }

            public void onRecoveryPlanQuery(
                final long correlationId, final int leaderMemberId, final int requestMemberId)
            {
                stream.format(
                    "onRecoveryPlanQuery %d %d %d%n", correlationId, leaderMemberId, requestMemberId);
                nextListener.onRecoveryPlanQuery(correlationId, leaderMemberId, requestMemberId);
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

    public static void copyDirectory(final File srcDirectory, final File dstDirectory)
    {
        final Path srcDir = srcDirectory.toPath();
        final Path dstDir = dstDirectory.toPath();

        try
        {
            // inspired by
            // https://stackoverflow.com/questions/1146153/copying-files-from-one-directory-to-another-in-java
            final List<Path> sources = Files.walk(srcDir).collect(toList());
            final List<Path> destinations = sources.stream()
                .map(srcDir::relativize)
                .map(dstDir::resolve)
                .collect(toList());

            for (int i = 0; i < sources.size(); i++)
            {
                Files.copy(sources.get(i), destinations.get(i));
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
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
