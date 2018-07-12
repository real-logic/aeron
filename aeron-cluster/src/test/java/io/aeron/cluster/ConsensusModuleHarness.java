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
import io.aeron.cluster.codecs.RecordingLogDecoder;
import io.aeron.cluster.codecs.RecoveryPlanDecoder;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.*;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import static io.aeron.CommonContext.*;
import static java.util.stream.Collectors.toList;

public class ConsensusModuleHarness implements AutoCloseable, ClusteredService
{
    public static final String DRIVER_DIRECTORY = "driver";
    public static final String CONSENSUS_MODULE_DIRECTORY = ClusteredServiceContainer.Configuration.clusterDirName();
    public static final String ARCHIVE_DIRECTORY = Archive.Configuration.archiveDirName();
    public static final String SERVICE_DIRECTORY = ClusteredServiceContainer.Configuration.clusterDirName();
    private static final String LOG_CHANNEL =
        "aeron:udp?term-length=64k|control-mode=manual|control=localhost:55550";

    private static final long MAX_CATALOG_ENTRIES = 1024;

    private static final PrintStream NULL_PRINT_STREAM = new PrintStream(new OutputStream()
    {
        public void write(final int b)
        {
        }
    });

    private final ClusteredMediaDriver clusteredMediaDriver;
    private final ClusteredServiceContainer clusteredServiceContainer;
    private final AtomicBoolean isTerminated = new AtomicBoolean();
    private final AtomicInteger roleValue = new AtomicInteger(-1);
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
    private final EpochClock epochClock = System::currentTimeMillis;
    private final MemberStatusCounters[] memberStatusCounters;
    private final AeronArchive.Context aeronArchiveContext;

    private int thisMemberIndex;

    ConsensusModuleHarness(
        final ConsensusModule.Context consensusModuleContext,
        final ClusteredService service,
        final MemberStatusListener[] memberStatusListeners,
        final boolean isCleanStart,
        final boolean cleanOnClose,
        final boolean printMemberStatusMessages)
    {
        this.service = service;
        this.members = ClusterMember.parse(consensusModuleContext.clusterMembers());
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
                .clusterDir(serviceDir)
                .idleStrategySupplier(() -> new SleepingMillisIdleStrategy(1))
                .clusteredService(this)
                .terminationHook(() -> {})
                .archiveContext(aeronArchiveContext.clone())
                .errorHandler(Throwable::printStackTrace));

        this.cleanOnClose = cleanOnClose;
        this.aeronArchiveContext = aeronArchiveContext.clone();
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriverPath));

        memberStatusSubscriptions = new Subscription[members.length];
        memberStatusAdapters = new MemberStatusAdapter[members.length];
        memberStatusPublications = new Publication[members.length];
        memberStatusCounters = new MemberStatusCounters[members.length];

        final PrintStream printStream = printMemberStatusMessages ? System.out : NULL_PRINT_STREAM;

        for (int i = 0; i < members.length; i++)
        {
            if (consensusModuleContext.clusterMemberId() != members[i].id() && null != memberStatusListeners[i])
            {
                memberStatusCounters[i] = new MemberStatusCounters();

                final MemberStatusListener listener = memberStatusMixIn(
                    i, printStream, memberStatusCounters[i], memberStatusListeners[i]);

                final ChannelUri memberStatusUri = ChannelUri.parse(consensusModuleContext.memberStatusChannel());
                memberStatusUri.put(ENDPOINT_PARAM_NAME, members[i].memberFacingEndpoint());

                final int statusStreamId = consensusModuleContext.memberStatusStreamId();

                memberStatusSubscriptions[i] =
                    aeron.addSubscription(memberStatusUri.toString(), statusStreamId);

                memberStatusAdapters[i] = new MemberStatusAdapter(
                    memberStatusSubscriptions[i], listener);
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

    ClusterMember member(final int index)
    {
        return members[index];
    }

    MemberStatusCounters memberStatusCounters(final int index)
    {
        return memberStatusCounters[index];
    }

    IntSupplier onRequestVoteCounter(final int index)
    {
        return () -> memberStatusCounters[index].onRequestVoteCounter;
    }

    IntSupplier onNewLeadershipTermCounter(final int index)
    {
        return () -> memberStatusCounters[index].onNewLeadershipTermCounter;
    }

    IntSupplier onVoteCounter(final int index)
    {
        return () -> memberStatusCounters[index].onVoteCounter;
    }

    IntSupplier onAppendedPositionCounter(final int index)
    {
        return () -> memberStatusCounters[index].onAppendedPositionCounter;
    }

    IntSupplier onCanvassPosition(final int index)
    {
        return () -> memberStatusCounters[index].onCanvassPositionCounter;
    }

    IntSupplier onCommitPosition(final int index)
    {
        return () -> memberStatusCounters[index].onCommitPositionCounter;
    }

    int pollMemberStatusAdapter(final int index)
    {
        if (null != memberStatusAdapters[index])
        {
            return memberStatusAdapters[index].poll();
        }

        return 0;
    }

    void awaitMemberStatusMessage(final int index)
    {
        idleStrategy.reset();
        while (memberStatusAdapters[index].poll() == 0)
        {
            idleStrategy.idle();
        }
    }

    void awaitMemberStatusMessage(final int index, final IntSupplier counterValue, final int initialCounterValue)
    {
        do
        {
            awaitMemberStatusMessage(index);
        }
        while (counterValue.getAsInt() == initialCounterValue);
    }

    void awaitMemberStatusMessage(final int index, final IntSupplier counterValue)
    {
        awaitMemberStatusMessage(index, counterValue, 0);
    }

    void pollMemberStatusAdapter(final int index, final long durationMs)
    {
        final long deadlineMs = epochClock.time() + durationMs;
        final MemberStatusAdapter memberStatusAdapter = memberStatusAdapters[index];
        idleStrategy.reset();

        while (epochClock.time() < deadlineMs)
        {
            if (null != memberStatusAdapter)
            {
                idleStrategy.idle(memberStatusAdapter.poll());
            }
            else
            {
                idleStrategy.idle();
            }
        }
    }

    Publication memberStatusPublication(final int index)
    {
        return memberStatusPublications[index];
    }

    MemberStatusPublisher memberStatusPublisher()
    {
        return memberStatusPublisher;
    }

    Publication createLogPublication(
        final ChannelUri channelUri,
        final RecordingExtent recordingExtent,
        final long position,
        final boolean emptyLog)
    {
        if (!emptyLog)
        {
            channelUri.initialPosition(position, recordingExtent.initialTermId, recordingExtent.termBufferLength);
            channelUri.put(MTU_LENGTH_PARAM_NAME, Integer.toString(recordingExtent.mtuLength));
        }

        final Publication publication = aeron.addExclusivePublication(
            channelUri.toString(),
            clusteredMediaDriver.consensusModule().context().logStreamId());

        if (!channelUri.containsKey(ENDPOINT_PARAM_NAME) && UDP_MEDIA.equals(channelUri.media()))
        {
            final ChannelUriStringBuilder builder = new ChannelUriStringBuilder().media(UDP_MEDIA);
            for (final ClusterMember member : members)
            {
                if (member == members[thisMemberIndex])
                {
                    publication.addDestination(builder.endpoint(member.logEndpoint()).build());
                }
            }
        }

        return publication;
    }

    void awaitServiceOnStart()
    {
        idleStrategy.reset();
        while (!serviceOnStart.get())
        {
            idleStrategy.idle();
        }
    }

    void awaitServiceOnMessageCounter(final int value)
    {
        idleStrategy.reset();
        while (serviceOnMessageCounter.get() < value)
        {
            idleStrategy.idle();
        }
    }

    void awaitServiceOnRoleChange(final Cluster.Role role)
    {
        idleStrategy.reset();
        while (roleValue.get() != role.code())
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
        final ClientSession session,
        final long correlationId,
        final long timestampMs,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        service.onSessionMessage(session, correlationId, timestampMs, buffer, offset, length, header);
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

    public void onRoleChange(final Cluster.Role newRole)
    {
        roleValue.lazySet(newRole.code());
        service.onRoleChange(newRole);
    }

    void recordingExtent(final long recordingId, final RecordingExtent recordingExtent)
    {
        try (AeronArchive archive = AeronArchive.connect(aeronArchiveContext.clone()))
        {
            if (archive.listRecording(recordingId, recordingExtent) == 0)
            {
                throw new IllegalStateException("could not find recordingId to get RecordingExtent");
            }
        }
    }

    static File harnessDirectory(final int clusterMemberId)
    {
        return new File(IoUtil.tmpDirName(), "aeron-cluster-" + clusterMemberId);
    }

    static long makeRecordingLog(
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
            false,
            false))
        {
            harness.awaitServiceOnStart();

            final AeronCluster.Context clusterContext = new AeronCluster.Context()
                .aeronDirectoryName(harness.aeron().context().aeronDirectoryName());

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

                // 96 is for the close session appended at the end and 96 for NewLeadershipTermEvent at the start
                return publication.position() + 192;
            }
        }
    }

    static long makeTruncatedRecordingLog(
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

    static long truncateRecordingLog(
        final File harnessDir,
        final long truncateRecordingId,
        final long truncatePosition)
    {
        final Archive.Context archiveContext = new Archive.Context()
            .archiveDir(new File(harnessDir, ARCHIVE_DIRECTORY));
        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context();

        try (ArchivingMediaDriver ignore = ArchivingMediaDriver.launch(mediaDriverContext, archiveContext);
            AeronArchive archive = AeronArchive.connect())
        {
            archive.truncateRecording(truncateRecordingId, truncatePosition);
        }

        mediaDriverContext.deleteAeronDirectory();

        return truncatePosition;
    }

    @SuppressWarnings("MethodLength")
    static MemberStatusListener memberStatusMixIn(
        final int index,
        final PrintStream stream,
        final MemberStatusCounters counters,
        final MemberStatusListener nextListener)
    {
        return new MemberStatusListener()
        {
            public void onCanvassPosition(
                final long logLeadershipTermId, final long logPosition, final int followerMemberId)
            {
                counters.onCanvassPositionCounter++;
                stream.format("onCanvassPositionCounter[%d] %d %d %d%n",
                    index, logLeadershipTermId, logPosition, followerMemberId);
                nextListener.onCanvassPosition(logLeadershipTermId, logPosition, followerMemberId);
            }

            public void onRequestVote(
                final long logLeadershipTermId,
                final long logPosition,
                final long candidateTermId,
                final int candidateId)
            {
                counters.onRequestVoteCounter++;
                stream.format("onRequestVote[%d] %d %d %d %d%n",
                    index, logLeadershipTermId, logPosition, candidateTermId, candidateId);
                nextListener.onRequestVote(logLeadershipTermId, logPosition, candidateTermId, candidateId);
            }

            public void onVote(
                final long candidateTermId,
                final long logLeadershipTermId,
                final long logPosition,
                final int candidateMemberId,
                final int followerMemberId,
                final boolean vote)
            {
                counters.onVoteCounter++;
                stream.format("onVote[%d] %d %d %d %d %d %s%n",
                    index,
                    candidateTermId,
                    logLeadershipTermId,
                    logPosition,
                    candidateMemberId,
                    followerMemberId,
                    vote);
                nextListener.onVote(
                    candidateTermId, logLeadershipTermId, logPosition, candidateMemberId, followerMemberId, vote);
            }

            public void onNewLeadershipTerm(
                final long logLeadershipTermId,
                final long logPosition,
                final long leadershipTermId,
                final int leaderMemberId,
                final int logSessionId)
            {
                counters.onNewLeadershipTermCounter++;
                stream.format("onNewLeadershipTerm[%d] %d %d %d %d %d%n",
                    index, logLeadershipTermId, logPosition, leadershipTermId, leaderMemberId, logSessionId);
                nextListener.onNewLeadershipTerm(
                    logLeadershipTermId, logPosition, leadershipTermId, leaderMemberId, logSessionId);
            }

            public void onAppendedPosition(
                final long leadershipTermId, final long logPosition, final int followerMemberId)
            {
                counters.onAppendedPositionCounter++;
                stream.format("onAppendedPosition[%d] %d %d %d%n",
                    index, leadershipTermId, logPosition, followerMemberId);
                nextListener.onAppendedPosition(leadershipTermId, logPosition, followerMemberId);
            }

            public void onCommitPosition(final long leadershipTermId, final long logPosition, final int leaderMemberId)
            {
                counters.onCommitPositionCounter++;
                stream.format("onCommitPosition[%d] %d %d %d%n",
                    index, leadershipTermId, logPosition, leaderMemberId);
                nextListener.onCommitPosition(leadershipTermId, logPosition, leaderMemberId);
            }

            public void onCatchupPosition(
                final long leadershipTermId, final long logPosition, final int followerMemberId)
            {
                counters.onCatchupPositionCounter++;
                stream.format("onCatchupPosition[%d] %d %d %d%n",
                    index, leadershipTermId, logPosition, followerMemberId);
                nextListener.onCatchupPosition(leadershipTermId, logPosition, followerMemberId);
            }

            public void onStopCatchup(final int replaySessionId, final int followerMemberId)
            {
                counters.onStopCatchupCounter++;
                stream.format("onStopCatchup[%d] %d %d%n",
                    index, replaySessionId, followerMemberId);
                nextListener.onStopCatchup(replaySessionId, followerMemberId);
            }

            public void onRecoveryPlanQuery(
                final long correlationId, final int requestMemberId, final int leaderMemberId)
            {
                counters.onRecoveryPlanQueryCounter++;
                stream.format("onRecoveryPlanQueryCounter[%d] %d %d %d%n",
                    index, correlationId, requestMemberId, leaderMemberId);
                nextListener.onRecoveryPlanQuery(correlationId, requestMemberId, leaderMemberId);
            }

            public void onRecoveryPlan(final RecoveryPlanDecoder decoder)
            {
                counters.onRecoveryPlanCounter++;
                stream.format("onRecoveryPlan[%d] %d %d %d%n",
                    index, decoder.correlationId(), decoder.requestMemberId(), decoder.leaderMemberId());
                nextListener.onRecoveryPlan(decoder);
            }

            public void onRecordingLogQuery(
                final long correlationId,
                final int requestMemberId,
                final int leaderMemberId,
                final long fromLeadershipTermId,
                final int count,
                final boolean includeSnapshots)
            {
                counters.onRecordingLogQueryCounter++;
                stream.format("onRecordingLogQuery[%d] %d %d %d%n",
                    index, correlationId, requestMemberId, leaderMemberId);
                nextListener.onRecordingLogQuery(
                    correlationId, requestMemberId, leaderMemberId, fromLeadershipTermId, count, includeSnapshots);
            }

            public void onRecordingLog(final RecordingLogDecoder decoder)
            {
                counters.onRecordingLogCounter++;
                stream.format("onRecordingLog[%d] %d %d %d%n",
                    index, decoder.correlationId(), decoder.requestMemberId(), decoder.leaderMemberId());
                nextListener.onRecordingLog(decoder);
            }
        };
    }

    static void copyDirectory(final File srcDirectory, final File dstDirectory)
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

    public static class MemberStatusCounters
    {
        int onCanvassPositionCounter = 0;
        int onRequestVoteCounter = 0;
        int onVoteCounter = 0;
        int onNewLeadershipTermCounter = 0;
        int onAppendedPositionCounter = 0;
        int onCommitPositionCounter = 0;
        int onCatchupPositionCounter = 0;
        int onStopCatchupCounter = 0;
        int onRecoveryPlanQueryCounter = 0;
        int onRecoveryPlanCounter = 0;
        int onRecordingLogQueryCounter = 0;
        int onRecordingLogCounter = 0;
    }

    private static void checkOfferResult(final long result)
    {
        if (result == Publication.NOT_CONNECTED ||
            result == Publication.CLOSED ||
            result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("unexpected publication state: " + result);
        }
    }
}
