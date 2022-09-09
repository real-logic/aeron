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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.ConsensusModuleEncoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;
import io.aeron.cluster.service.ClusterNodeControlProperties;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.NULL_SESSION_ID;
import static io.aeron.Publication.*;

/**
 * A tool to patch the latest consensus module snapshot if it has divergence in the pending service messages state.
 * This tool patches the state on a single node. It is recommended to run the tool on the leader node and distribute
 * the patched snapshot to all other nodes in a cluster.
 * <p>
 * The tool will attempt to connect to the running MediaDriver and the Archive of the cluster node.
 * <p>
 * <em>Note: Run the tool only if the cluster is suspended or completely stopped!</em>
 */
public class ConsensusModuleSnapshotPendingServiceMessagesPatch
{
    static final int SNAPSHOT_REPLAY_STREAM_ID = 103;

    /**
     * Execute the code to patch the latest snapshot.
     *
     * @param clusterDir of the cluster node to run the patch tool on.
     * @return {@code true} if the patch was applied or {@code false} if consensus module state was already correct.
     * @throws NullPointerException     if {@code null == clusterDir}.
     * @throws IllegalArgumentException if {@code clusterDir} does not exist or is not a directory.
     * @throws ClusterException         if {@code clusterDir} does not contain a recording log or if the log does not
     *                                  contain a valid snapshot.
     */
    public boolean execute(final File clusterDir)
    {
        if (!clusterDir.exists() || !clusterDir.isDirectory())
        {
            throw new IllegalArgumentException("invalid cluster directory: " + clusterDir.getAbsolutePath());
        }

        final RecordingLog.Entry entry = ClusterTool.findLatestValidSnapshot(clusterDir);
        if (null == entry)
        {
            throw new ClusterException("no valid snapshot found");
        }

        final long recordingId = entry.recordingId;
        final ClusterNodeControlProperties properties = ClusterTool.loadControlProperties(clusterDir);
        final RecordingSignalCapture recordingSignalCapture = new RecordingSignalCapture();
        try (Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(properties.aeronDirectoryName));
            AeronArchive archive = AeronArchive.connect(new AeronArchive.Context()
                .controlRequestChannel(IPC_CHANNEL)
                .controlResponseChannel(IPC_CHANNEL)
                .recordingSignalConsumer(recordingSignalCapture)
                .aeron(aeron)))
        {
            final SnapshotReader snapshotReader = new SnapshotReader();
            replayLocalSnapshotRecording(aeron, archive, recordingId, snapshotReader);
            final long targetNextServiceSessionId =
                snapshotReader.logServiceSessionId + 1 + snapshotReader.pendingServiceMessageCount;
            if (targetNextServiceSessionId != snapshotReader.nextServiceSessionId)
            {
                final long tempRecordingId = createNewSnapshotRecording(
                    aeron, archive, recordingId, targetNextServiceSessionId);

                final long stopPosition = awaitRecordingStopPosition(archive, recordingId);
                final long newStopPosition = awaitRecordingStopPosition(archive, tempRecordingId);
                if (stopPosition != newStopPosition)
                {
                    throw new ClusterException("new snapshot recording incomplete: expectedStopPosition=" +
                        stopPosition + ", actualStopPosition=" + newStopPosition);
                }

                archive.truncateRecording(recordingId, 0);
                awaitRecordingSignal(archive, recordingSignalCapture, recordingId, RecordingSignal.DELETE);

                archive.replicate(
                    tempRecordingId,
                    recordingId,
                    archive.context().controlRequestStreamId(),
                    IPC_CHANNEL,
                    null);

                awaitRecordingSignal(archive, recordingSignalCapture, recordingId, RecordingSignal.REPLICATE_END);
                awaitRecordingSignal(archive, recordingSignalCapture, recordingId, RecordingSignal.STOP);

                final long replicatedStopPosition = awaitRecordingStopPosition(archive, recordingId);
                if (stopPosition != replicatedStopPosition)
                {
                    throw new ClusterException("incomplete replication of the new recording: expectedStopPosition=" +
                        stopPosition + ", replicatedStopPosition=" + replicatedStopPosition);
                }

                archive.purgeRecording(tempRecordingId);
                awaitRecordingSignal(archive, recordingSignalCapture, tempRecordingId, RecordingSignal.DELETE);

                return true;
            }
        }

        return false;
    }

    static void replayLocalSnapshotRecording(
        final Aeron aeron,
        final AeronArchive archive,
        final long snapshotRecordingId,
        final ConsensusModuleSnapshotListener listener)
    {
        final String channel = IPC_CHANNEL;
        final int streamId = SNAPSHOT_REPLAY_STREAM_ID;
        final int sessionId = (int)archive.startReplay(
            snapshotRecordingId, 0, AeronArchive.NULL_LENGTH, channel, streamId);
        try
        {
            final String replayChannel = ChannelUri.addSessionId(channel, sessionId);
            try (Subscription subscription = aeron.addSubscription(replayChannel, streamId))
            {
                Image image;
                while (null == (image = subscription.imageBySessionId(sessionId)))
                {
                    archive.checkForErrorResponse();
                    Thread.yield();
                }

                final ConsensusModuleSnapshotAdapter adapter = new ConsensusModuleSnapshotAdapter(image, listener);
                while (true)
                {
                    final int fragments = adapter.poll();
                    if (0 == fragments)
                    {
                        if (adapter.isDone())
                        {
                            break;
                        }

                        if (image.isClosed())
                        {
                            throw new ClusterException("snapshot ended unexpectedly: " + image);
                        }

                        archive.checkForErrorResponse();
                        Thread.yield();
                    }
                }
            }
        }
        finally
        {
            archive.stopAllReplays(snapshotRecordingId);
        }
    }

    private static long createNewSnapshotRecording(
        final Aeron aeron,
        final AeronArchive archive,
        final long oldRecordingId,
        final long targetNextServiceSessionId)
    {
        final RecordingDescriptorCapture oldRecordingDescriptorCapture = loadRecordingDescriptor(
            archive, oldRecordingId);

        final ChannelUri channelUri = ChannelUri.parse(IPC_CHANNEL);
        channelUri.put(CommonContext.ALIAS_PARAM_NAME, "snapshot-patch");
        channelUri.put(CommonContext.MTU_LENGTH_PARAM_NAME, Integer.toString(oldRecordingDescriptorCapture.mtuLength));
        channelUri.initialPosition(
            0, oldRecordingDescriptorCapture.initialTermId, oldRecordingDescriptorCapture.termBufferLength);
        final String channel = channelUri.toString();
        final int streamId = oldRecordingDescriptorCapture.streamId;

        try (ExclusivePublication snapshotPublication = archive.addRecordedExclusivePublication(channel, streamId))
        {
            try
            {
                final int publicationSessionId = snapshotPublication.sessionId();
                final CountersReader countersReader = aeron.countersReader();
                final int counterId = awaitRecordingCounter(publicationSessionId, countersReader);
                final long newRecordingId = RecordingPos.getRecordingId(countersReader, counterId);

                replayLocalSnapshotRecording(
                    aeron,
                    archive,
                    oldRecordingId,
                    new SnapshotWriter(snapshotPublication, targetNextServiceSessionId));

                awaitRecordingComplete(countersReader, counterId, snapshotPublication.position(), newRecordingId);

                return newRecordingId;
            }
            finally
            {
                archive.stopRecording(snapshotPublication);
            }
        }
    }

    private static RecordingDescriptorCapture loadRecordingDescriptor(
        final AeronArchive archive, final long oldRecordingId)
    {
        final RecordingDescriptorCapture recordingDescriptorCapture = new RecordingDescriptorCapture();
        if (1 != archive.listRecording(oldRecordingId, recordingDescriptorCapture))
        {
            throw new ClusterException("failed to read recording descriptor for id: " + oldRecordingId);
        }

        return recordingDescriptorCapture;
    }

    private static int awaitRecordingCounter(final int publicationSessionId, final CountersReader countersReader)
    {
        int counterId;
        while (NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(countersReader, publicationSessionId)))
        {
            Thread.yield();
        }

        return counterId;
    }

    private static void awaitRecordingComplete(
        final CountersReader counters, final int counterId, final long position, final long recordingId)
    {
        while (counters.getCounterValue(counterId) < position)
        {
            Thread.yield();
            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new ClusterException("recording has stopped unexpectedly: " + recordingId);
            }
        }
    }

    private static long awaitRecordingStopPosition(final AeronArchive archive, final long recordingId)
    {
        long stopPosition;
        while (NULL_VALUE == (stopPosition = archive.getStopPosition(recordingId)))
        {
            Thread.yield();
        }

        return stopPosition;
    }

    private static void awaitRecordingSignal(
        final AeronArchive archive,
        final RecordingSignalCapture recordingSignalCapture,
        final long recordingId,
        final RecordingSignal expectedSignal)
    {
        recordingSignalCapture.reset();
        while (recordingId != recordingSignalCapture.recordingId || expectedSignal != recordingSignalCapture.signal)
        {
            if (0 == archive.pollForRecordingSignals())
            {
                Thread.yield();
            }
        }
    }

    /**
     * Entry point to launch the tool. Requires a single parameter of a cluster directory.
     *
     * @param args for the tool.
     */
    public static void main(final String[] args)
    {
        if (1 != args.length)
        {
            System.out.println("Usage: <cluster-dir>");
            System.exit(-1);
        }

        new ConsensusModuleSnapshotPendingServiceMessagesPatch().execute(new File(args[0]));
    }

    private static final class SnapshotReader implements ConsensusModuleSnapshotListener
    {
        private long nextServiceSessionId = NULL_SESSION_ID;
        private long logServiceSessionId = NULL_SESSION_ID;
        private int pendingServiceMessageCount = 0;

        public void onLoadBeginSnapshot(
            final int appVersion,
            final TimeUnit timeUnit,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadConsensusModuleState(
            final long nextSessionId,
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            this.nextServiceSessionId = nextServiceSessionId;
            this.logServiceSessionId = logServiceSessionId;
        }

        public void onLoadPendingMessage(
            final long clusterSessionId, final DirectBuffer buffer, final int offset, final int length)
        {
            pendingServiceMessageCount++;
        }

        public void onLoadClusterMembers(
            final int memberId,
            final int highMemberId,
            final String clusterMembers,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadClusterSession(
            final long clusterSessionId,
            final long correlationId,
            final long openedLogPosition,
            final long timeOfLastActivity,
            final CloseReason closeReason,
            final int responseStreamId,
            final String responseChannel,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadTimer(
            final long correlationId,
            final long deadline,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadPendingMessageTracker(
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final int serviceId,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
        }

        public void onLoadEndSnapshot(final DirectBuffer buffer, final int offset, final int length)
        {
        }
    }

    private static final class SnapshotWriter implements ConsensusModuleSnapshotListener
    {
        private final ExpandableArrayBuffer tempBuffer = new ExpandableArrayBuffer(1024);
        private final ConsensusModuleEncoder consensusModuleEncoder = new ConsensusModuleEncoder();
        private final SessionMessageHeaderEncoder sessionMessageHeaderEncoder = new SessionMessageHeaderEncoder();
        private final ExclusivePublication snapshotPublication;
        private final long targetNextServiceSessionId;
        private long nextClusterSessionId;

        SnapshotWriter(final ExclusivePublication snapshotPublication, final long targetNextServiceSessionId)
        {
            this.snapshotPublication = snapshotPublication;
            this.targetNextServiceSessionId = targetNextServiceSessionId;
        }

        public void onLoadBeginSnapshot(
            final int appVersion,
            final TimeUnit timeUnit,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            writeToASnapshot(buffer, offset, length);
        }

        public void onLoadConsensusModuleState(
            final long nextSessionId,
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            nextClusterSessionId = logServiceSessionId + 1;

            tempBuffer.putBytes(0, buffer, offset, length);
            consensusModuleEncoder
                .wrap(tempBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
                .nextServiceSessionId(targetNextServiceSessionId);

            writeToASnapshot(tempBuffer, 0, length);
        }

        public void onLoadClusterMembers(
            final int memberId,
            final int highMemberId,
            final String clusterMembers,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            writeToASnapshot(buffer, offset, length);
        }

        public void onLoadPendingMessage(
            final long clusterSessionId,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            tempBuffer.putBytes(0, buffer, offset, length);
            sessionMessageHeaderEncoder
                .wrap(tempBuffer, MessageHeaderEncoder.ENCODED_LENGTH)
                .clusterSessionId(nextClusterSessionId++);

            writeToASnapshot(tempBuffer, 0, length);
        }

        public void onLoadClusterSession(
            final long clusterSessionId,
            final long correlationId,
            final long openedLogPosition,
            final long timeOfLastActivity,
            final CloseReason closeReason,
            final int responseStreamId,
            final String responseChannel,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            writeToASnapshot(buffer, offset, length);
        }

        public void onLoadTimer(
            final long correlationId,
            final long deadline,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            writeToASnapshot(buffer, offset, length);
        }

        public void onLoadPendingMessageTracker(
            final long nextServiceSessionId,
            final long logServiceSessionId,
            final int pendingMessageCapacity,
            final int serviceId,
            final DirectBuffer buffer,
            final int offset,
            final int length)
        {
            writeToASnapshot(buffer, offset, length);
        }

        public void onLoadEndSnapshot(final DirectBuffer buffer, final int offset, final int length)
        {
            writeToASnapshot(buffer, offset, length);
        }

        private void writeToASnapshot(final DirectBuffer buffer, final int offset, final int length)
        {
            long result;
            while ((result = snapshotPublication.offer(buffer, offset, length)) < 0)
            {
                if (result == CLOSED ||
                    result == NOT_CONNECTED ||
                    result == MAX_POSITION_EXCEEDED)
                {
                    throw new ClusterException("can't offer into a snapshot: " + result);
                }

                Thread.yield();
            }
        }
    }

    private static final class RecordingSignalCapture implements RecordingSignalConsumer
    {
        long recordingId;
        RecordingSignal signal;

        public void onSignal(
            final long controlSessionId,
            final long correlationId,
            final long recordingId,
            final long subscriptionId,
            final long position,
            final RecordingSignal signal)
        {
            if (NULL_VALUE != recordingId)
            {
                this.recordingId = recordingId;
            }
            this.signal = signal;
        }

        void reset()
        {
            recordingId = NULL_VALUE;
            signal = null;
        }
    }

    private static final class RecordingDescriptorCapture implements RecordingDescriptorConsumer
    {
        int initialTermId;
        int termBufferLength;
        int mtuLength;
        int streamId;

        public void onRecordingDescriptor(
            final long controlSessionId,
            final long correlationId,
            final long recordingId,
            final long startTimestamp,
            final long stopTimestamp,
            final long startPosition,
            final long stopPosition,
            final int initialTermId,
            final int segmentFileLength,
            final int termBufferLength,
            final int mtuLength,
            final int sessionId,
            final int streamId,
            final String strippedChannel,
            final String originalChannel,
            final String sourceIdentity)
        {
            this.initialTermId = initialTermId;
            this.termBufferLength = termBufferLength;
            this.mtuLength = mtuLength;
            this.streamId = streamId;
        }
    }
}
