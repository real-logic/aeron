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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.ArchiveProxy;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.codecs.SnapshotRecordingsDecoder;
import io.aeron.cluster.codecs.SnapshotRecordingsEncoder;
import io.aeron.test.StubNanoClock;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static java.lang.Math.abs;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class BackgroundSnapshotReplicatorTest
{
    private final ConsensusPublisher mockConsensusPublisher = mock(ConsensusPublisher.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final AeronArchive mockAeronArchive = mock(AeronArchive.class);
    private final ArchiveProxy mockArchiveProxy = mock(ArchiveProxy.class);
    private final DistinctErrorLog mockErrorLog = mock(DistinctErrorLog.class);
    private final MutableLong counter = new MutableLong(100);
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final SnapshotRecordingsEncoder recordingsEncoder = new SnapshotRecordingsEncoder();
    private final SnapshotRecordingsDecoder recordingsDecoder = new SnapshotRecordingsDecoder();
    private final ConsensusModule.Context consensusModuleCtx = new ConsensusModule.Context()
        .serviceCount(1)
        .aeron(mockAeron)
        .errorLog(mockErrorLog);
    private final AeronArchive.Context aeronArchiveCtx = new AeronArchive.Context();
    private final StubNanoClock clock = new StubNanoClock(100, 10);
    private final long logPositionForSnapshot = 298374234L;
    private final Random random = new Random();
    private final ClusterMember thisMember = new ClusterMember(
        1, "ingress:1000", "consensus:1001", "log:1002", "catchup:1003", "archive:1004", "endpoints...");
    private final ClusterMember memberTakingSnapshot = new ClusterMember(
        2, "ingress:2000", "consensus:2001", "log:2002", "catchup:2003", "archive:2004", "endpoints...");

    @BeforeEach
    void setUp()
    {
        when(mockAeron.nextCorrelationId()).then(invocation -> counter.incrementAndGet());
        when(mockAeronArchive.archiveProxy()).thenReturn(mockArchiveProxy);
        when(mockAeronArchive.context()).thenReturn(aeronArchiveCtx);
        when(mockAeronArchive.controlSessionId()).thenReturn(9823427234L);

        recordingsEncoder.wrap(buffer, 0);
        recordingsDecoder.wrap(
            buffer, 0, SnapshotRecordingsDecoder.BLOCK_LENGTH, SnapshotRecordingsDecoder.SCHEMA_VERSION);
        consensusModuleCtx.replicationChannel("aeron:udp?endpoint=archive:1004");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    @SuppressWarnings("checkstyle:methodlength")
    void shouldQueryForLatestSnapshot(final boolean sendExtendSignal)
    {
        when(mockArchiveProxy.replicate(
            anyLong(), anyLong(), anyLong(), anyInt(), any(), any(), any(), anyLong(), anyLong())).thenReturn(true);

        final long controlSessionId = mockAeronArchive.controlSessionId();
        final List<TestSnapshotInfo> testSnapshots = createSnapshotInfo(2, logPositionForSnapshot);

        final BackgroundSnapshotReplicator snapshotReplicator = new BackgroundSnapshotReplicator(
            consensusModuleCtx, mockConsensusPublisher);
        snapshotReplicator.archive(mockAeronArchive);

        snapshotReplicator.setLatestSnapshot(memberTakingSnapshot, logPositionForSnapshot);

        // Query
        snapshotReplicator.doWork(clock.nextTime(), thisMember);
        verify(mockConsensusPublisher).snapshotRecordingQuery(any(), anyLong(), eq(thisMember.id()));

        recordingsEncoder.wrap(buffer, 0).correlationId(counter.get());
        applyTestSnapshotsToEncoder(testSnapshots, recordingsEncoder);
        recordingsEncoder.memberEndpoints(memberTakingSnapshot.endpoints());

        snapshotReplicator.onSnapshotRecordings(counter.get(), recordingsDecoder.sbeRewind(), 2);

        // Replicate first snapshot...
        snapshotReplicator.doWork(clock.nextTime(), thisMember);

        final ArgumentCaptor<Long> correlationIdCaptor = ArgumentCaptor.forClass(Long.TYPE);

        verify(mockArchiveProxy).replicate(
            eq(testSnapshots.get(0).srcRecordingId),
            eq(RecordingPos.NULL_RECORDING_ID),
            eq(AeronArchive.NULL_POSITION),
            eq(aeronArchiveCtx.controlRequestStreamId()),
            contains("endpoint=" + memberTakingSnapshot.archiveEndpoint()),
            eq(null),
            eq(consensusModuleCtx.replicationChannel()),
            correlationIdCaptor.capture(),
            eq(controlSessionId));

        snapshotReplicator.onArchiveControlResponse(correlationIdCaptor.getValue(), testSnapshots.get(0).replicationId);
        if (sendExtendSignal)
        {
            snapshotReplicator.onRecordingSignal(
                testSnapshots.get(0).replicationId, testSnapshots.get(0).dstRecordingId, 0, RecordingSignal.EXTEND);
        }
        snapshotReplicator.onRecordingSignal(
            testSnapshots.get(0).replicationId, testSnapshots.get(0).dstRecordingId, 0, RecordingSignal.STOP);

        snapshotReplicator.doWork(clock.nextTime(), thisMember);
        assertFalse(snapshotReplicator.isComplete());

        verify(mockArchiveProxy).replicate(
            eq(testSnapshots.get(1).srcRecordingId),
            eq(RecordingPos.NULL_RECORDING_ID),
            eq(AeronArchive.NULL_POSITION),
            eq(aeronArchiveCtx.controlRequestStreamId()),
            contains("endpoint=" + memberTakingSnapshot.archiveEndpoint()),
            eq(null),
            eq(consensusModuleCtx.replicationChannel()),
            correlationIdCaptor.capture(),
            eq(controlSessionId));

        snapshotReplicator.onArchiveControlResponse(correlationIdCaptor.getValue(), testSnapshots.get(1).replicationId);
        if (sendExtendSignal)
        {
            snapshotReplicator.onRecordingSignal(
                testSnapshots.get(1).replicationId, testSnapshots.get(1).dstRecordingId, 0, RecordingSignal.EXTEND);
        }
        snapshotReplicator.onRecordingSignal(
            testSnapshots.get(1).replicationId, testSnapshots.get(1).dstRecordingId, 0, RecordingSignal.STOP);

        snapshotReplicator.doWork(clock.nextTime(), thisMember);

        assertTrue(snapshotReplicator.isComplete());
        final List<RecordingLog.Snapshot> snapshots = snapshotReplicator.snapshotsRetrieved();
        assertSnapshots(testSnapshots, snapshots);
    }

    @Test
    void shouldRecordErrorIfFailed()
    {
        when(mockArchiveProxy.replicate(
            anyLong(), anyLong(), anyLong(), anyInt(), any(), any(), any(), anyLong(), anyLong())).thenReturn(true);

        final long controlSessionId = mockAeronArchive.controlSessionId();
        final List<TestSnapshotInfo> testSnapshots = createSnapshotInfo(2, logPositionForSnapshot);

        final BackgroundSnapshotReplicator snapshotReplicator = new BackgroundSnapshotReplicator(
            consensusModuleCtx, mockConsensusPublisher);
        snapshotReplicator.archive(mockAeronArchive);

        snapshotReplicator.setLatestSnapshot(memberTakingSnapshot, logPositionForSnapshot);

        // Query
        snapshotReplicator.doWork(clock.nextTime(), thisMember);
        verify(mockConsensusPublisher).snapshotRecordingQuery(any(), anyLong(), eq(thisMember.id()));

        recordingsEncoder.wrap(buffer, 0).correlationId(counter.get());
        applyTestSnapshotsToEncoder(testSnapshots, recordingsEncoder);
        recordingsEncoder.memberEndpoints(memberTakingSnapshot.endpoints());

        snapshotReplicator.onSnapshotRecordings(counter.get(), recordingsDecoder.sbeRewind(), 2);

        // Replicate first snapshot...
        snapshotReplicator.doWork(clock.nextTime(), thisMember);

        final ArgumentCaptor<Long> correlationIdCaptor = ArgumentCaptor.forClass(Long.TYPE);

        verify(mockArchiveProxy).replicate(
            eq(testSnapshots.get(0).srcRecordingId),
            eq(RecordingPos.NULL_RECORDING_ID),
            eq(AeronArchive.NULL_POSITION),
            eq(aeronArchiveCtx.controlRequestStreamId()),
            contains("endpoint=" + memberTakingSnapshot.archiveEndpoint()),
            eq(null),
            eq(consensusModuleCtx.replicationChannel()),
            correlationIdCaptor.capture(),
            eq(controlSessionId));

        final long replicationId1 = counter.incrementAndGet();
        snapshotReplicator.onArchiveControlResponse(correlationIdCaptor.getValue(), replicationId1);
        final ArchiveException failure = new ArchiveException("Some random failure");
        snapshotReplicator.onArchiveControlError(replicationId1, failure);

        // Fail and cleanup
        snapshotReplicator.doWork(clock.nextTime(), thisMember);

        verify(mockErrorLog).record(failure);
        assertTrue(snapshotReplicator.snapshotsRetrieved().isEmpty());
    }

    @Test
    @SuppressWarnings("checkstyle:methodlength")
    void shouldRetryUntilCorrectNumberOfSnapshotsAvailable()
    {
        when(mockArchiveProxy.replicate(
            anyLong(), anyLong(), anyLong(), anyInt(), any(), any(), any(), anyLong(), anyLong())).thenReturn(true);

        final List<TestSnapshotInfo> testSnapshots = createSnapshotInfo(2, logPositionForSnapshot);

        final BackgroundSnapshotReplicator snapshotReplicator = new BackgroundSnapshotReplicator(
            consensusModuleCtx, mockConsensusPublisher);
        snapshotReplicator.archive(mockAeronArchive);

        snapshotReplicator.setLatestSnapshot(memberTakingSnapshot, logPositionForSnapshot);

        // Query
        snapshotReplicator.doWork(clock.nextTime(), thisMember);
        verify(mockConsensusPublisher).snapshotRecordingQuery(any(), anyLong(), eq(thisMember.id()));

        recordingsEncoder.wrap(buffer, 0).correlationId(counter.get());
        applyTestSnapshotsToEncoder(testSnapshots.subList(0, 1), recordingsEncoder);
        recordingsEncoder.memberEndpoints(memberTakingSnapshot.endpoints());
        snapshotReplicator.onSnapshotRecordings(counter.get(), recordingsDecoder.sbeRewind(), 2);

        snapshotReplicator.doWork(clock.nextTime(), thisMember);

        verify(mockArchiveProxy, never()).replicate(
            anyLong(), anyLong(), anyLong(), anyInt(), any(), any(), any(), anyLong(), anyLong());

        snapshotReplicator.doWork(clock.nextTime(consensusModuleCtx.dynamicJoinIntervalNs()), thisMember);
        verify(mockConsensusPublisher, times(2)).snapshotRecordingQuery(any(), anyLong(), eq(thisMember.id()));

        recordingsEncoder.wrap(buffer, 0).correlationId(counter.get());
        applyTestSnapshotsToEncoder(testSnapshots, recordingsEncoder);
        recordingsEncoder.memberEndpoints(memberTakingSnapshot.endpoints());
        snapshotReplicator.onSnapshotRecordings(counter.get(), recordingsDecoder.sbeRewind(), 2);

        snapshotReplicator.doWork(clock.nextTime(), thisMember);

        verify(mockArchiveProxy).replicate(
            anyLong(), anyLong(), anyLong(), anyInt(), any(), any(), any(), anyLong(), anyLong());
    }

    @Test
    @SuppressWarnings("checkstyle:methodlength")
    void shouldNotRespondToSnapshotRecordingsMessageWhenIdle()
    {
        when(mockArchiveProxy.replicate(
            anyLong(), anyLong(), anyLong(), anyInt(), any(), any(), any(), anyLong(), anyLong())).thenReturn(true);

        final List<TestSnapshotInfo> testSnapshots = createSnapshotInfo(2, logPositionForSnapshot);

        final BackgroundSnapshotReplicator snapshotReplicator = new BackgroundSnapshotReplicator(
            consensusModuleCtx, mockConsensusPublisher);
        snapshotReplicator.archive(mockAeronArchive);

        recordingsEncoder.wrap(buffer, 0).correlationId(counter.get());
        applyTestSnapshotsToEncoder(testSnapshots, recordingsEncoder);
        recordingsEncoder.memberEndpoints(memberTakingSnapshot.endpoints());
        snapshotReplicator.onSnapshotRecordings(counter.get(), recordingsDecoder.sbeRewind(), 2);

        // Replicate first snapshot...
        snapshotReplicator.doWork(clock.nextTime(), thisMember);

        verify(mockArchiveProxy, never()).replicate(
            anyLong(), anyLong(), anyLong(), anyInt(), any(), any(), any(), anyLong(), anyLong());
    }

    private static void assertSnapshots(
        final List<TestSnapshotInfo> testSnapshots,
        final List<RecordingLog.Snapshot> snapshots)
    {
        assertEquals(testSnapshots.size(), snapshots.size());
        for (int i = 0; i < testSnapshots.size(); i++)
        {
            final TestSnapshotInfo expectedSnapshot = testSnapshots.get(i);
            final RecordingLog.Snapshot snapshot = snapshots.get(i);

            assertEquals(expectedSnapshot.dstRecordingId, snapshot.recordingId);
            assertEquals(expectedSnapshot.leadershipTermId, snapshot.leadershipTermId);
            assertEquals(expectedSnapshot.logPosition, snapshot.logPosition);
            assertEquals(expectedSnapshot.serviceId, snapshot.serviceId);
            assertEquals(expectedSnapshot.termBaseLogPosition, snapshot.termBaseLogPosition);
            assertEquals(expectedSnapshot.timestamp, snapshot.timestamp);
        }
    }

    private static void applyTestSnapshotsToEncoder(
        final List<TestSnapshotInfo> testSnapshots,
        final SnapshotRecordingsEncoder recordingsEncoder)
    {
        final SnapshotRecordingsEncoder.SnapshotsEncoder snapshotsEncoder = recordingsEncoder.snapshotsCount(
            testSnapshots.size());

        for (int i = 0; i < testSnapshots.size(); i++)
        {
            snapshotsEncoder.next()
                .recordingId(testSnapshots.get(i).srcRecordingId)
                .leadershipTermId(testSnapshots.get(i).leadershipTermId)
                .termBaseLogPosition(testSnapshots.get(i).termBaseLogPosition)
                .logPosition(testSnapshots.get(i).logPosition)
                .timestamp(testSnapshots.get(i).timestamp)
                .serviceId(testSnapshots.get(i).serviceId);
        }
    }

    static final class TestSnapshotInfo
    {
        final long srcRecordingId;
        final long dstRecordingId;
        final long logPosition;
        final long leadershipTermId;
        final long termBaseLogPosition;
        final long timestamp;
        final long replicationId;
        final int serviceId;

        TestSnapshotInfo(
            final long srcRecordingId,
            final long dstRecordingId,
            final long logPosition,
            final long leadershipTermId,
            final long termBaseLogPosition,
            final long timestamp,
            final long replicationId,
            final int serviceId)
        {
            this.srcRecordingId = srcRecordingId;
            this.dstRecordingId = dstRecordingId;
            this.logPosition = logPosition;
            this.leadershipTermId = leadershipTermId;
            this.termBaseLogPosition = termBaseLogPosition;
            this.timestamp = timestamp;
            this.replicationId = replicationId;
            this.serviceId = serviceId;
        }

        static TestSnapshotInfo create(final long logPosition, final int serviceId, final Random random)
        {
            return new TestSnapshotInfo(
                abs(random.nextLong()),
                abs(random.nextLong()),
                logPosition,
                abs(random.nextLong()),
                abs(random.nextLong()),
                abs(random.nextLong()),
                abs(random.nextLong()),
                serviceId);
        }
    }

    private List<TestSnapshotInfo> createSnapshotInfo(final int numSnapshots, final long logPosition)
    {
        assertTrue(0 < numSnapshots);

        final List<TestSnapshotInfo> snapshots = new ArrayList<>();

        snapshots.add(TestSnapshotInfo.create(logPosition, ConsensusModule.Configuration.SERVICE_ID, random));
        for (int i = 1; i < numSnapshots; i++)
        {
            final int serviceId = i - 1;
            snapshots.add(TestSnapshotInfo.create(logPosition, serviceId, random));
        }

        return snapshots;
    }
}