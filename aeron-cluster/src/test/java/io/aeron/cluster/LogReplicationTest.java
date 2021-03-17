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

import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class LogReplicationTest
{
    private static final long SRC_RECORDING_ID = 1;
    private static final long DST_RECORDING_ID = 2;
    private static final int SRC_ARCHIVE_STREAM_ID = 3;
    private static final long REPLICATION_ID = 4;
    private static final String ENDPOINT = "localhost:20123";
    private static final long PROGRESS_CHECK_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long PROGRESS_CHECK_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    private final AeronArchive aeronArchive = mock(AeronArchive.class);
    private final ControlResponsePoller controlResponsePoller = mock(ControlResponsePoller.class);
    private final Subscription subscription = mock(Subscription.class);


    @BeforeEach
    void setUp()
    {
        when(aeronArchive.controlResponsePoller()).thenReturn(controlResponsePoller);
        when(controlResponsePoller.subscription()).thenReturn(subscription);
        when(aeronArchive.replicate(anyLong(), anyLong(), anyInt(), anyString(), nullable(String.class), anyLong()))
            .thenReturn(REPLICATION_ID);
    }

    @ParameterizedTest
    @ValueSource(strings = { "START", "STOP", "SYNC", "EXTEND", "REPLICATE", "MERGE" })
    void shouldBeDoneWhenRecordingPositionMatchesStopPositionRegardlessOfSignal(final String recordingSignalString)
    {
        final RecordingSignal recordingSignal = RecordingSignal.valueOf(recordingSignalString);

        final long stopPosition = 982734;
        final long nowNs = 0;

        final LogReplication logReplication = new LogReplication(
            aeronArchive, SRC_RECORDING_ID, DST_RECORDING_ID, SRC_ARCHIVE_STREAM_ID, ENDPOINT, stopPosition,
            PROGRESS_CHECK_TIMEOUT_NS, PROGRESS_CHECK_INTERVAL_NS);

        assertFalse(logReplication.isDone(nowNs));

        logReplication.onSignal(-1, REPLICATION_ID, DST_RECORDING_ID, -1, stopPosition - 1, recordingSignal);
        assertFalse(logReplication.isDone(nowNs));

        logReplication.onSignal(-1, REPLICATION_ID, DST_RECORDING_ID, -1, stopPosition, recordingSignal);
        assertTrue(logReplication.isDone(nowNs));
    }

    @ParameterizedTest
    @ValueSource(strings = { "START", "SYNC", "EXTEND", "REPLICATE", "MERGE" })
    void shouldStopReplicationIfNotAlreadyStopped(final String recordingSignalString)
    {
        final RecordingSignal recordingSignal = RecordingSignal.valueOf(recordingSignalString);

        final long stopPosition = 982734;
        final long nowNs = 0;

        final LogReplication logReplication = new LogReplication(
            aeronArchive, SRC_RECORDING_ID, DST_RECORDING_ID, SRC_ARCHIVE_STREAM_ID, ENDPOINT, stopPosition,
            PROGRESS_CHECK_TIMEOUT_NS, PROGRESS_CHECK_INTERVAL_NS);

        logReplication.onSignal(-1, REPLICATION_ID, DST_RECORDING_ID, -1, stopPosition, recordingSignal);
        assertTrue(logReplication.isDone(nowNs));

        logReplication.close();
        verify(aeronArchive).stopReplication(REPLICATION_ID);
    }

    @Test
    void shouldNotStopReplicationIfStopSignalled()
    {
        final RecordingSignal recordingSignal = RecordingSignal.STOP;

        final long stopPosition = 982734;
        final long nowNs = 0;

        final LogReplication logReplication = new LogReplication(
            aeronArchive, SRC_RECORDING_ID, DST_RECORDING_ID, SRC_ARCHIVE_STREAM_ID, ENDPOINT, stopPosition,
            PROGRESS_CHECK_TIMEOUT_NS, PROGRESS_CHECK_INTERVAL_NS);

        logReplication.onSignal(-1, REPLICATION_ID, DST_RECORDING_ID, -1, stopPosition, recordingSignal);
        assertTrue(logReplication.isDone(nowNs));

        logReplication.close();
        verify(aeronArchive, never()).stopReplication(anyLong());
    }

    @Test
    void shouldFailIfRecordingLogIsDeletedDuringReplication()
    {
        final RecordingSignal recordingSignal = RecordingSignal.DELETE;
        final long stopPosition = 982734;

        final LogReplication logReplication = new LogReplication(
            aeronArchive, SRC_RECORDING_ID, DST_RECORDING_ID, SRC_ARCHIVE_STREAM_ID, ENDPOINT, stopPosition,
            PROGRESS_CHECK_TIMEOUT_NS, PROGRESS_CHECK_INTERVAL_NS);

        assertThrows(
            ClusterException.class,
            () -> logReplication.onSignal(-1, REPLICATION_ID, DST_RECORDING_ID, -1, stopPosition - 1, recordingSignal));
    }

    @Test
    void shouldFailIfRecordingMovesPastStopPosition()
    {
        final long stopPosition = 982734;

        final LogReplication logReplication = new LogReplication(
            aeronArchive, SRC_RECORDING_ID, DST_RECORDING_ID, SRC_ARCHIVE_STREAM_ID, ENDPOINT, stopPosition,
            PROGRESS_CHECK_TIMEOUT_NS, PROGRESS_CHECK_INTERVAL_NS);

        logReplication.onSignal(-1, REPLICATION_ID, DST_RECORDING_ID, -1, stopPosition + 1, RecordingSignal.SYNC);

        assertThrows(ClusterException.class, () -> logReplication.isDone(0));
    }

    @Test
    void shouldPollForProgressAndFailIfNotProgressing()
    {
        final long stopPosition = 982734;
        when(aeronArchive.getRecordingPosition(DST_RECORDING_ID)).thenReturn(stopPosition - 1);

        final LogReplication logReplication = new LogReplication(
            aeronArchive, SRC_RECORDING_ID, DST_RECORDING_ID, SRC_ARCHIVE_STREAM_ID, ENDPOINT, stopPosition,
            PROGRESS_CHECK_TIMEOUT_NS, PROGRESS_CHECK_INTERVAL_NS);

        logReplication.onSignal(-1, REPLICATION_ID, DST_RECORDING_ID, -1, stopPosition - 1, RecordingSignal.SYNC);

        final long t0 = 20L;

        assertFalse(logReplication.isDone(t0));

        assertFalse(logReplication.isDone(t0 + (logReplication.progressCheckIntervalNs() - 1)));
        verify(aeronArchive, never()).getRecordingPosition(anyLong());

        assertFalse(logReplication.isDone(t0 + logReplication.progressCheckIntervalNs()));
        verify(aeronArchive).getRecordingPosition(DST_RECORDING_ID);

        assertFalse(logReplication.isDone(t0 + (logReplication.progressCheckTimeoutNs() - 1)));

        assertThrows(ClusterException.class, () -> logReplication.isDone(t0 + logReplication.progressCheckTimeoutNs()));
    }
}