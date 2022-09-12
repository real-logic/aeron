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
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.cluster.client.ClusterException;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class LogReplicationTest
{
    private static final long SRC_RECORDING_ID = 1;
    private static final long DST_RECORDING_ID = 2;
    private static final long REPLICATION_ID = 4;

    private static final String ENDPOINT = "localhost:20123";
    private static final String REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final long PROGRESS_CHECK_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    private static final long PROGRESS_CHECK_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    private final CountersReader countersReader = mock(CountersReader.class);
    private final AeronArchive aeronArchive = mock(AeronArchive.class);
    private final ControlResponsePoller controlResponsePoller = mock(ControlResponsePoller.class);
    private final Subscription subscription = mock(Subscription.class);

    @BeforeEach
    void setUp()
    {
        final AeronArchive.Context ctx = mock(AeronArchive.Context.class);
        final Aeron aeron = mock(Aeron.class);
        when(aeronArchive.context()).thenReturn(ctx);
        when(ctx.aeron()).thenReturn(aeron);
        when(aeron.countersReader()).thenReturn(countersReader);

        when(aeronArchive.controlResponsePoller()).thenReturn(controlResponsePoller);
        when(controlResponsePoller.subscription()).thenReturn(subscription);
        when(aeronArchive.replicate(
            anyLong(), anyLong(), anyLong(), anyInt(), anyString(), nullable(String.class), nullable(String.class)))
            .thenReturn(REPLICATION_ID);
    }

    @ParameterizedTest
    @EnumSource(value = RecordingSignal.class, mode = EnumSource.Mode.EXCLUDE, names = { "DELETE", "NULL_VAL" })
    void shouldBeDoneWhenRecordingPositionMatchesStopPositionRegardlessOfSignal(final RecordingSignal recordingSignal)
    {
        final long stopPosition = 982734;
        final long nowNs = 0;

        final LogReplication logReplication = new LogReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            DST_RECORDING_ID,
            stopPosition,
            ENDPOINT,
            REPLICATION_CHANNEL,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);

        assertFalse(logReplication.isDone(nowNs));

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition - 1, recordingSignal);
        assertFalse(logReplication.isDone(nowNs));

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition, recordingSignal);
        if (RecordingSignal.REPLICATE_END == recordingSignal)
        {
            assertTrue(logReplication.isDone(nowNs));
        }
        else
        {
            assertFalse(logReplication.isDone(nowNs));
        }
    }

    @ParameterizedTest
    @EnumSource(value = RecordingSignal.class, names = { "START", "SYNC", "EXTEND", "REPLICATE", "MERGE" })
    void shouldStopReplicationIfNotAlreadyStopped(final RecordingSignal recordingSignal)
    {
        final long stopPosition = 982734;
        final long nowNs = 0;

        final LogReplication logReplication = new LogReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            DST_RECORDING_ID,
            stopPosition,
            ENDPOINT,
            REPLICATION_CHANNEL,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition, recordingSignal);
        assertFalse(logReplication.isDone(nowNs));

        logReplication.close();
        verify(aeronArchive).tryStopReplication(REPLICATION_ID);
    }

    @Test
    void shouldNotStopReplicationIfStopSignalled()
    {
        final long stopPosition = 982734;
        final long nowNs = 0;

        final LogReplication logReplication = new LogReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            DST_RECORDING_ID,
            stopPosition,
            ENDPOINT,
            REPLICATION_CHANNEL,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition, RecordingSignal.STOP);

        logReplication.close();
        verify(aeronArchive, never()).stopReplication(anyLong());
    }

    @Test
    void shouldFailIfRecordingLogIsDeletedDuringReplication()
    {
        final RecordingSignal recordingSignal = RecordingSignal.DELETE;
        final long stopPosition = 982734;
        final long nowNs = 0;

        final LogReplication logReplication = new LogReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            DST_RECORDING_ID,
            stopPosition,
            ENDPOINT,
            REPLICATION_CHANNEL,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);

        assertThrows(
            ClusterException.class,
            () -> logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition - 1, recordingSignal));
    }

    @Test
    void shouldFailIfRecordingMovesPastStopPosition()
    {
        final long stopPosition = 982734;
        final long nowNs = 0;

        final LogReplication logReplication = new LogReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            DST_RECORDING_ID,
            stopPosition,
            ENDPOINT,
            REPLICATION_CHANNEL,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            nowNs);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, stopPosition + 1, RecordingSignal.STOP);

        assertThrows(ClusterException.class, () -> logReplication.isDone(0));
    }

    @Test
    void shouldPollForProgressAndFailIfNotProgressing()
    {
        final long stopPosition = 982734;
        final long t0 = 20L;

        final LogReplication logReplication = new LogReplication(
            aeronArchive,
            SRC_RECORDING_ID,
            DST_RECORDING_ID,
            stopPosition,
            ENDPOINT,
            REPLICATION_CHANNEL,
            PROGRESS_CHECK_TIMEOUT_NS,
            PROGRESS_CHECK_INTERVAL_NS,
            t0);

        logReplication.onSignal(REPLICATION_ID, DST_RECORDING_ID, 0, RecordingSignal.EXTEND);

        assertFalse(logReplication.isDone(t0));

        assertFalse(logReplication.isDone(t0 + (PROGRESS_CHECK_INTERVAL_NS - 1)));

        assertFalse(logReplication.isDone(t0 + PROGRESS_CHECK_INTERVAL_NS));

        assertThrows(
            ClusterException.class,
            () -> logReplication.isDone(t0 + PROGRESS_CHECK_INTERVAL_NS + PROGRESS_CHECK_TIMEOUT_NS));
    }
}
