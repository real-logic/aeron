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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.*;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.generateRandomDirName;
import static io.aeron.archive.ArchiveSystemTests.*;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
class ReplicateRecordingTest
{
    private static final int SRC_CONTROL_STREAM_ID = AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT;
    private static final String SRC_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8090";
    private static final String SRC_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String DST_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8095";
    private static final String DST_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String SRC_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String DST_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:0";
    private static final String REPLAY_CHANNEL = "aeron:udp?endpoint=localhost:6666";
    private static final int REPLAY_STREAM_ID = 101;
    private static final long TIMER_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(15);

    private static final int LIVE_STREAM_ID = 1033;
    private static final String LIVE_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .controlEndpoint("localhost:8100")
        .termLength(TERM_LENGTH)
        .build();
    private TestMediaDriver srcDriver;
    private Archive srcArchive;
    private TestMediaDriver dstDriver;
    private Archive dstArchive;
    private Aeron srcAeron;
    private Aeron dstAeron;
    private AeronArchive srcAeronArchive;
    private AeronArchive dstAeronArchive;
    private TestRecordingSignalConsumer srcRecordingSignalConsumer;
    private TestRecordingSignalConsumer dstRecordingSignalConsumer;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void before()
    {
        final String srcAeronDirectoryName = generateRandomDirName();
        final String dstAeronDirectoryName = generateRandomDirName();

        final MediaDriver.Context srcContext = new MediaDriver.Context()
            .aeronDirectoryName(srcAeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(true)
            .timerIntervalNs(TIMER_INTERVAL_NS)
            .dirDeleteOnStart(true);

        final Archive.Context srcArchiveCtx = new Archive.Context()
            .catalogCapacity(CATALOG_CAPACITY)
            .aeronDirectoryName(srcAeronDirectoryName)
            .controlChannel(SRC_CONTROL_REQUEST_CHANNEL)
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel(SRC_CONTROL_RESPONSE_CHANNEL))
            .recordingEventsEnabled(false)
            .replicationChannel(SRC_REPLICATION_CHANNEL)
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "src-archive"))
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED);
        final MediaDriver.Context dstContext = new MediaDriver.Context()
            .aeronDirectoryName(dstAeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(true)
            .timerIntervalNs(TIMER_INTERVAL_NS)
            .dirDeleteOnStart(true);
        final Archive.Context dstArchiveCtx = new Archive.Context()
            .catalogCapacity(CATALOG_CAPACITY)
            .aeronDirectoryName(dstAeronDirectoryName)
            .controlChannel(DST_CONTROL_REQUEST_CHANNEL)
            .archiveClientContext(new AeronArchive.Context().controlResponseChannel(DST_CONTROL_RESPONSE_CHANNEL))
            .recordingEventsEnabled(false)
            .replicationChannel(DST_REPLICATION_CHANNEL)
            .deleteArchiveOnStart(true)
            .archiveDir(new File(SystemUtil.tmpDirName(), "dst-archive"))
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED);

        try
        {
            srcDriver = TestMediaDriver.launch(srcContext, systemTestWatcher);
            srcArchive = Archive.launch(srcArchiveCtx);
            dstDriver = TestMediaDriver.launch(dstContext, systemTestWatcher);
            dstArchive = Archive.launch(dstArchiveCtx);
        }
        finally
        {
            systemTestWatcher.dataCollector().add(srcContext.aeronDirectory());
            systemTestWatcher.dataCollector().add(dstContext.aeronDirectory());
            systemTestWatcher.dataCollector().add(dstArchiveCtx.archiveDir());
            systemTestWatcher.dataCollector().add(srcArchiveCtx.archiveDir());
        }

        srcAeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(srcAeronDirectoryName));

        dstAeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(dstAeronDirectoryName));

        srcAeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .idleStrategy(YieldingIdleStrategy.INSTANCE)
                .controlRequestChannel(SRC_CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(SRC_CONTROL_RESPONSE_CHANNEL)
                .aeron(srcAeron));

        dstAeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .idleStrategy(YieldingIdleStrategy.INSTANCE)
                .controlRequestChannel(DST_CONTROL_REQUEST_CHANNEL)
                .controlResponseChannel(DST_CONTROL_RESPONSE_CHANNEL)
                .aeron(dstAeron));

        srcRecordingSignalConsumer = injectRecordingSignalConsumer(srcAeronArchive);
        dstRecordingSignalConsumer = injectRecordingSignalConsumer(dstAeronArchive);
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(
            srcAeronArchive,
            dstAeronArchive,
            srcAeron,
            dstAeron,
            srcArchive,
            dstArchive,
            dstDriver,
            srcDriver);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldThrowExceptionWhenDstRecordingIdUnknown(final boolean useParams)
    {
        final long unknownId = 7L;
        try
        {
            if (useParams)
            {
                dstAeronArchive.replicate(
                    NULL_VALUE,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().dstRecordingId(unknownId));
            }
            else
            {
                dstAeronArchive.replicate(
                    NULL_VALUE, unknownId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }
        }
        catch (final ArchiveException ex)
        {
            assertEquals(ArchiveException.UNKNOWN_RECORDING, ex.errorCode());
            assertTrue(ex.getMessage().endsWith(Long.toString(unknownId)));
            return;
        }

        fail("expected archive exception");
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldThrowExceptionWhenSrcRecordingIdUnknown(final boolean useParams)
    {
        final long unknownId = 7L;
        if (useParams)
        {
            dstAeronArchive.replicate(
                unknownId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
        }
        else
        {
            dstAeronArchive.replicate(
                unknownId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
        }

        String errorResponse;
        while (null == (errorResponse = dstAeronArchive.pollForErrorResponse()))
        {
            Thread.yield();
        }
        assertEquals("unknown src recording id " + unknownId, errorResponse);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateStoppedRecording(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);

        dstRecordingSignalConsumer.reset();
        if (useParams)
        {
            dstAeronArchive.replicate(
                srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
        }
        else
        {
            dstAeronArchive.replicate(
                srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
        }

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
        final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateWithOlderVersion(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);

        dstRecordingSignalConsumer.reset();
        if (useParams)
        {
            dstAeronArchive.replicate(
                srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
        }
        else
        {
            dstAeronArchive.replicate(
                srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
        }

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
        final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateStoppedRecordingsConcurrently(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long[] srcRecordingIds = new long[2];
        long position = 0;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        for (int i = 0; i < 2; i++)
        {
            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader counters = srcAeron.countersReader();
                final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId());
                srcRecordingIds[i] = RecordingPos.getRecordingId(counters, counterId);

                offer(publication, messageCount, messagePrefix);
                position = publication.position();
                Tests.awaitPosition(counters, counterId, position);
            }
            awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingIds[i], RecordingSignal.STOP);
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);

        for (int i = 0; i < 2; i++)
        {
            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.archiveProxy().replicate(
                    srcRecordingIds[i],
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams(),
                    dstAeronArchive.context().aeron().nextCorrelationId(),
                    dstAeronArchive.controlSessionId());
            }
            else
            {
                dstAeronArchive.archiveProxy().replicate(
                    srcRecordingIds[i],
                    NULL_VALUE,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    null,
                    dstAeronArchive.context().aeron().nextCorrelationId(),
                    dstAeronArchive.controlSessionId());
            }
        }

        int stopCount = 0;
        while (stopCount < 2)
        {
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.STOP);
            stopCount++;
        }

        assertEquals(dstAeronArchive.getStopPosition(0), position);
        assertEquals(dstAeronArchive.getStopPosition(1), position);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveWithoutMergingRecording(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(srcCounters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            final long replicationId;
            if (useParams)
            {
                replicationId = dstAeronArchive.replicate(
                    srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
            }
            else
            {
                replicationId = dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(
                dstAeronArchive,
                dstRecordingSignalConsumer,
                dstRecordingId,
                RecordingSignal.EXTEND);

            final CountersReader dstCounters = dstAeron.countersReader();
            final int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            dstRecordingSignalConsumer.reset();
            dstAeronArchive.stopReplication(replicationId);
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.STOP);
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveRecordingAndStopAtSpecifiedPosition(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(srcCounters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            final long firstPosition = publication.position();
            Tests.awaitPosition(srcCounters, counterId, firstPosition);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().stopPosition(firstPosition));
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    NULL_VALUE,
                    firstPosition,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    null,
                    null);
            }

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.STOP);

            offer(publication, messageCount, messagePrefix);
            final int srcCounterId = RecordingPos.findCounterIdByRecording(srcCounters, srcRecordingId);
            Tests.awaitPosition(srcCounters, srcCounterId, publication.position());

            assertTrue(firstPosition < publication.position());
            long dstStopPosition;
            while (NULL_POSITION == (dstStopPosition = dstAeronArchive.getStopPosition(dstRecordingId)))
            {
                Tests.yield();
            }
            assertEquals(firstPosition, dstStopPosition);
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateMoreThanOnce(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(srcCounters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            long replicationId;
            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                replicationId = dstAeronArchive.replicate(
                    srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
            }
            else
            {
                replicationId = dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);

            final CountersReader dstCounters = dstAeron.countersReader();
            int dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            dstRecordingSignalConsumer.reset();
            dstAeronArchive.stopReplication(replicationId);
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.STOP);

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                replicationId = dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().dstRecordingId(dstRecordingId));
            }
            else
            {
                replicationId = dstAeronArchive.replicate(
                    srcRecordingId, dstRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);

            dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            dstRecordingSignalConsumer.reset();
            dstAeronArchive.stopReplication(replicationId);
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.STOP);
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateSyncedRecording(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(srcCounters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            srcRecordingSignalConsumer.reset();
            srcAeronArchive.stopRecording(subscriptionId);
            awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, new ReplicationParams());
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);


            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.SYNC);
            resetAndAwaitSignal(
                dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.REPLICATE_END);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.STOP);

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().dstRecordingId(dstRecordingId));
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId, dstRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.SYNC);
            resetAndAwaitSignal(
                dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.REPLICATE_END);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveRecordingAndMerge(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        srcRecordingSignalConsumer.reset();
        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, RecordingSignal.START);
            final long signaledRecordingId = srcRecordingSignalConsumer.recordingId;
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(srcCounters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);
            assertEquals(srcRecordingId, signaledRecordingId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().liveDestination(LIVE_CHANNEL));
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, LIVE_CHANNEL);
            }

            offer(publication, messageCount, messagePrefix);

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
            final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.MERGE);

            final CountersReader dstCounters = dstAeron.countersReader();
            final int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveRecordingAndMergeBeforeDataFlows(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;
        final long dstRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(srcCounters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                dstAeronArchive.replicate(
                    srcRecordingId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    new ReplicationParams().liveDestination(LIVE_CHANNEL));
            }
            else
            {
                dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, LIVE_CHANNEL);
            }

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
            dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.MERGE);

            final CountersReader dstCounters = dstAeron.countersReader();
            final int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());
        }

        dstRecordingSignalConsumer.reset();
        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.REPLICATE_END);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    @InterruptAfter(10)
    void shouldReplicateLiveRecordingAndMergeWhileFollowingWithTaggedSubscription(final boolean useParams)
    {
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long srcRecordingId;
        final long dstRecordingId;
        final long channelTagId = dstAeron.nextCorrelationId();
        final long subscriptionTagId = dstAeron.nextCorrelationId();
        final String taggedChannel =
            "aeron:udp?control-mode=manual|rejoin=false|tags=" + channelTagId + "," + subscriptionTagId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID);
            Subscription taggedSubscription = dstAeron.addSubscription(taggedChannel, LIVE_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(srcCounters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            Tests.awaitPosition(srcCounters, counterId, publication.position());

            dstRecordingSignalConsumer.reset();
            if (useParams)
            {
                final ReplicationParams replicationParams = new ReplicationParams()
                    .liveDestination(LIVE_CHANNEL)
                    .channelTagId(channelTagId)
                    .subscriptionTagId(subscriptionTagId);

                dstAeronArchive.replicate(
                    srcRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, replicationParams);
            }
            else
            {
                dstAeronArchive.taggedReplicate(
                    srcRecordingId,
                    NULL_VALUE,
                    channelTagId,
                    subscriptionTagId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    LIVE_CHANNEL);
            }

            consume(taggedSubscription, messageCount, messagePrefix);

            offer(publication, messageCount, messagePrefix);
            consume(taggedSubscription, messageCount, messagePrefix);

            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
            dstRecordingId = dstRecordingSignalConsumer.recordingId;
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);
            resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.MERGE);

            final CountersReader dstCounters = dstAeron.countersReader();
            final int dstCounterId =
                RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);

            offer(publication, messageCount, messagePrefix);
            consume(taggedSubscription, messageCount, messagePrefix);
            Tests.awaitPosition(dstCounters, dstCounterId, publication.position());

            final Image image = taggedSubscription.imageBySessionId(publication.sessionId());
            assertEquals(publication.position(), image.position());
        }

        dstRecordingSignalConsumer.reset();
        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.REPLICATE_END);
    }

    @Test
    @InterruptAfter(10)
    public void shouldReplicateStoppedRecordingWithFileIoMaxLength()
    {
        final String messagePrefix = "Message-Prefix-";
        final int fileIoMaxLength = 1500;
        final int longMessagePadding = (2 * fileIoMaxLength) + 1000;

        final StringBuilder longMessagePrefix = new StringBuilder();
        longMessagePrefix.append(messagePrefix);
        while (longMessagePrefix.length() < longMessagePadding)
        {
            longMessagePrefix.append('X');
        }
        longMessagePrefix.append('-');

        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, longMessagePrefix.toString());
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);

        dstRecordingSignalConsumer.reset();
        dstAeronArchive.replicate(
            srcRecordingId,
            SRC_CONTROL_STREAM_ID,
            SRC_CONTROL_REQUEST_CHANNEL,
            new ReplicationParams().fileIoMaxLength(fileIoMaxLength));

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, RecordingSignal.REPLICATE);
        final long dstRecordingId = dstRecordingSignalConsumer.recordingId;
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.STOP);

        assertNotEquals(NULL_VALUE, dstRecordingId);

        validateRecordingAreEqual(srcRecordingId, dstRecordingId);
    }

    @Test
    @InterruptAfter(10)
    public void shouldErrorReplicateIfFileIoMaxLengthIsLessThanMtu()
    {
        final String messagePrefix = "Message-Prefix-";
        final int fileIoMaxLength = 1500;
        final int longMessagePadding = (2 * fileIoMaxLength) + 1000;

        final StringBuilder longMessagePrefix = new StringBuilder();
        longMessagePrefix.append(messagePrefix);
        while (longMessagePrefix.length() < longMessagePadding)
        {
            longMessagePrefix.append('X');
        }
        longMessagePrefix.append('-');

        final int messageCount = 10;
        final long srcRecordingId;

        final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId());
            srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, longMessagePrefix.toString());
            Tests.awaitPosition(counters, counterId, publication.position());
        }

        srcRecordingSignalConsumer.reset();
        srcAeronArchive.stopRecording(subscriptionId);
        awaitSignal(srcAeronArchive, srcRecordingSignalConsumer, srcRecordingId, RecordingSignal.STOP);

        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);
        final int i = srcAeronArchive.listRecording(srcRecordingId, collector.reset());
        assertEquals(1, i);

        final RecordingDescriptor descriptor = collector.descriptors().get(0);
        final int mtu = descriptor.mtuLength();

        dstAeronArchive.replicate(
            srcRecordingId,
            SRC_CONTROL_STREAM_ID,
            SRC_CONTROL_REQUEST_CHANNEL,
            new ReplicationParams().fileIoMaxLength(mtu - 1));

        String error;
        while (null == (error = dstAeronArchive.pollForErrorResponse()))
        {
            Tests.yield();
        }

        assertThat(error, Matchers.containsString("mtuLength"));
        assertThat(error, Matchers.containsString("fileIoMaxLength"));
    }

    @ParameterizedTest
    @InterruptAfter(10)
    @CsvSource({
        "aeron:ipc?alias=src-recording|mtu=1344|init-term-id=777|term-id=1111112|term-offset=4096|" +
            "term-length=512K, aeron:udp?alias=OTHER|endpoint=localhost:8108|term-length=1G, 5, 1",
        "aeron:udp?alias=OTHER|endpoint=localhost:8108|term-length=1G, aeron:ipc?alias=dst-recording|mtu=1344|" +
            "init-term-id=1111111|term-id=1111112|term-offset=4096|term-length=512K, 3, 10",
        "aeron:udp?endpoint=localhost:8108|mtu=1344|init-term-id=11|term-id=15|term-offset=1024|term-length=512K, " +
            "aeron:udp?endpoint=localhost:8109|mtu=1376|init-term-id=222|term-id=333|term-offset=96|term-length=256M" +
            ", 7, 4",
        "aeron:ipc?alias=src, aeron:udp?alias=dst|endpoint=localhost:8080, 21, 21",
        "aeron:udp?alias=src|endpoint=localhost:8080|init-term-id=3|term-id=5|term-length=64K|term-offset=64, " +
            "aeron:ipc?alias=dst|init-term-id=11|term-id=13|term-length=64K|term-offset=2752, 42, 19"
    })
    public void shouldReplicateStoppedRecordingOverAnExistingTruncatedRecordingReplacingAllParameters(
        final String srcChannel, final String dstChannel, final int srcMessageCount, final int dstMessageCount)
    {
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(1);
        final int srcStreamId = 3333;
        final long srcRecordingId = createStoppedRecording(
            srcAeronArchive,
            srcRecordingSignalConsumer,
            srcChannel,
            srcStreamId,
            "src recording data", srcMessageCount);

        int dstStreamId = 555;
        long dstRecordingId;
        while (srcRecordingId >= (dstRecordingId = createStoppedRecording(
            dstAeronArchive, dstRecordingSignalConsumer, "aeron:ipc?term-length=64K", dstStreamId, "temp", 1)))
        {
            dstRecordingSignalConsumer.reset();
            dstAeronArchive.truncateRecording(dstRecordingId, 0);
            awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.DELETE);
            dstStreamId++;
        }

        dstRecordingId = createStoppedRecording(
            dstAeronArchive, dstRecordingSignalConsumer, dstChannel, dstStreamId, "destination 42", dstMessageCount);
        assertNotEquals(srcRecordingId, dstRecordingId);
        assertNotEquals(srcStreamId, dstStreamId);

        assertEquals(1, srcAeronArchive.listRecording(srcRecordingId, collector.reset()));
        final RecordingDescriptor srcRecording = collector.descriptors().get(0).retain();
        assertEquals(1, dstAeronArchive.listRecording(dstRecordingId, collector.reset()));
        final RecordingDescriptor dstRecording = collector.descriptors().get(0).retain();
        assertNotEquals(srcRecording.startTimestamp(), dstRecording.startTimestamp());
        assertNotEquals(srcRecording.stopTimestamp(), dstRecording.stopTimestamp());
        assertNotEquals(srcRecording.controlSessionId(), dstRecording.controlSessionId());
        assertNotEquals(srcRecording.sessionId(), dstRecording.sessionId());
        assertNotEquals(srcRecording.streamId(), dstRecording.streamId());
        assertNotEquals(srcRecording.strippedChannel(), dstRecording.strippedChannel());
        assertNotEquals(srcRecording.originalChannel(), dstRecording.originalChannel());
        assertEquals(srcRecording.sourceIdentity(), dstRecording.sourceIdentity());

        dstRecordingSignalConsumer.reset();
        dstAeronArchive.truncateRecording(dstRecordingId, dstRecording.startPosition());
        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.DELETE);

        dstRecordingSignalConsumer.reset();
        dstAeronArchive.replicate(
            srcRecordingId, dstRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);

        awaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.EXTEND);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.SYNC);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.REPLICATE_END);
        resetAndAwaitSignal(dstAeronArchive, dstRecordingSignalConsumer, dstRecordingId, RecordingSignal.STOP);

        assertEquals(1, dstAeronArchive.listRecording(dstRecordingId, collector.reset()));
        final RecordingDescriptor replicatedRecording = collector.descriptors().get(0).retain();
        assertEquals(srcRecording.startTimestamp(), replicatedRecording.startTimestamp());
        assertEquals(srcRecording.startPosition(), replicatedRecording.startPosition());
        assertEquals(srcRecording.stopPosition(), replicatedRecording.stopPosition());
        assertEquals(srcRecording.initialTermId(), replicatedRecording.initialTermId());
        assertEquals(srcRecording.segmentFileLength(), replicatedRecording.segmentFileLength());
        assertEquals(srcRecording.termBufferLength(), replicatedRecording.termBufferLength());
        assertEquals(srcRecording.mtuLength(), replicatedRecording.mtuLength());
        assertEquals(srcRecording.sessionId(), replicatedRecording.sessionId());
        assertEquals(srcRecording.streamId(), replicatedRecording.streamId());
        assertEquals(srcRecording.strippedChannel(), replicatedRecording.strippedChannel());
        assertEquals(srcRecording.originalChannel(), replicatedRecording.originalChannel());
        assertEquals(srcRecording.sourceIdentity(), replicatedRecording.sourceIdentity());
        assertEquals(dstRecording.controlSessionId(), replicatedRecording.controlSessionId());
        // extend recording will overwrite the stopTimestamp
        assertNotEquals(srcRecording.stopTimestamp(), replicatedRecording.stopTimestamp());
        assertNotEquals(dstRecording.stopTimestamp(), replicatedRecording.stopTimestamp());
    }

    private void readRecordingIntoBuffer(final long srcRecordingId, final ExpandableArrayBuffer srcRecordingData)
    {
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);
        final int i = srcAeronArchive.listRecording(srcRecordingId, collector.reset());
        assertEquals(1, i);
        final RecordingDescriptor descriptor = collector.descriptors().get(0);
        final long length = descriptor.stopPosition() - descriptor.startPosition();

        try (Subscription replay = srcAeronArchive.replay(
            srcRecordingId, descriptor.startPosition(), length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            final MutableInteger position = new MutableInteger(0);
            Tests.awaitConnected(replay);

            // Assumes session specific subscription used for replay.
            final Image image = replay.imageAtIndex(0);

            while (!image.isEndOfStream())
            {
                image.poll(
                    (buffer, offset, len, header) ->
                    {
                        srcRecordingData.putBytes(position.get(), buffer, offset, len);
                        position.addAndGet(len);
                    },
                    10);
            }
        }
    }

    private long createStoppedRecording(
        final AeronArchive aeronArchive,
        final TestRecordingSignalConsumer recordingSignalConsumer,
        final String channel,
        final int streamId,
        final String payload,
        final int messageCount)
    {
        try (ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId))
        {
            long recordingId = Long.MIN_VALUE;
            try
            {
                final CountersReader counters = aeronArchive.context().aeron().countersReader();
                final int counterId = Tests.awaitRecordingCounterId(counters, publication.sessionId());
                recordingId = RecordingPos.getRecordingId(counters, counterId);

                offer(publication, messageCount, payload);
                Tests.awaitPosition(counters, counterId, publication.position());
                return recordingId;
            }
            finally
            {
                recordingSignalConsumer.reset();
                aeronArchive.stopRecording(publication);
                awaitSignal(aeronArchive, recordingSignalConsumer, recordingId, RecordingSignal.STOP);
            }
        }
    }

    private void validateRecordingAreEqual(final long srcRecordingId, final long dstRecordingId)
    {
        final ExpandableArrayBuffer srcRecordingData = new ExpandableArrayBuffer();
        readRecordingIntoBuffer(srcRecordingId, srcRecordingData);

        final ExpandableArrayBuffer dstRecordingData = new ExpandableArrayBuffer();
        readRecordingIntoBuffer(dstRecordingId, dstRecordingData);

        assertEquals(srcRecordingData, dstRecordingData);
    }
}
