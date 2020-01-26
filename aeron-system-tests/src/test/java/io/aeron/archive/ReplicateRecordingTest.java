/*
 * Copyright 2014-2020 Real Logic Limited.
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
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;
import org.agrona.collections.ObjectHashSet;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.generateRandomDirName;
import static io.aeron.archive.Common.*;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

public class ReplicateRecordingTest
{
    private static final int SRC_CONTROL_STREAM_ID = AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT;
    private static final String SRC_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8090";
    private static final String SRC_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:8091";
    private static final String DST_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8095";
    private static final String DST_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:8096";
    private static final String SRC_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:8040";
    private static final String DST_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:8041";

    private static final int LIVE_STREAM_ID = 1033;
    private static final String LIVE_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .controlEndpoint("localhost:8100")
        .termLength(TERM_LENGTH)
        .build();

    private ArchivingMediaDriver srcArchivingMediaDriver;
    private ArchivingMediaDriver dstArchivingMediaDriver;
    private Aeron srcAeron;
    private Aeron dstAeron;
    private AeronArchive srcAeronArchive;
    private AeronArchive dstAeronArchive;

    @BeforeEach
    public void before()
    {
        final String srcAeronDirectoryName = generateRandomDirName();
        final String dstAeronDirectoryName = generateRandomDirName();

        srcArchivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(srcAeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(true)
                .dirDeleteOnShutdown(true)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(srcAeronDirectoryName)
                .controlChannel(SRC_CONTROL_REQUEST_CHANNEL)
                .archiveClientContext(new AeronArchive.Context().controlResponseChannel(SRC_CONTROL_RESPONSE_CHANNEL))
                .recordingEventsEnabled(false)
                .replicationChannel(SRC_REPLICATION_CHANNEL)
                .deleteArchiveOnStart(true)
                .archiveDir(new File(SystemUtil.tmpDirName(), "src-archive"))
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

        dstArchivingMediaDriver = ArchivingMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(dstAeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Throwable::printStackTrace)
                .spiesSimulateConnection(true)
                .dirDeleteOnShutdown(true)
                .dirDeleteOnStart(true),
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .aeronDirectoryName(dstAeronDirectoryName)
                .controlChannel(DST_CONTROL_REQUEST_CHANNEL)
                .archiveClientContext(new AeronArchive.Context().controlResponseChannel(DST_CONTROL_RESPONSE_CHANNEL))
                .recordingEventsEnabled(false)
                .replicationChannel(DST_REPLICATION_CHANNEL)
                .deleteArchiveOnStart(true)
                .archiveDir(new File(SystemUtil.tmpDirName(), "dst-archive"))
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

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
    }

    @AfterEach
    public void after()
    {
        CloseHelper.close(srcAeronArchive);
        CloseHelper.close(dstAeronArchive);
        CloseHelper.close(srcAeron);
        CloseHelper.close(dstAeron);
        CloseHelper.close(dstArchivingMediaDriver);
        CloseHelper.close(srcArchivingMediaDriver);

        dstArchivingMediaDriver.archive().context().deleteArchiveDirectory();
        srcArchivingMediaDriver.archive().context().deleteArchiveDirectory();
    }

    @Test
    public void shouldThrowExceptionWhenDstRecordingIdUnknown()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            final long unknownId = 7L;
            try
            {
                dstAeronArchive.replicate(
                    NULL_VALUE, unknownId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);
            }
            catch (final ArchiveException ex)
            {
                assertEquals(ArchiveException.UNKNOWN_RECORDING, ex.errorCode());
                assertTrue(ex.getMessage().endsWith(Long.toString(unknownId)));
                return;
            }

            fail("expected archive exception");
        });
    }

    @Test
    public void shouldReplicateStoppedRecording()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            final String messagePrefix = "Message-Prefix-";
            final int messageCount = 10;
            final long srcRecordingId;

            final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader counters = srcAeron.countersReader();
                final int counterId = awaitRecordingCounterId(counters, publication.sessionId());
                srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

                offer(publication, messageCount, messagePrefix);
                awaitPosition(counters, counterId, publication.position());
            }

            srcAeronArchive.stopRecording(subscriptionId);

            final MutableLong dstRecordingId = new MutableLong();
            final MutableReference<RecordingSignal> signalRef = new MutableReference<>();
            final RecordingSignalAdapter adapter = newRecordingSignalAdapter(signalRef, dstRecordingId);

            dstAeronArchive.replicate(
                srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);

            awaitSignal(signalRef, adapter);
            assertEquals(RecordingSignal.REPLICATE, signalRef.get());

            awaitSignal(signalRef, adapter);
            assertEquals(RecordingSignal.EXTEND, signalRef.get());

            final ObjectHashSet<RecordingSignal> transitionEventsSet = new ObjectHashSet<>();
            awaitSignal(signalRef, adapter);
            transitionEventsSet.add(signalRef.get());

            awaitSignal(signalRef, adapter);
            transitionEventsSet.add(signalRef.get());

            assertTrue(transitionEventsSet.contains(RecordingSignal.STOP));
            assertTrue(transitionEventsSet.contains(RecordingSignal.SYNC));
        });
    }

    @Test
    public void shouldReplicateStoppedRecordingTwiceConcurrently()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            final String messagePrefix = "Message-Prefix-";
            final int messageCount = 10;
            final long srcRecordingId;
            final long position;

            final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader counters = srcAeron.countersReader();
                final int counterId = awaitRecordingCounterId(counters, publication.sessionId());
                srcRecordingId = RecordingPos.getRecordingId(counters, counterId);

                offer(publication, messageCount, messagePrefix);
                position = publication.position();
                awaitPosition(counters, counterId, position);
            }

            srcAeronArchive.stopRecording(subscriptionId);

            final MutableLong dstRecordingId = new MutableLong();
            final MutableReference<RecordingSignal> signalRef = new MutableReference<>();
            final RecordingSignalAdapter adapter = newRecordingSignalAdapter(signalRef, dstRecordingId);

            dstAeronArchive.replicate(
                srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);

            dstAeronArchive.replicate(
                srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);

            int stopCount = 0;
            while (stopCount < 2)
            {
                awaitSignal(signalRef, adapter);
                if (signalRef.get() == RecordingSignal.STOP)
                {
                    stopCount++;
                }
            }

            assertEquals(dstAeronArchive.getStopPosition(0), position);
            assertEquals(dstAeronArchive.getStopPosition(1), position);
        });
    }

    @Test
    public void shouldReplicateLiveWithoutMergingRecording()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            final String messagePrefix = "Message-Prefix-";
            final int messageCount = 10;
            final long srcRecordingId;

            final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader srcCounters = srcAeron.countersReader();
                final int counterId = awaitRecordingCounterId(srcCounters, publication.sessionId());
                srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

                offer(publication, messageCount, messagePrefix);
                awaitPosition(srcCounters, counterId, publication.position());

                final MutableLong dstRecordingId = new MutableLong();
                final MutableReference<RecordingSignal> signalRef = new MutableReference<>();
                final RecordingSignalAdapter adapter = newRecordingSignalAdapter(signalRef, dstRecordingId);

                final long replicationId = dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.REPLICATE, signalRef.get());

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.EXTEND, signalRef.get());

                final CountersReader dstCounters = dstAeron.countersReader();
                final int dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId.get());
                awaitPosition(dstCounters, dstCounterId, publication.position());

                offer(publication, messageCount, messagePrefix);
                awaitPosition(dstCounters, dstCounterId, publication.position());

                dstAeronArchive.stopReplication(replicationId);

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.STOP, signalRef.get());
            }

            srcAeronArchive.stopRecording(subscriptionId);
        });
    }

    @Test
    public void shouldReplicateMoreThanOnce()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            final String messagePrefix = "Message-Prefix-";
            final int messageCount = 10;
            final long srcRecordingId;

            final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);

            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader srcCounters = srcAeron.countersReader();
                final int counterId = awaitRecordingCounterId(srcCounters, publication.sessionId());
                srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

                offer(publication, messageCount, messagePrefix);
                awaitPosition(srcCounters, counterId, publication.position());

                final MutableLong recordingIdRef = new MutableLong();
                final MutableReference<RecordingSignal> signalRef = new MutableReference<>();
                final RecordingSignalAdapter adapter = newRecordingSignalAdapter(signalRef, recordingIdRef);

                long replicationId = dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.REPLICATE, signalRef.get());

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.EXTEND, signalRef.get());

                final CountersReader dstCounters = dstAeron.countersReader();
                final long dstRecordingId = recordingIdRef.get();
                int dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);
                awaitPosition(dstCounters, dstCounterId, publication.position());

                dstAeronArchive.stopReplication(replicationId);

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.STOP, signalRef.get());

                replicationId = dstAeronArchive.replicate(
                    srcRecordingId, dstRecordingId, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, null);

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.EXTEND, signalRef.get());

                dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);

                offer(publication, messageCount, messagePrefix);
                awaitPosition(dstCounters, dstCounterId, publication.position());

                dstAeronArchive.stopReplication(replicationId);
            }

            srcAeronArchive.stopRecording(subscriptionId);
        });
    }

    @Test
    public void shouldReplicateLiveRecordingAndMerge()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            final String messagePrefix = "Message-Prefix-";
            final int messageCount = 10;
            final long srcRecordingId;

            final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
            final MutableReference<RecordingSignal> signalRef = new MutableReference<>();
            final RecordingSignalAdapter adapter;

            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader srcCounters = srcAeron.countersReader();
                final int counterId = awaitRecordingCounterId(srcCounters, publication.sessionId());
                srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

                offer(publication, messageCount, messagePrefix);
                awaitPosition(srcCounters, counterId, publication.position());

                final MutableLong dstRecordingId = new MutableLong();
                adapter = newRecordingSignalAdapter(signalRef, dstRecordingId);

                dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, LIVE_CHANNEL);

                offer(publication, messageCount, messagePrefix);

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.REPLICATE, signalRef.get());

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.EXTEND, signalRef.get());

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.MERGE, signalRef.get());

                final CountersReader dstCounters = dstAeron.countersReader();
                final int dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId.get());

                offer(publication, messageCount, messagePrefix);
                awaitPosition(dstCounters, dstCounterId, publication.position());
            }

            srcAeronArchive.stopRecording(subscriptionId);

            awaitSignal(signalRef, adapter);
            assertEquals(RecordingSignal.STOP, signalRef.get());
        });
    }

    @Test
    public void shouldReplicateLiveRecordingAndMergeBeforeDataFlows()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            final String messagePrefix = "Message-Prefix-";
            final int messageCount = 10;
            final long srcRecordingId;

            final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
            final MutableReference<RecordingSignal> signalRef = new MutableReference<>();
            final RecordingSignalAdapter adapter;

            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID))
            {
                final CountersReader srcCounters = srcAeron.countersReader();
                final int counterId = awaitRecordingCounterId(srcCounters, publication.sessionId());
                srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

                final MutableLong dstRecordingId = new MutableLong();
                adapter = newRecordingSignalAdapter(signalRef, dstRecordingId);

                dstAeronArchive.replicate(
                    srcRecordingId, NULL_VALUE, SRC_CONTROL_STREAM_ID, SRC_CONTROL_REQUEST_CHANNEL, LIVE_CHANNEL);

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.REPLICATE, signalRef.get());

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.EXTEND, signalRef.get());

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.MERGE, signalRef.get());

                final CountersReader dstCounters = dstAeron.countersReader();
                final int dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId.get());

                offer(publication, messageCount, messagePrefix);
                awaitPosition(dstCounters, dstCounterId, publication.position());
            }

            srcAeronArchive.stopRecording(subscriptionId);

            awaitSignal(signalRef, adapter);
            assertEquals(RecordingSignal.STOP, signalRef.get());
        });
    }

    @Test
    public void shouldReplicateLiveRecordingAndMergeWhileFollowingWithTaggedSubscription()
    {
        assertTimeoutPreemptively(ofSeconds(10), () ->
        {
            final String messagePrefix = "Message-Prefix-";
            final int messageCount = 10;
            final long srcRecordingId;
            final long channelTagId = 333;
            final long subscriptionTagId = 777;
            final String taggedChannel = "aeron:udp?control-mode=manual|tags=" + channelTagId + "," + subscriptionTagId;

            final long subscriptionId = srcAeronArchive.startRecording(LIVE_CHANNEL, LIVE_STREAM_ID, LOCAL);
            final MutableReference<RecordingSignal> signalRef = new MutableReference<>();
            final RecordingSignalAdapter adapter;

            try (Publication publication = srcAeron.addPublication(LIVE_CHANNEL, LIVE_STREAM_ID);
                Subscription taggedSubscription = dstAeron.addSubscription(taggedChannel, LIVE_STREAM_ID))
            {
                final CountersReader srcCounters = srcAeron.countersReader();
                final int counterId = awaitRecordingCounterId(srcCounters, publication.sessionId());
                srcRecordingId = RecordingPos.getRecordingId(srcCounters, counterId);

                offer(publication, messageCount, messagePrefix);
                awaitPosition(srcCounters, counterId, publication.position());

                final MutableLong dstRecordingId = new MutableLong();
                adapter = newRecordingSignalAdapter(signalRef, dstRecordingId);

                dstAeronArchive.taggedReplicate(
                    srcRecordingId,
                    NULL_VALUE,
                    channelTagId,
                    subscriptionTagId,
                    SRC_CONTROL_STREAM_ID,
                    SRC_CONTROL_REQUEST_CHANNEL,
                    LIVE_CHANNEL);

                consume(taggedSubscription, messageCount, messagePrefix);

                offer(publication, messageCount, messagePrefix);
                consume(taggedSubscription, messageCount, messagePrefix);

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.REPLICATE, signalRef.get());

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.EXTEND, signalRef.get());

                awaitSignal(signalRef, adapter);
                assertEquals(RecordingSignal.MERGE, signalRef.get());

                final CountersReader dstCounters = dstAeron.countersReader();
                final int dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId.get());

                offer(publication, messageCount, messagePrefix);
                consume(taggedSubscription, messageCount, messagePrefix);
                awaitPosition(dstCounters, dstCounterId, publication.position());

                final Image image = taggedSubscription.imageBySessionId(publication.sessionId());
                assertEquals(publication.position(), image.position());
            }

            srcAeronArchive.stopRecording(subscriptionId);

            awaitSignal(signalRef, adapter);
            assertEquals(RecordingSignal.STOP, signalRef.get());
        });
    }

    private RecordingSignalAdapter newRecordingSignalAdapter(
        final MutableReference<RecordingSignal> signalRef, final MutableLong recordingIdRef)
    {
        final ControlEventListener listener =
            (controlSessionId, correlationId, relevantId, code, errorMessage) ->
            {
                if (code == ControlResponseCode.ERROR)
                {
                    fail(errorMessage + " " + code);
                }
            };

        final RecordingSignalConsumer consumer =
            (controlSessionId, correlationId, recordingId, subscriptionId, position, transitionType) ->
            {
                recordingIdRef.set(recordingId);
                signalRef.set(transitionType);
            };

        final Subscription subscription = dstAeronArchive.controlResponsePoller().subscription();
        final long controlSessionId = dstAeronArchive.controlSessionId();

        return new RecordingSignalAdapter(controlSessionId, listener, consumer, subscription, FRAGMENT_LIMIT);
    }
}
