/*
 * Copyright 2014-2019 Real Logic Ltd.
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
import io.aeron.archive.codecs.RecordingTransitionType;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.Common.*;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReplicateRecordingTest
{
    private static final int SRC_CONTROL_STREAM_ID = AeronArchive.Configuration.CONTROL_STREAM_ID_DEFAULT;
    private static final String SRC_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8090";
    private static final String DST_CONTROL_REQUEST_CHANNEL = "aeron:udp?endpoint=localhost:8091";
    private static final String SRC_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:8095";
    private static final String DST_CONTROL_RESPONSE_CHANNEL = "aeron:udp?endpoint=localhost:8096";
    private static final String SRC_EVENTS_CHANNEL = "aeron:udp?control-mode=dynamic|control=localhost:8030";
    private static final String DST_EVENTS_CHANNEL = "aeron:udp?control-mode=dynamic|control=localhost:8031";
    private static final String SRC_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:8040";
    private static final String DST_REPLICATION_CHANNEL = "aeron:udp?endpoint=localhost:8041";

    private static final int RECORDING_STREAM_ID = 33;
    private static final String RECORDING_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .termLength(TERM_BUFFER_LENGTH)
        .build();

    private ArchivingMediaDriver srcArchivingMediaDriver;
    private ArchivingMediaDriver dstArchivingMediaDriver;
    private Aeron srcAeron;
    private Aeron dstAeron;
    private AeronArchive srcAeronArchive;
    private AeronArchive dstAeronArchive;

    @Before
    public void before()
    {
        final String srcAeronDirectoryName = CommonContext.generateRandomDirName();
        final String dstAeronDirectoryName = CommonContext.generateRandomDirName();

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
                .recordingEventsChannel(SRC_EVENTS_CHANNEL)
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
                .recordingEventsChannel(DST_EVENTS_CHANNEL)
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
                .controlRequestChannel(SRC_CONTROL_REQUEST_CHANNEL)
                .aeron(dstAeron));

        dstAeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .controlRequestChannel(DST_CONTROL_REQUEST_CHANNEL)
                .aeron(dstAeron));
    }

    @After
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

    @Test(timeout = 10_000L)
    public void shouldThrowExceptionWhenDstRecordingIdUnknown()
    {
        final long unknownId = 7L;
        final boolean liveMerge = false;
        try
        {
            dstAeronArchive.replicate(
                NULL_VALUE, unknownId, SRC_CONTROL_REQUEST_CHANNEL, SRC_CONTROL_STREAM_ID, liveMerge);
        }
        catch (final ArchiveException ex)
        {
            assertEquals(ArchiveException.UNKNOWN_RECORDING, ex.errorCode());
            assertTrue(ex.getMessage().endsWith(Long.toString(unknownId)));
            return;
        }

        fail("expected archive exception");
    }

    @Test(timeout = 10_000L)
    public void shouldReplicatedStoppedRecording()
    {
        final boolean liveMerge = false;
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long recordingId;

        final long subscriptionId = srcAeronArchive.startRecording(RECORDING_CHANNEL, RECORDING_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(RECORDING_CHANNEL, RECORDING_STREAM_ID))
        {
            final CountersReader counters = srcAeron.countersReader();
            final int counterId = getRecordingCounterId(counters, publication.sessionId());
            recordingId = RecordingPos.getRecordingId(counters, counterId);

            offer(publication, messageCount, messagePrefix);
            awaitPosition(counters, counterId, publication.position());
        }

        srcAeronArchive.stopRecording(subscriptionId);

        final MutableLong dstRecordingId = new MutableLong();
        final MutableReference<RecordingTransitionType> transitionTypeRef = new MutableReference<>();
        final RecordingTransitionAdapter adapter = newRecordingTransitionAdapter(transitionTypeRef, dstRecordingId);

        dstAeronArchive.replicate(
            recordingId, NULL_VALUE, SRC_CONTROL_REQUEST_CHANNEL, SRC_CONTROL_STREAM_ID, liveMerge);

        awaitTransition(transitionTypeRef, adapter);
        assertEquals(RecordingTransitionType.REPLICATE, transitionTypeRef.get());

        awaitTransition(transitionTypeRef, adapter);
        assertEquals(RecordingTransitionType.EXTEND, transitionTypeRef.get());

        awaitTransition(transitionTypeRef, adapter);
        assertEquals(RecordingTransitionType.STOP, transitionTypeRef.get());
    }

    @Test(timeout = 10_000L)
    public void shouldReplicatedLiveRecording()
    {
        final boolean liveMerge = false;
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long recordingId;

        final long subscriptionId = srcAeronArchive.startRecording(RECORDING_CHANNEL, RECORDING_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(RECORDING_CHANNEL, RECORDING_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId = getRecordingCounterId(srcCounters, publication.sessionId());
            recordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            awaitPosition(srcCounters, counterId, publication.position());

            final MutableLong dstRecordingId = new MutableLong();
            final MutableReference<RecordingTransitionType> transitionTypeRef = new MutableReference<>();
            final RecordingTransitionAdapter adapter = newRecordingTransitionAdapter(transitionTypeRef, dstRecordingId);

            final long replicationId = dstAeronArchive.replicate(
                recordingId, NULL_VALUE, SRC_CONTROL_REQUEST_CHANNEL, SRC_CONTROL_STREAM_ID, liveMerge);

            awaitTransition(transitionTypeRef, adapter);
            assertEquals(RecordingTransitionType.REPLICATE, transitionTypeRef.get());

            awaitTransition(transitionTypeRef, adapter);
            assertEquals(RecordingTransitionType.EXTEND, transitionTypeRef.get());

            final CountersReader dstCounters = dstAeron.countersReader();
            final int dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId.get());
            awaitPosition(dstCounters, dstCounterId, publication.position());

            offer(publication, messageCount, messagePrefix);
            awaitPosition(srcCounters, counterId, publication.position());
            awaitPosition(dstCounters, dstCounterId, publication.position());

            dstAeronArchive.stopReplication(replicationId);

            awaitTransition(transitionTypeRef, adapter);
            assertEquals(RecordingTransitionType.STOP, transitionTypeRef.get());
        }

        srcAeronArchive.stopRecording(subscriptionId);
    }

    @Test(timeout = 10_000L)
    public void shouldReplicateMoreThanOnce()
    {
        final boolean liveMerge = false;
        final String messagePrefix = "Message-Prefix-";
        final int messageCount = 10;
        final long recordingId;

        final long subscriptionId = srcAeronArchive.startRecording(RECORDING_CHANNEL, RECORDING_STREAM_ID, LOCAL);

        try (Publication publication = srcAeron.addPublication(RECORDING_CHANNEL, RECORDING_STREAM_ID))
        {
            final CountersReader srcCounters = srcAeron.countersReader();
            final int counterId = getRecordingCounterId(srcCounters, publication.sessionId());
            recordingId = RecordingPos.getRecordingId(srcCounters, counterId);

            offer(publication, messageCount, messagePrefix);
            awaitPosition(srcCounters, counterId, publication.position());

            final MutableLong recordingIdRef = new MutableLong();
            final MutableReference<RecordingTransitionType> transitionTypeRef = new MutableReference<>();
            final RecordingTransitionAdapter adapter = newRecordingTransitionAdapter(transitionTypeRef, recordingIdRef);

            long replicationId = dstAeronArchive.replicate(
                recordingId, NULL_VALUE, SRC_CONTROL_REQUEST_CHANNEL, SRC_CONTROL_STREAM_ID, liveMerge);

            awaitTransition(transitionTypeRef, adapter);
            assertEquals(RecordingTransitionType.REPLICATE, transitionTypeRef.get());

            awaitTransition(transitionTypeRef, adapter);
            assertEquals(RecordingTransitionType.EXTEND, transitionTypeRef.get());

            final CountersReader dstCounters = dstAeron.countersReader();
            final long dstRecordingId = recordingIdRef.get();
            int dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);
            awaitPosition(dstCounters, dstCounterId, publication.position());

            dstAeronArchive.stopReplication(replicationId);

            awaitTransition(transitionTypeRef, adapter);
            assertEquals(RecordingTransitionType.STOP, transitionTypeRef.get());

            replicationId = dstAeronArchive.replicate(
                recordingId, dstRecordingId, SRC_CONTROL_REQUEST_CHANNEL, SRC_CONTROL_STREAM_ID, liveMerge);

            awaitTransition(transitionTypeRef, adapter);
            assertEquals(RecordingTransitionType.EXTEND, transitionTypeRef.get());

            dstCounterId = RecordingPos.findCounterIdByRecording(dstCounters, dstRecordingId);

            offer(publication, messageCount, messagePrefix);
            awaitPosition(srcCounters, counterId, publication.position());
            awaitPosition(dstCounters, dstCounterId, publication.position());

            dstAeronArchive.stopReplication(replicationId);
        }

        srcAeronArchive.stopRecording(subscriptionId);
    }

    private RecordingTransitionAdapter newRecordingTransitionAdapter(
        final MutableReference<RecordingTransitionType> transitionTypeRef,
        final MutableLong recordingIdRef)
    {
        final ControlEventListener listener =
            (controlSessionId, correlationId, relevantId, code, errorMessage) ->
            {
                if (code == ControlResponseCode.ERROR)
                {
                    fail(errorMessage + " " + code);
                }
            };

        final RecordingTransitionConsumer consumer =
            (controlSessionId, correlationId, recordingId, subscriptionId, position, transitionType) ->
            {
                recordingIdRef.set(recordingId);
                transitionTypeRef.set(transitionType);
            };

        final Subscription subscription = dstAeronArchive.controlResponsePoller().subscription();
        final long controlSessionId = dstAeronArchive.controlSessionId();

        return new RecordingTransitionAdapter(controlSessionId, listener, consumer, subscription, FRAGMENT_LIMIT);
    }
}
