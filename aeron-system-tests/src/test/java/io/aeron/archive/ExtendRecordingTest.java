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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlEventListener;
import io.aeron.archive.client.RecordingSignalAdapter;
import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;

import static io.aeron.archive.codecs.RecordingSignal.*;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

public class ExtendRecordingTest
{
    private static final String MY_ALIAS = "my-log";
    private static final String MESSAGE_PREFIX = "Message-Prefix-";
    private static final int MTU_LENGTH = Configuration.mtuLength();

    private static final int RECORDED_STREAM_ID = 33;
    private static final String RECORDED_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .mtu(MTU_LENGTH)
        .termLength(ArchiveSystemTests.TERM_LENGTH)
        .alias(MY_ALIAS)
        .build();

    private static final int REPLAY_STREAM_ID = 66;
    private static final String REPLAY_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:6666")
        .build();

    private static final String EXTEND_CHANNEL = new ChannelUriStringBuilder()
        .media("udp")
        .endpoint("localhost:3333")
        .build();

    private TestMediaDriver driver;
    private Archive archive;
    private Aeron aeron;
    private File archiveDir;
    private AeronArchive aeronArchive;

    private final RecordingSignalConsumer recordingSignalConsumer = mock(RecordingSignalConsumer.class);
    private final ArrayList<String> errors = new ArrayList<>();
    private final ControlEventListener controlEventListener =
        (controlSessionId, correlationId, relevantId, code, errorMessage) -> errors.add(errorMessage);

    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @BeforeEach
    public void before()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        if (null == archiveDir)
        {
            archiveDir = new File(SystemUtil.tmpDirName(), "archive");
        }

        driver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectoryName)
                .termBufferSparseFile(true)
                .threadingMode(ThreadingMode.SHARED)
                .errorHandler(Tests::onError)
                .spiesSimulateConnection(false)
                .dirDeleteOnStart(true),
            testWatcher);

        archive = Archive.launch(
            new Archive.Context()
                .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
                .aeronDirectoryName(aeronDirectoryName)
                .archiveDir(archiveDir)
                .errorHandler(Tests::onError)
                .fileSyncLevel(0)
                .threadingMode(ArchiveThreadingMode.SHARED));

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            new AeronArchive.Context()
                .aeron(aeron));
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);

        archive.context().deleteDirectory();
        driver.context().deleteDirectory();
    }

    @Test
    @Timeout(10)
    public void shouldExtendRecordingAndReplay()
    {
        final long controlSessionId = aeronArchive.controlSessionId();
        final RecordingSignalAdapter recordingSignalAdapter;
        final int messageCount = 10;
        final long subscriptionIdOne;
        final long subscriptionIdTwo;
        final long stopOne;
        final long stopTwo;
        final long recordingId;

        try (Publication publication = aeron.addPublication(RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {
            recordingSignalAdapter = new RecordingSignalAdapter(
                controlSessionId,
                controlEventListener,
                recordingSignalConsumer,
                aeronArchive.controlResponsePoller().subscription(),
                ArchiveSystemTests.FRAGMENT_LIMIT);

            subscriptionIdOne = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
            ArchiveSystemTests.pollForSignal(recordingSignalAdapter);

            try
            {
                offer(publication, 0, messageCount);

                final CountersReader counters = aeron.countersReader();
                final int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                recordingId = RecordingPos.getRecordingId(counters, counterId);

                consume(subscription, 0, messageCount);

                stopOne = publication.position();
                ArchiveSystemTests.awaitPosition(counters, counterId, stopOne);
            }
            finally
            {
                aeronArchive.stopRecording(subscriptionIdOne);
                ArchiveSystemTests.pollForSignal(recordingSignalAdapter);
            }
        }

        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector();
        assertEquals(1L, aeronArchive.listRecordingsForUri(0, 1, "alias=" + MY_ALIAS, RECORDED_STREAM_ID, collector));
        final RecordingDescriptor recording = collector.descriptors.get(0);
        assertEquals(recordingId, recording.recordingId);

        final String publicationExtendChannel = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:3333")
            .initialPosition(recording.stopPosition, recording.initialTermId, recording.termBufferLength)
            .mtu(recording.mtuLength)
            .alias(MY_ALIAS)
            .build();

        try (Subscription subscription = Tests.reAddSubscription(aeron, EXTEND_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = aeron.addExclusivePublication(publicationExtendChannel, RECORDED_STREAM_ID))
        {
            subscriptionIdTwo = aeronArchive.extendRecording(recordingId, EXTEND_CHANNEL, RECORDED_STREAM_ID, LOCAL);
            ArchiveSystemTests.pollForSignal(recordingSignalAdapter);

            try
            {
                offer(publication, messageCount, messageCount);

                final CountersReader counters = aeron.countersReader();
                final int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());

                consume(subscription, messageCount, messageCount);

                stopTwo = publication.position();
                ArchiveSystemTests.awaitPosition(counters, counterId, stopTwo);
            }
            finally
            {
                aeronArchive.stopRecording(subscriptionIdTwo);
                ArchiveSystemTests.pollForSignal(recordingSignalAdapter);
            }
        }

        replay(messageCount, stopTwo, recordingId);
        assertEquals(Collections.EMPTY_LIST, errors);

        final InOrder inOrder = Mockito.inOrder(recordingSignalConsumer);
        inOrder.verify(recordingSignalConsumer).onSignal(
            eq(controlSessionId), anyLong(), eq(recordingId), eq(subscriptionIdOne), eq(0L), eq(START));
        inOrder.verify(recordingSignalConsumer).onSignal(
            eq(controlSessionId), anyLong(), eq(recordingId), eq(subscriptionIdOne), eq(stopOne), eq(STOP));
        inOrder.verify(recordingSignalConsumer).onSignal(
            eq(controlSessionId), anyLong(), eq(recordingId), eq(subscriptionIdTwo), eq(stopOne), eq(EXTEND));
        inOrder.verify(recordingSignalConsumer).onSignal(
            eq(controlSessionId), anyLong(), eq(recordingId), eq(subscriptionIdTwo), eq(stopTwo), eq(STOP));
    }

    private void replay(final int messageCount, final long secondStopPosition, final long recordingId)
    {
        final long fromPosition = 0L;
        final long length = secondStopPosition - fromPosition;

        try (Subscription subscription = aeronArchive.replay(
            recordingId, fromPosition, length, REPLAY_CHANNEL, REPLAY_STREAM_ID))
        {
            consume(subscription, 0, messageCount * 2);
            assertEquals(secondStopPosition, subscription.imageAtIndex(0).position());
        }
    }

    private static void offer(final Publication publication, final int startIndex, final int count)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        for (int i = startIndex; i < (startIndex + count); i++)
        {
            final int length = buffer.putStringWithoutLengthAscii(0, MESSAGE_PREFIX + i);

            while (publication.offer(buffer, 0, length) <= 0)
            {
                Tests.yield();
            }
        }
    }

    private static void consume(final Subscription subscription, final int startIndex, final int count)
    {
        final MutableInteger received = new MutableInteger(startIndex);

        final FragmentHandler fragmentHandler = new FragmentAssembler(
            (buffer, offset, length, header) ->
            {
                final String expected = MESSAGE_PREFIX + received.value;
                final String actual = buffer.getStringWithoutLengthAscii(offset, length);

                assertEquals(expected, actual);

                received.value++;
            });

        while (received.value < (startIndex + count))
        {
            if (0 == subscription.poll(fragmentHandler, ArchiveSystemTests.FRAGMENT_LIMIT))
            {
                Tests.yield();
            }
        }

        assertEquals(startIndex + count, received.get());
    }
}
