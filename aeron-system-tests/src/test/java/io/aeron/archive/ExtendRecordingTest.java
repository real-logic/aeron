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
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.io.File;

import static io.aeron.archive.codecs.RecordingSignal.*;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;

@ExtendWith(InterruptingTestCallback.class)
class ExtendRecordingTest
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

    private final RecordingSignalConsumer mockRecordingSignalConsumer = mock(RecordingSignalConsumer.class);

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @BeforeEach
    void before()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        if (null == archiveDir)
        {
            archiveDir = new File(SystemUtil.tmpDirName(), "archive");
        }

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .spiesSimulateConnection(false)
            .dirDeleteOnStart(true);

        final Archive.Context archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(ArchiveSystemTests.CATALOG_CAPACITY)
            .aeronDirectoryName(aeronDirectoryName)
            .archiveDir(archiveDir)
            .fileSyncLevel(0)
            .threadingMode(ArchiveThreadingMode.SHARED);

        try
        {
            driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
            archive = Archive.launch(archiveCtx);
        }
        finally
        {
            systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
            systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());
        }

        aeron = Aeron.connect(
            new Aeron.Context()
                .aeronDirectoryName(aeronDirectoryName));

        aeronArchive = AeronArchive.connect(
            TestContexts.localhostAeronArchive()
                .recordingSignalConsumer(mockRecordingSignalConsumer)
                .aeron(aeron));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeronArchive, aeron, archive, driver);
    }

    private interface PublicationFactory
    {
        Publication create(Aeron aeron, String uri, int streamId);
    }

    @InterruptAfter(10)
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void shouldExtendRecordingAndReplay(final boolean exclusive)
    {
        final long controlSessionId = aeronArchive.controlSessionId();
        final int messageCount = 10;
        final long subscriptionIdOne;
        final long subscriptionIdTwo;
        final long stopOne;
        final long stopTwo;
        final long recordingId;

        final PublicationFactory publicationFactory =
            exclusive ? Aeron::addExclusivePublication : Aeron::addPublication;

        try (Publication publication = publicationFactory.create(aeron, RECORDED_CHANNEL, RECORDED_STREAM_ID);
            Subscription subscription = aeron.addSubscription(RECORDED_CHANNEL, RECORDED_STREAM_ID))
        {

            subscriptionIdOne = aeronArchive.startRecording(RECORDED_CHANNEL, RECORDED_STREAM_ID, LOCAL);
            pollForRecordingSignal(aeronArchive);

            try
            {
                offer(publication, 0, messageCount);

                final CountersReader counters = aeron.countersReader();
                final int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                recordingId = RecordingPos.getRecordingId(counters, counterId);

                consume(subscription, 0, messageCount);

                stopOne = publication.position();
                Tests.awaitPosition(counters, counterId, stopOne);
            }
            finally
            {
                aeronArchive.stopRecording(subscriptionIdOne);
                pollForRecordingSignal(aeronArchive);
            }
        }

        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(10);
        assertEquals(
            1L, aeronArchive.listRecordingsForUri(0, 1, "alias=" + MY_ALIAS, RECORDED_STREAM_ID, collector.reset()));
        final RecordingDescriptor recording = collector.descriptors().get(0);
        assertEquals(recordingId, recording.recordingId());

        final String publicationExtendChannel = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:3333")
            .initialPosition(recording.stopPosition(), recording.initialTermId(), recording.termBufferLength())
            .mtu(recording.mtuLength())
            .alias(MY_ALIAS)
            .build();

        try (Subscription subscription = Tests.reAddSubscription(aeron, EXTEND_CHANNEL, RECORDED_STREAM_ID);
            Publication publication = publicationFactory.create(aeron, publicationExtendChannel, RECORDED_STREAM_ID))
        {
            subscriptionIdTwo = aeronArchive.extendRecording(recordingId, EXTEND_CHANNEL, RECORDED_STREAM_ID, LOCAL);
            pollForRecordingSignal(aeronArchive);

            try
            {
                offer(publication, messageCount, messageCount);

                final CountersReader counters = aeron.countersReader();
                final int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());

                consume(subscription, messageCount, messageCount);

                stopTwo = publication.position();
                Tests.awaitPosition(counters, counterId, stopTwo);
            }
            finally
            {
                aeronArchive.stopRecording(subscriptionIdTwo);
                pollForRecordingSignal(aeronArchive);
            }
        }

        replay(messageCount, stopTwo, recordingId);

        final InOrder inOrder = Mockito.inOrder(mockRecordingSignalConsumer);
        inOrder.verify(mockRecordingSignalConsumer).onSignal(
            eq(controlSessionId), anyLong(), eq(recordingId), eq(subscriptionIdOne), eq(0L), eq(START));
        inOrder.verify(mockRecordingSignalConsumer).onSignal(
            eq(controlSessionId), anyLong(), eq(recordingId), eq(subscriptionIdOne), eq(stopOne), eq(STOP));
        inOrder.verify(mockRecordingSignalConsumer).onSignal(
            eq(controlSessionId), anyLong(), eq(recordingId), eq(subscriptionIdTwo), eq(stopOne), eq(EXTEND));
        inOrder.verify(mockRecordingSignalConsumer).onSignal(
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

    void pollForRecordingSignal(final AeronArchive aeronArchive)
    {
        while (0 == aeronArchive.pollForRecordingSignals())
        {
            Tests.yield();
        }
    }
}
