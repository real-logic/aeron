/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveProxy;
import io.aeron.archive.client.ControlResponseAdapter;
import io.aeron.archive.client.RecordingEventsAdapter;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.archive.TestUtil.awaitConnectedReply;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static org.agrona.BufferUtil.allocateDirectAligned;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@RunWith(value = Parameterized.class)
public class ArchiveTest
{
    @Parameterized.Parameter
    public ThreadingMode threadingMode;

    @Parameterized.Parameter(value = 1)
    public ArchiveThreadingMode archiveThreadingMode;
    private long controlSessionId;

    @Parameterized.Parameters(name = "threading modes: driver={0} archive={1}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(
            new Object[][]
            {
                { ThreadingMode.INVOKER, ArchiveThreadingMode.SHARED },
                { ThreadingMode.SHARED, ArchiveThreadingMode.SHARED },
                { ThreadingMode.DEDICATED, ArchiveThreadingMode.DEDICATED },
            });
    }

    private static final String CONTROL_URI = "aeron:udp?endpoint=127.0.0.1:54327";
    private static final int CONTROL_STREAM_ID = 100;
    private static final String REPLAY_URI = "aeron:ipc";
    private static final int MESSAGE_COUNT = 5000;
    private static final int SYNC_LEVEL = 0;
    private static final int PUBLISH_STREAM_ID = 1;
    private static final int MAX_FRAGMENT_SIZE = 1024;
    private static final int REPLAY_STREAM_ID = 101;

    private final UnsafeBuffer buffer = new UnsafeBuffer(allocateDirectAligned(4096, FrameDescriptor.FRAME_ALIGNMENT));
    private final Random rnd = new Random();
    private final long seed = System.nanoTime();

    @Rule
    public final TestWatcher testWatcher = TestUtil.newWatcher(this.getClass(), seed);

    private String publishUri;
    private Aeron publishingClient;
    private Archive archive;
    private MediaDriver driver;
    private long recordingId;
    private long remaining;
    private int messageCount;
    private int[] messageLengths;
    private long totalDataLength;
    private long totalRecordingLength;
    private volatile long recorded;
    private long requestedStartPosition;
    private volatile long stopPosition = -1;
    private Throwable trackerError;

    private Subscription controlResponse;
    private long correlationId;
    private long startPosition;
    private int requestedInitialTermId;

    @Before
    public void before()
    {
        rnd.setSeed(seed);
        requestedInitialTermId = rnd.nextInt(1234);

        final int termLength = 1 << (16 + rnd.nextInt(10)); // 1M to 8M
        final int mtu = 1 << (10 + rnd.nextInt(3)); // 1024 to 8096
        final int requestedStartTermOffset = BitUtil.align(rnd.nextInt(termLength), FrameDescriptor.FRAME_ALIGNMENT);
        final int requestedStartTermId = requestedInitialTermId + rnd.nextInt(1000);

        final ChannelUriStringBuilder channelUriStringBuilder = new ChannelUriStringBuilder()
            .endpoint("127.0.0.1:54325")
            .termLength(termLength)
            .mtu(mtu)
            .media("udp");

        channelUriStringBuilder
            .initialTermId(requestedInitialTermId)
            .termId(requestedStartTermId)
            .termOffset(requestedStartTermOffset);

        publishUri = channelUriStringBuilder.build();

        requestedStartPosition = ((requestedStartTermId - requestedInitialTermId) * (long)termLength) +
            requestedStartTermOffset;

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .termBufferSparseFile(true)
            .threadingMode(threadingMode)
            .spiesSimulateConnection(true)
            .errorHandler(Throwable::printStackTrace)
            .dirDeleteOnStart(true);

        driver = MediaDriver.launch(driverCtx);

        final Archive.Context archiverCtx = new Archive.Context()
            .fileSyncLevel(SYNC_LEVEL)
            .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
            .archiveDir(TestUtil.makeTempDir())
            .segmentFileLength(termLength << rnd.nextInt(4))
            .threadingMode(archiveThreadingMode)
            .errorCounter(driverCtx.systemCounters().get(SystemCounterDescriptor.ERRORS))
            .errorHandler(driverCtx.errorHandler());

        archive = Archive.launch(archiverCtx);
        publishingClient = Aeron.connect();
    }

    @After
    public void after()
    {
        CloseHelper.close(publishingClient);
        CloseHelper.close(archive);
        CloseHelper.close(driver);

        archive.context().deleteArchiveDirectory();
        driver.context().deleteAeronDirectory();
    }

    @Test(timeout = 10000)
    public void recordAndReplayExclusivePublication() throws IOException
    {
        final String controlChannel = archive.context().controlChannel();
        final int controlStreamId = archive.context().controlStreamId();

        final String recordingChannel = archive.context().recordingEventsChannel();
        final int recordingStreamId = archive.context().recordingEventsStreamId();

        try (Publication controlPublication = publishingClient.addPublication(controlChannel, controlStreamId);
            Subscription recordingEvents = publishingClient.addSubscription(recordingChannel, recordingStreamId))
        {
            final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

            prePublicationActionsAndVerifications(archiveProxy, controlPublication, recordingEvents);

            final ExclusivePublication recordedPublication =
                publishingClient.addExclusivePublication(publishUri, PUBLISH_STREAM_ID);

            TestUtil.await(recordedPublication::isConnected);

            final int sessionId = recordedPublication.sessionId();
            final int termBufferLength = recordedPublication.termBufferLength();
            final int initialTermId = recordedPublication.initialTermId();
            final int maxPayloadLength = recordedPublication.maxPayloadLength();
            final long startPosition = recordedPublication.position();

            assertThat(startPosition, is(requestedStartPosition));
            assertThat(recordedPublication.initialTermId(), is(requestedInitialTermId));
            preSendChecks(archiveProxy, recordingEvents, sessionId, termBufferLength, startPosition);

            final int messageCount = prepAndSendMessages(recordingEvents, recordedPublication);

            postPublicationValidations(
                archiveProxy,
                recordingEvents,
                termBufferLength,
                initialTermId,
                maxPayloadLength,
                messageCount);
        }
    }

    @Test(timeout = 10000)
    public void replayExclusivePublicationWhileRecording()
    {
        final String controlChannel = archive.context().controlChannel();
        final int controlStreamId = archive.context().controlStreamId();

        final String recordingChannel = archive.context().recordingEventsChannel();
        final int recordingStreamId = archive.context().recordingEventsStreamId();

        try (Publication controlPublication = publishingClient.addPublication(controlChannel, controlStreamId);
            Subscription recordingEvents = publishingClient.addSubscription(recordingChannel, recordingStreamId))
        {
            final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

            prePublicationActionsAndVerifications(archiveProxy, controlPublication, recordingEvents);

            final ExclusivePublication recordedPublication =
                publishingClient.addExclusivePublication(publishUri, PUBLISH_STREAM_ID);

            TestUtil.await(recordedPublication::isConnected);

            final int sessionId = recordedPublication.sessionId();
            final int termBufferLength = recordedPublication.termBufferLength();
            final int initialTermId = recordedPublication.initialTermId();
            final int maxPayloadLength = recordedPublication.maxPayloadLength();
            final long startPosition = recordedPublication.position();

            assertThat(startPosition, is(requestedStartPosition));
            assertThat(recordedPublication.initialTermId(), is(requestedInitialTermId));
            preSendChecks(archiveProxy, recordingEvents, sessionId, termBufferLength, startPosition);

            final int messageCount = MESSAGE_COUNT;
            final CountDownLatch waitForData = new CountDownLatch(2);
            prepMessagesAndListener(recordingEvents, messageCount, waitForData);
            validateActiveRecordingReplay(
                archiveProxy,
                termBufferLength,
                initialTermId,
                maxPayloadLength,
                messageCount,
                waitForData);

            publishDataToRecorded(recordedPublication, messageCount);
            await(waitForData);
        }
    }

    @Test(timeout = 10000)
    public void recordAndReplayRegularPublication() throws IOException
    {
        final String controlChannel = archive.context().controlChannel();
        final int controlStreamId = archive.context().controlStreamId();

        final String recordingChannel = archive.context().recordingEventsChannel();
        final int recordingStreamId = archive.context().recordingEventsStreamId();

        try (Publication controlPublication = publishingClient.addPublication(controlChannel, controlStreamId);
            Subscription recordingEvents = publishingClient.addSubscription(recordingChannel, recordingStreamId))
        {
            final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

            prePublicationActionsAndVerifications(archiveProxy, controlPublication, recordingEvents);

            final Publication recordedPublication = publishingClient.addPublication(publishUri, PUBLISH_STREAM_ID);
            TestUtil.await(recordedPublication::isConnected);

            final int sessionId = recordedPublication.sessionId();
            final int termBufferLength = recordedPublication.termBufferLength();
            final int initialTermId = recordedPublication.initialTermId();
            final int maxPayloadLength = recordedPublication.maxPayloadLength();
            final long startPosition = recordedPublication.position();

            preSendChecks(archiveProxy, recordingEvents, sessionId, termBufferLength, startPosition);

            final int messageCount = prepAndSendMessages(recordingEvents, recordedPublication);

            postPublicationValidations(
                archiveProxy,
                recordingEvents,
                termBufferLength,
                initialTermId,
                maxPayloadLength,
                messageCount);
        }
    }

    private void preSendChecks(
        final ArchiveProxy archiveProxy,
        final Subscription recordingEvents,
        final int sessionId,
        final int termBufferLength,
        final long startPosition)
    {
        final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
            new FailRecordingEventsListener()
            {
                public void onStart(
                    final long recordingId0,
                    final long startPosition0,
                    final int sessionId0,
                    final int streamId0,
                    final String channel,
                    final String sourceIdentity)
                {
                    recordingId = recordingId0;
                    assertThat(streamId0, is(PUBLISH_STREAM_ID));
                    assertThat(sessionId0, is(sessionId));
                    assertThat(startPosition0, is(startPosition));
                }
            },
            recordingEvents,
            1);

        TestUtil.await(() -> recordingEventsAdapter.poll() != 0);

        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);
    }

    private void postPublicationValidations(
        final ArchiveProxy archiveProxy,
        final Subscription recordingEvents,
        final int termBufferLength,
        final int initialTermId,
        final int maxPayloadLength,
        final int messageCount) throws IOException
    {
        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);

        assertNull(trackerError);

        final long requestStopCorrelationId = correlationId++;
        TestUtil.await(() -> archiveProxy.stopRecording(
            publishUri,
            PUBLISH_STREAM_ID,
            requestStopCorrelationId,
            controlSessionId));

        TestUtil.awaitOk(controlResponse, requestStopCorrelationId);

        final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
            new FailRecordingEventsListener()
            {
                public void onStop(final long rId, final long startPosition, final long stopPosition)
                {
                    assertThat(rId, is(recordingId));
                }
            },
            recordingEvents,
            1);

        TestUtil.await(() -> recordingEventsAdapter.poll() != 0);

        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);

        validateArchiveFile(messageCount, recordingId);

        validateReplay(
            archiveProxy,
            messageCount,
            initialTermId,
            maxPayloadLength,
            termBufferLength);
    }

    private void prePublicationActionsAndVerifications(
        final ArchiveProxy archiveProxy,
        final Publication controlPublication,
        final Subscription recordingEvents)
    {
        TestUtil.await(controlPublication::isConnected);
        TestUtil.await(recordingEvents::isConnected);

        controlResponse = publishingClient.addSubscription(CONTROL_URI, CONTROL_STREAM_ID);
        final long connectCorrelationId = correlationId++;
        assertTrue(archiveProxy.connect(CONTROL_URI, CONTROL_STREAM_ID, connectCorrelationId));

        TestUtil.await(controlResponse::isConnected);
        awaitConnectedReply(controlResponse, connectCorrelationId, (sessionId) -> controlSessionId = sessionId);
        verifyEmptyDescriptorList(archiveProxy);

        final long startRecordingCorrelationId = correlationId++;
        TestUtil.await(() -> archiveProxy.startRecording(
            publishUri,
            PUBLISH_STREAM_ID,
            SourceLocation.LOCAL,
            startRecordingCorrelationId,
            controlSessionId));

        TestUtil.awaitOk(controlResponse, startRecordingCorrelationId);
    }

    private void verifyEmptyDescriptorList(final ArchiveProxy client)
    {
        final long requestRecordingsCorrelationId = correlationId++;
        client.listRecordings(0, 100, requestRecordingsCorrelationId, controlSessionId);
        TestUtil.awaitResponse(controlResponse, requestRecordingsCorrelationId);
    }

    private void verifyDescriptorListOngoingArchive(
        final ArchiveProxy archiveProxy, final int publicationTermBufferLength)
    {
        final long requestRecordingsCorrelationId = correlationId++;
        archiveProxy.listRecordings(recordingId, 1, requestRecordingsCorrelationId, controlSessionId);

        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
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
                    assertThat(recordingId, is(ArchiveTest.this.recordingId));
                    assertThat(termBufferLength, is(publicationTermBufferLength));
                    assertThat(streamId, is(PUBLISH_STREAM_ID));
                    assertThat(correlationId, is(requestRecordingsCorrelationId));
                    assertThat(originalChannel, is(publishUri));
                }

            },
            controlResponse,
            1
        );

        TestUtil.await(() -> controlResponseAdapter.poll() != 0);
    }

    private int prepAndSendMessages(final Subscription recordingEvents, final Publication publication)
    {
        final int messageCount = MESSAGE_COUNT;
        final CountDownLatch waitForData = new CountDownLatch(1);
        prepMessagesAndListener(recordingEvents, messageCount, waitForData);
        publishDataToRecorded(publication, messageCount);
        await(waitForData);

        return messageCount;
    }

    private int prepAndSendMessages(final Subscription recordingEvents, final ExclusivePublication publication)
    {
        final int messageCount = MESSAGE_COUNT;
        final CountDownLatch waitForData = new CountDownLatch(1);
        prepMessagesAndListener(recordingEvents, messageCount, waitForData);
        publishDataToRecorded(publication, messageCount);
        await(waitForData);

        return messageCount;
    }

    private void await(final CountDownLatch waitForData)
    {
        try
        {
            waitForData.await();
        }
        catch (final InterruptedException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private void prepMessagesAndListener(
        final Subscription recordingEvents, final int messageCount, final CountDownLatch waitForData)
    {
        messageLengths = new int[messageCount];
        for (int i = 0; i < messageCount; i++)
        {
            final int messageLength = 64 + rnd.nextInt(MAX_FRAGMENT_SIZE - 64) - HEADER_LENGTH;
            messageLengths[i] = messageLength + HEADER_LENGTH;
            totalDataLength += BitUtil.align(messageLengths[i], FrameDescriptor.FRAME_ALIGNMENT);
        }

        trackRecordingProgress(recordingEvents, waitForData);
    }

    private void publishDataToRecorded(final Publication publication, final int messageCount)
    {
        startPosition = publication.position();

        buffer.setMemory(0, 1024, (byte)'z');
        buffer.putStringAscii(32, "TEST");

        for (int i = 0; i < messageCount; i++)
        {
            final int dataLength = messageLengths[i] - HEADER_LENGTH;
            buffer.putInt(0, i);
            TestUtil.offer(publication, buffer, dataLength);
        }

        final long position = publication.position();
        totalRecordingLength = position - startPosition;
        stopPosition = position;
    }

    private void publishDataToRecorded(final ExclusivePublication publication, final int messageCount)
    {
        startPosition = publication.position();

        buffer.setMemory(0, 1024, (byte)'z');
        buffer.putStringAscii(32, "TEST");

        for (int i = 0; i < messageCount; i++)
        {
            final int dataLength = messageLengths[i] - HEADER_LENGTH;
            buffer.putInt(0, i);
            TestUtil.offer(publication, buffer, dataLength);
        }

        final long position = publication.position();
        totalRecordingLength = position - startPosition;
        stopPosition = position;
    }

    private void validateReplay(
        final ArchiveProxy archiveProxy,
        final int messageCount,
        final int initialTermId,
        final int maxPayloadLength,
        final int termBufferLength)
    {
        try (Subscription replay = publishingClient.addSubscription(REPLAY_URI, REPLAY_STREAM_ID))
        {
            final long replayCorrelationId = correlationId++;

            TestUtil.await(() -> archiveProxy.replay(
                recordingId,
                startPosition,
                totalRecordingLength,
                REPLAY_URI,
                REPLAY_STREAM_ID,
                replayCorrelationId,
                controlSessionId));

            TestUtil.awaitOk(controlResponse, replayCorrelationId);
            TestUtil.await(replay::isConnected);
            final Image image = replay.images().get(0);
            assertThat(image.initialTermId(), is(initialTermId));
            assertThat(image.mtuLength(), is(maxPayloadLength + HEADER_LENGTH));
            assertThat(image.termBufferLength(), is(termBufferLength));
            assertThat(image.position(), is(startPosition));

            this.messageCount = 0;
            remaining = totalDataLength;

            while (remaining > 0)
            {
                TestUtil.poll(replay, this::validateFragment2);
            }

            assertThat(this.messageCount, is(messageCount));
            assertThat(remaining, is(0L));
        }
    }

    private void validateArchiveFile(final int messageCount, final long recordingId) throws IOException
    {
        remaining = totalDataLength;
        final File archiveDir = archive.context().archiveDir();

        try (Catalog catalog = new Catalog(archiveDir, null, 0, System::currentTimeMillis))
        {
            final RecordingFragmentReader archiveDataFileReader = new RecordingFragmentReader(
                catalog,
                catalog.recordingSummary(recordingId, new RecordingSummary()),
                archiveDir,
                AeronArchive.NULL_POSITION,
                RecordingFragmentReader.NULL_LENGTH,
                null);

            this.messageCount = 0;
            remaining = totalDataLength;
            while (!archiveDataFileReader.isDone())
            {
                archiveDataFileReader.controlledPoll(this::validateFragment1, messageCount);
            }

            archiveDataFileReader.close();

            assertThat(remaining, is(0L));
            assertThat(this.messageCount, is(messageCount));
        }
    }

    @SuppressWarnings("SameReturnValue")
    private boolean validateFragment1(final UnsafeBuffer buffer, final int offset, final int length)
    {
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(buffer.addressOffset() + offset - HEADER_LENGTH, HEADER_LENGTH);
        if (headerFlyweight.headerType() == HDR_TYPE_PAD)
        {
            return true;
        }

        final int expectedLength = messageLengths[messageCount] - HEADER_LENGTH;
        assertThat("on fragment[" + messageCount + "]", length, is(expectedLength));
        assertThat(buffer.getInt(offset), is(messageCount));
        assertThat(buffer.getByte(offset + 4), is((byte)'z'));

        remaining -= BitUtil.align(messageLengths[messageCount], FrameDescriptor.FRAME_ALIGNMENT);
        messageCount++;

        return true;
    }

    private void validateFragment2(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        @SuppressWarnings("unused") final Header header)
    {
        assertThat(length, is(messageLengths[messageCount] - HEADER_LENGTH));
        assertThat(buffer.getInt(offset), is(messageCount));
        assertThat(buffer.getByte(offset + 4), is((byte)'z'));
        remaining -= BitUtil.align(messageLengths[messageCount], FrameDescriptor.FRAME_ALIGNMENT);
        messageCount++;
    }

    private void trackRecordingProgress(final Subscription recordingEvents, final CountDownLatch waitForData)
    {
        final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
            new FailRecordingEventsListener()
            {
                public void onProgress(
                    final long recordingId0,
                    final long startPosition,
                    final long position)
                {
                    assertThat(recordingId0, is(recordingId));
                    recorded = position - startPosition;
                }
            },
            recordingEvents,
            1);

        final Thread thread = new Thread(
            () ->
            {
                try
                {
                    recorded = 0;

                    while (stopPosition == -1 || recorded < totalRecordingLength)
                    {
                        if (recordingEventsAdapter.poll() == 0)
                        {
                            LockSupport.parkNanos(1);
                        }
                    }
                }
                catch (final Throwable throwable)
                {
                    throwable.printStackTrace();
                    trackerError = throwable;
                }

                waitForData.countDown();
            });

        thread.setDaemon(true);
        thread.setName("recording-progress-tracker");
        thread.start();
    }

    private void validateActiveRecordingReplay(
        final ArchiveProxy archiveProxy,
        final int termBufferLength,
        final int initialTermId,
        final int maxPayloadLength,
        final int messageCount,
        final CountDownLatch waitForData)
    {
        final Thread thread = new Thread(
            () ->
            {
                do
                {
                    LockSupport.parkNanos(1000000);
                }
                while (recorded == 0);

                try (Subscription replay = publishingClient.addSubscription(REPLAY_URI, REPLAY_STREAM_ID))
                {
                    final long replayCorrelationId = correlationId++;

                    TestUtil.await(() -> archiveProxy.replay(
                        recordingId,
                        startPosition,
                        Long.MAX_VALUE,
                        REPLAY_URI,
                        REPLAY_STREAM_ID,
                        replayCorrelationId,
                        controlSessionId));

                    TestUtil.awaitOk(controlResponse, replayCorrelationId);

                    TestUtil.await(replay::isConnected);

                    final Image image = replay.images().get(0);
                    assertThat(image.initialTermId(), is(initialTermId));
                    assertThat(image.mtuLength(), is(maxPayloadLength + HEADER_LENGTH));
                    assertThat(image.termBufferLength(), is(termBufferLength));
                    assertThat(image.position(), is(startPosition));

                    this.messageCount = 0;
                    remaining = totalDataLength;

                    while (this.messageCount < messageCount)
                    {
                        TestUtil.poll(replay, this::validateFragment2);
                    }

                    waitForData.countDown();
                }
            });

        thread.setName("replay-consumer");
        thread.setDaemon(true);
        thread.start();
    }
}
