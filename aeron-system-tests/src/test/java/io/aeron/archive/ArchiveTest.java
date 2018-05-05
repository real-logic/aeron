/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import org.agrona.*;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static io.aeron.archive.TestUtil.awaitConnectedReply;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
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

    private static final long MAX_CATALOG_ENTRIES = 1024;
    private static final String CONTROL_RESPONSE_URI = CommonContext.IPC_CHANNEL;
    private static final int CONTROL_RESPONSE_STREAM_ID = 100;
    private static final String REPLAY_URI = CommonContext.IPC_CHANNEL;
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
    private Aeron client;
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
    private volatile long stopPosition = NULL_POSITION;
    private Throwable trackerError;

    private Subscription controlResponse;
    private long correlationId;
    private long startPosition;
    private int requestedInitialTermId;

    private Thread replayConsumer = null;
    private Thread progressTracker = null;

    @Before
    public void before()
    {
        rnd.setSeed(seed);
        requestedInitialTermId = rnd.nextInt(1234);

        final int termLength = 1 << (16 + rnd.nextInt(10)); // 1M to 8M
        final int mtu = 1 << (10 + rnd.nextInt(3)); // 1024 to 8096
        final int requestedStartTermOffset = BitUtil.align(rnd.nextInt(termLength), FrameDescriptor.FRAME_ALIGNMENT);
        final int requestedStartTermId = requestedInitialTermId + rnd.nextInt(1000);
        final int segmentFileLength = termLength << rnd.nextInt(4);

        publishUri = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("localhost:54325")
            .termLength(termLength)
            .mtu(mtu)
            .initialTermId(requestedInitialTermId)
            .termId(requestedStartTermId)
            .termOffset(requestedStartTermOffset)
            .build();

        requestedStartPosition =
            ((requestedStartTermId - requestedInitialTermId) * (long)termLength) + requestedStartTermOffset;

        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .termBufferSparseFile(true)
                .threadingMode(threadingMode)
                .sharedIdleStrategy(new YieldingIdleStrategy())
                .spiesSimulateConnection(true)
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true));

        archive = Archive.launch(
            new Archive.Context()
                .maxCatalogEntries(MAX_CATALOG_ENTRIES)
                .fileSyncLevel(SYNC_LEVEL)
                .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
                .deleteArchiveOnStart(true)
                .archiveDir(new File(IoUtil.tmpDirName(), "archive-test"))
                .segmentFileLength(segmentFileLength)
                .threadingMode(archiveThreadingMode)
                .idleStrategySupplier(YieldingIdleStrategy::new)
                .errorCounter(driver.context().systemCounters().get(SystemCounterDescriptor.ERRORS))
                .errorHandler(driver.context().errorHandler()));

        client = Aeron.connect(new Aeron.Context().errorHandler(Throwable::printStackTrace));

        recorded = 0;
    }

    @After
    public void after()
    {
        if (null != replayConsumer)
        {
            replayConsumer.interrupt();
        }

        if (null != progressTracker)
        {
            progressTracker.interrupt();
        }

        CloseHelper.close(client);
        CloseHelper.close(archive);
        CloseHelper.close(driver);

        archive.context().deleteArchiveDirectory();
        driver.context().deleteAeronDirectory();
    }

    @Test(timeout = 10_000)
    public void recordAndReplayExclusivePublication()
    {
        final String controlChannel = archive.context().localControlChannel();
        final int controlStreamId = archive.context().localControlStreamId();

        final String recordingChannel = archive.context().recordingEventsChannel();
        final int recordingStreamId = archive.context().recordingEventsStreamId();

        final Publication controlPublication = client.addPublication(controlChannel, controlStreamId);
        final Subscription recordingEvents = client.addSubscription(recordingChannel, recordingStreamId);
        final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

        prePublicationActionsAndVerifications(archiveProxy, controlPublication, recordingEvents);

        final ExclusivePublication recordedPublication =
            client.addExclusivePublication(publishUri, PUBLISH_STREAM_ID);

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

    @Test(timeout = 10_000)
    public void replayExclusivePublicationWhileRecording()
    {
        final String controlChannel = archive.context().localControlChannel();
        final int controlStreamId = archive.context().localControlStreamId();

        final String recordingChannel = archive.context().recordingEventsChannel();
        final int recordingStreamId = archive.context().recordingEventsStreamId();

        final Publication controlPublication = client.addPublication(controlChannel, controlStreamId);
        final Subscription recordingEvents = client.addSubscription(recordingChannel, recordingStreamId);
        final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

        prePublicationActionsAndVerifications(archiveProxy, controlPublication, recordingEvents);

        final ExclusivePublication recordedPublication =
            client.addExclusivePublication(publishUri, PUBLISH_STREAM_ID);

        final int sessionId = recordedPublication.sessionId();
        final int termBufferLength = recordedPublication.termBufferLength();
        final int initialTermId = recordedPublication.initialTermId();
        final int maxPayloadLength = recordedPublication.maxPayloadLength();
        final long startPosition = recordedPublication.position();

        assertThat(startPosition, is(requestedStartPosition));
        assertThat(recordedPublication.initialTermId(), is(requestedInitialTermId));
        preSendChecks(archiveProxy, recordingEvents, sessionId, termBufferLength, startPosition);

        final int messageCount = MESSAGE_COUNT;
        final CountDownLatch streamConsumed = new CountDownLatch(2);

        prepMessagesAndListener(recordingEvents, messageCount, streamConsumed);
        replayConsumer = validateActiveRecordingReplay(
            archiveProxy,
            termBufferLength,
            initialTermId,
            maxPayloadLength,
            messageCount,
            streamConsumed);

        publishDataToBeRecorded(recordedPublication, messageCount);
        await(streamConsumed);
    }

    @Test(timeout = 10_000)
    public void recordAndReplayRegularPublication()
    {
        final String controlChannel = archive.context().localControlChannel();
        final int controlStreamId = archive.context().localControlStreamId();

        final String recordingChannel = archive.context().recordingEventsChannel();
        final int recordingStreamId = archive.context().recordingEventsStreamId();

        final Publication controlPublication = client.addPublication(controlChannel, controlStreamId);
        final Subscription recordingEvents = client.addSubscription(recordingChannel, recordingStreamId);
        final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

        prePublicationActionsAndVerifications(archiveProxy, controlPublication, recordingEvents);

        final Publication recordedPublication = client.addPublication(publishUri, PUBLISH_STREAM_ID);

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

    private void preSendChecks(
        final ArchiveProxy archiveProxy,
        final Subscription recordingEvents,
        final int sessionId,
        final int termBufferLength,
        final long startPosition)
    {
        final MutableBoolean recordingStarted = new MutableBoolean();
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
                    recordingStarted.set(true);
                }
            },
            recordingEvents,
            1);

        while (!recordingStarted.get())
        {
            if (recordingEventsAdapter.poll() == 0)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }
        }

        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);
    }

    private void postPublicationValidations(
        final ArchiveProxy archiveProxy,
        final Subscription recordingEvents,
        final int termBufferLength,
        final int initialTermId,
        final int maxPayloadLength,
        final int messageCount)
    {
        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);

        assertNull(trackerError);

        final long requestStopCorrelationId = correlationId++;
        if (!archiveProxy.stopRecording(publishUri, PUBLISH_STREAM_ID, requestStopCorrelationId, controlSessionId))
        {
            throw new IllegalStateException("Failed to stop recording");
        }

        TestUtil.awaitOk(controlResponse, requestStopCorrelationId);

        final MutableBoolean recordingStopped = new MutableBoolean();
        final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
            new FailRecordingEventsListener()
            {
                public void onStop(final long id, final long startPosition, final long stopPosition)
                {
                    assertThat(id, is(recordingId));
                    recordingStopped.set(true);
                }
            },
            recordingEvents,
            1);

        while (!recordingStopped.get())
        {
            if (recordingEventsAdapter.poll() == 0)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }
        }

        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);
        validateArchiveFile(messageCount, recordingId);
        validateReplay(archiveProxy, messageCount, initialTermId, maxPayloadLength, termBufferLength);
    }

    private void prePublicationActionsAndVerifications(
        final ArchiveProxy archiveProxy,
        final Publication controlPublication,
        final Subscription recordingEvents)
    {
        TestUtil.await(controlPublication::isConnected);
        TestUtil.await(recordingEvents::isConnected);

        controlResponse = client.addSubscription(CONTROL_RESPONSE_URI, CONTROL_RESPONSE_STREAM_ID);
        final long connectCorrelationId = correlationId++;
        assertTrue(archiveProxy.connect(CONTROL_RESPONSE_URI, CONTROL_RESPONSE_STREAM_ID, connectCorrelationId));

        TestUtil.await(controlResponse::isConnected);
        awaitConnectedReply(controlResponse, connectCorrelationId, (sessionId) -> controlSessionId = sessionId);
        verifyEmptyDescriptorList(archiveProxy);

        final long startRecordingCorrelationId = correlationId++;
        if (!archiveProxy.startRecording(
            publishUri,
            PUBLISH_STREAM_ID,
            SourceLocation.LOCAL,
            startRecordingCorrelationId,
            controlSessionId))
        {
            throw new IllegalStateException("Failed to start recording");
        }

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
        archiveProxy.listRecording(recordingId, requestRecordingsCorrelationId, controlSessionId);

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

        while (controlResponseAdapter.poll() == 0)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }

    private int prepAndSendMessages(final Subscription recordingEvents, final Publication publication)
    {
        final int messageCount = MESSAGE_COUNT;
        final CountDownLatch waitForData = new CountDownLatch(1);
        prepMessagesAndListener(recordingEvents, messageCount, waitForData);
        publishDataToBeRecorded(publication, messageCount);
        await(waitForData);

        return messageCount;
    }

    private int prepAndSendMessages(final Subscription recordingEvents, final ExclusivePublication publication)
    {
        final int messageCount = MESSAGE_COUNT;
        final CountDownLatch waitForData = new CountDownLatch(1);
        prepMessagesAndListener(recordingEvents, messageCount, waitForData);
        publishDataToBeRecorded(publication, messageCount);
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
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void prepMessagesAndListener(
        final Subscription recordingEvents, final int messageCount, final CountDownLatch streamConsumedLatch)
    {
        messageLengths = new int[messageCount];
        for (int i = 0; i < messageCount; i++)
        {
            final int messageLength = 64 + rnd.nextInt(MAX_FRAGMENT_SIZE - 64) - HEADER_LENGTH;
            messageLengths[i] = messageLength + HEADER_LENGTH;
            totalDataLength += BitUtil.align(messageLengths[i], FrameDescriptor.FRAME_ALIGNMENT);
        }

        progressTracker = trackRecordingProgress(recordingEvents, streamConsumedLatch);
    }

    private void publishDataToBeRecorded(final Publication publication, final int messageCount)
    {
        startPosition = publication.position();

        buffer.setMemory(0, 1024, (byte)'z');
        buffer.putStringAscii(32, "TEST");

        for (int i = 0; i < messageCount; i++)
        {
            final int dataLength = messageLengths[i] - HEADER_LENGTH;
            buffer.putInt(0, i);

            while (true)
            {
                final long result = publication.offer(buffer, 0, dataLength);
                if (result > 0)
                {
                    break;
                }

                if (result == Publication.CLOSED || result == Publication.NOT_CONNECTED)
                {
                    throw new IllegalStateException("Publication not connected: result=" + result);
                }

                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }
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
        try (Subscription replay = client.addSubscription(REPLAY_URI, REPLAY_STREAM_ID))
        {
            final long replayCorrelationId = correlationId++;

            if (!archiveProxy.replay(
                recordingId,
                startPosition,
                totalRecordingLength,
                REPLAY_URI,
                REPLAY_STREAM_ID,
                replayCorrelationId,
                controlSessionId))
            {
                throw new IllegalStateException("Failed to replay");
            }

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
                final int fragments = replay.poll(this::validateFragment, 10);
                if (0 == fragments)
                {
                    SystemTest.checkInterruptedStatus();
                    Thread.yield();
                }
            }

            assertThat(this.messageCount, is(messageCount));
            assertThat(remaining, is(0L));
        }
    }

    private void validateArchiveFile(final int messageCount, final long recordingId)
    {
        final File archiveDir = archive.context().archiveDir();
        final Catalog catalog = archive.context().catalog();
        remaining = totalDataLength;
        this.messageCount = 0;

        while (catalog.stopPosition(recordingId) != stopPosition)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        try (RecordingFragmentReader archiveDataFileReader = new RecordingFragmentReader(
            catalog,
            catalog.recordingSummary(recordingId, new RecordingSummary()),
            archiveDir,
            NULL_POSITION,
            AeronArchive.NULL_LENGTH,
            null))
        {
            while (!archiveDataFileReader.isDone())
            {
                archiveDataFileReader.controlledPoll(this::validateReplayFragment, messageCount);
                SystemTest.checkInterruptedStatus();
            }
        }

        assertThat(remaining, is(0L));
        assertThat(this.messageCount, is(messageCount));
    }

    @SuppressWarnings("unused")
    private boolean validateReplayFragment(
        final UnsafeBuffer buffer,
        final int offset,
        final int length,
        final int frameType,
        final byte flags,
        final long reservedValue)
    {
        if (!FrameDescriptor.isPaddingFrame(buffer, offset - HEADER_LENGTH))
        {
            final int expectedLength = messageLengths[messageCount] - HEADER_LENGTH;
            if (length != expectedLength)
            {
                fail("Message length=" + length + " expected=" + expectedLength + " messageCount=" + messageCount);
            }

            assertThat(buffer.getInt(offset), is(messageCount));
            assertThat(buffer.getByte(offset + 4), is((byte)'z'));

            remaining -= BitUtil.align(messageLengths[messageCount], FrameDescriptor.FRAME_ALIGNMENT);
            messageCount++;
        }

        return true;
    }

    @SuppressWarnings("unused")
    private void validateFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        assertThat(length, is(messageLengths[messageCount] - HEADER_LENGTH));
        assertThat(buffer.getInt(offset), is(messageCount));
        assertThat(buffer.getByte(offset + 4), is((byte)'z'));

        remaining -= BitUtil.align(messageLengths[messageCount], FrameDescriptor.FRAME_ALIGNMENT);
        messageCount++;
    }

    private Thread trackRecordingProgress(final Subscription recordingEvents, final CountDownLatch streamConsumed)
    {
        final RecordingEventsAdapter recordingEventsAdapter = new RecordingEventsAdapter(
            new FailRecordingEventsListener()
            {
                public void onProgress(final long recordingId0, final long startPosition, final long position)
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

                    while (stopPosition == NULL_POSITION || recorded < totalRecordingLength)
                    {
                        if (recordingEventsAdapter.poll() == 0)
                        {
                            SystemTest.checkInterruptedStatus();
                            SystemTest.sleep(1);
                        }
                    }
                }
                catch (final Throwable throwable)
                {
                    throwable.printStackTrace();
                    trackerError = throwable;
                }

                streamConsumed.countDown();
            });

        thread.setDaemon(true);
        thread.setName("recording-progress-tracker");
        thread.start();

        return thread;
    }

    private Thread validateActiveRecordingReplay(
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
                while (0 == recorded)
                {
                    SystemTest.sleep(1);
                }

                try (Subscription replay = client.addSubscription(REPLAY_URI, REPLAY_STREAM_ID))
                {
                    final long replayCorrelationId = correlationId++;

                    if (!archiveProxy.replay(
                        recordingId,
                        startPosition,
                        Long.MAX_VALUE,
                        REPLAY_URI,
                        REPLAY_STREAM_ID,
                        replayCorrelationId,
                        controlSessionId))
                    {
                        throw new IllegalStateException("Failed to start replay");
                    }

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
                        final int fragments = replay.poll(this::validateFragment, 10);
                        if (0 == fragments)
                        {
                            SystemTest.checkInterruptedStatus();
                            Thread.yield();
                        }
                    }

                    waitForData.countDown();
                }
            });

        thread.setName("replay-consumer");
        thread.setDaemon(true);
        thread.start();

        return thread;
    }
}
