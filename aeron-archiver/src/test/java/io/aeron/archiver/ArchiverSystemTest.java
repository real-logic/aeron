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
package io.aeron.archiver;

import io.aeron.*;
import io.aeron.archiver.client.ArchiveProxy;
import io.aeron.archiver.client.ControlResponsePoller;
import io.aeron.archiver.client.RecordingEventsPoller;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.CommonContext.SPY_PREFIX;
import static io.aeron.archiver.TestUtil.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class ArchiverSystemTest
{
    private static final double MEGABYTE = 1024.0d * 1024.0d;

    private static final String CONTROL_URI = "aeron:udp?endpoint=127.0.0.1:54327";
    private static final int CONTROL_STREAM_ID = 100;
    private static final String REPLAY_URI = "aeron:ipc";
    private String publishUri;
    private static final int PUBLISH_STREAM_ID = 1;
    private static final int MAX_FRAGMENT_SIZE = 1024;
    private static final int REPLAY_STREAM_ID = 101;

    private final MediaDriver.Context driverCtx = new MediaDriver.Context();
    private final Archiver.Context archiverCtx = new Archiver.Context();
    private Aeron publishingClient;
    private Archiver archiver;
    private MediaDriver driver;
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private File archiveDir;
    private long recordingId;
    private long remaining;
    private int fragmentCount;
    private int[] fragmentLength;
    private long totalDataLength;
    private long totalRecordingLength;
    private long recorded;
    private long requestedJoinPosition;
    private volatile long endPosition = -1;
    private Throwable trackerError;
    private final Random rnd = new Random();
    private long seed;

    @Rule
    public TestWatcher testWatcher = new TestWatcher()
    {
        protected void failed(final Throwable t, final Description description)
        {
            System.err.println(ArchiverSystemTest.class.getName() + " failed with random seed: " + seed);
        }
    };

    private Subscription controlResponse;
    private long correlationId;
    private long joinPosition;
    private int requestedInitialTermId;

    @Before
    public void before() throws Exception
    {
        seed = System.nanoTime();
        rnd.setSeed(seed);

        requestedInitialTermId = rnd.nextInt(1234);
        final int termLength = 1 << (16 + rnd.nextInt(10)); // 1M to 8M
        final int mtu = 1 << (10 + rnd.nextInt(3)); // 1024 to 8096
        final int termOffset = BitUtil.align(rnd.nextInt(termLength), FrameDescriptor.FRAME_ALIGNMENT);
        final int termId = requestedInitialTermId + rnd.nextInt(1000);

        final ChannelUriStringBuilder channelUriStringBuilder = new ChannelUriStringBuilder()
            .endpoint("127.0.0.1:54325")
            .termLength(termLength)
            .mtu(mtu)
            .media("udp");

        channelUriStringBuilder
            .initialTermId(requestedInitialTermId)
            .termId(termId)
            .termOffset(termOffset);

        publishUri = channelUriStringBuilder.build();

        requestedJoinPosition = ((termId - requestedInitialTermId) * (long)termLength) + termOffset;

        driverCtx
            .termBufferSparseFile(true)
            .threadingMode(driverThreadingMode())
            .errorHandler(Throwable::printStackTrace)
            .dirsDeleteOnStart(true)
            .useConcurrentCounterManager(true);

        driver = MediaDriver.launch(driverCtx);

        final int segmentFileLength = termLength << rnd.nextInt(4);
        archiveDir = TestUtil.makeTempDir();
        archiverCtx
            .fileSyncLevel(0)
            .mediaDriverAgentInvoker(driver.sharedAgentInvoker())
            .archiveDir(archiveDir)
            .segmentFileLength(segmentFileLength)
            .threadingMode(archiverThreadingMode())
            .countersManager(driverCtx.countersManager())
            .errorHandler(driverCtx.errorHandler());

        archiver = Archiver.launch(archiverCtx);

        println("Archiver started, dir: " + archiverCtx.archiveDir().getAbsolutePath());
        publishingClient = Aeron.connect();
    }

    ArchiverThreadingMode archiverThreadingMode()
    {
        return ArchiverThreadingMode.SHARED;
    }

    ThreadingMode driverThreadingMode()
    {
        return ThreadingMode.INVOKER;
    }

    @After
    public void after() throws Exception
    {
        CloseHelper.close(publishingClient);
        CloseHelper.close(archiver);
        CloseHelper.close(driver);

        if (null != archiveDir)
        {
            IoUtil.delete(archiveDir, false);
        }

        driverCtx.deleteAeronDirectory();
    }

    @Test(timeout = 10000)
    public void recordAndReplayExclusivePublication() throws IOException, InterruptedException
    {
        try (Publication controlPublication = publishingClient.addPublication(
                archiverCtx.controlChannel(), archiverCtx.controlStreamId());
             Subscription recordingEvents = publishingClient.addSubscription(
                archiverCtx.recordingEventsChannel(), archiverCtx.recordingEventsStreamId()))
        {
            final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

            prePublicationActionsAndVerifications(publishingClient, archiveProxy, controlPublication, recordingEvents);

            final ExclusivePublication recordedPublication =
                publishingClient.addExclusivePublication(publishUri, PUBLISH_STREAM_ID);

            waitFor(recordedPublication::isConnected);

            final int sessionId = recordedPublication.sessionId();
            final int streamId = recordedPublication.streamId();
            final int termBufferLength = recordedPublication.termBufferLength();
            final int initialTermId = recordedPublication.initialTermId();
            final int maxPayloadLength = recordedPublication.maxPayloadLength();
            final long joinPosition = recordedPublication.position();

            assertThat(joinPosition, is(requestedJoinPosition));
            assertThat(recordedPublication.initialTermId(), is(requestedInitialTermId));
            preSendChecks(archiveProxy, recordingEvents, sessionId, termBufferLength, joinPosition);

            final int messageCount = prepAndSendMessages(recordingEvents, recordedPublication);

            postPublicationValidations(
                archiveProxy,
                recordingEvents,
                sessionId,
                streamId,
                termBufferLength,
                initialTermId,
                maxPayloadLength,
                messageCount);
        }
    }

    @Test(timeout = 10000)
    public void recordAndReplayRegularPublication() throws IOException, InterruptedException
    {
        try (Publication controlPublication = publishingClient.addPublication(
                archiverCtx.controlChannel(), archiverCtx.controlStreamId());
             Subscription recordingEvents = publishingClient.addSubscription(
                archiverCtx.recordingEventsChannel(), archiverCtx.recordingEventsStreamId()))
        {
            final ArchiveProxy archiveProxy = new ArchiveProxy(controlPublication);

            prePublicationActionsAndVerifications(publishingClient, archiveProxy, controlPublication, recordingEvents);

            final Publication recordedPublication = publishingClient.addPublication(publishUri, PUBLISH_STREAM_ID);
            awaitPublicationIsConnected(recordedPublication);

            final int sessionId = recordedPublication.sessionId();
            final int streamId = recordedPublication.streamId();
            final int termBufferLength = recordedPublication.termBufferLength();
            final int initialTermId = recordedPublication.initialTermId();
            final int maxPayloadLength = recordedPublication.maxPayloadLength();
            final long joinPosition = recordedPublication.position();

            preSendChecks(archiveProxy, recordingEvents, sessionId, termBufferLength, joinPosition);

            final int messageCount = prepAndSendMessages(recordingEvents, recordedPublication);

            postPublicationValidations(
                archiveProxy,
                recordingEvents,
                sessionId,
                streamId,
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
        final long joinPosition)
    {
        final RecordingEventsPoller recordingEventsPoller = new RecordingEventsPoller(
            new FailRecordingEventsListener()
            {
                public void onStart(
                    final long recordingId0,
                    final long joinPosition0,
                    final int sessionId0,
                    final int streamId0,
                    final String channel,
                    final String sourceIdentity)
                {
                    recordingId = recordingId0;
                    assertThat(streamId0, is(PUBLISH_STREAM_ID));
                    assertThat(sessionId0, is(sessionId));
                    assertThat(joinPosition0, is(joinPosition));
                    println("Recording started. sourceIdentity: " + sourceIdentity);
                }
            },
            recordingEvents,
            1);

        waitFor(() -> recordingEventsPoller.poll() != 0);

        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);
    }

    private void postPublicationValidations(
        final ArchiveProxy archiveProxy,
        final Subscription recordingEvents,
        final int sessionId,
        final int streamId,
        final int termBufferLength,
        final int initialTermId,
        final int maxPayloadLength,
        final int messageCount) throws IOException
    {
        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);

        assertNull(trackerError);
        println("All data arrived");

        println("Request stop recording");
        final long requestStopCorrelationId = this.correlationId++;
        waitFor(() -> archiveProxy.stopRecording(
            recordingUri(publishUri),
            PUBLISH_STREAM_ID,
            requestStopCorrelationId));
        waitForOk(controlResponse, requestStopCorrelationId);

        final RecordingEventsPoller recordingEventsPoller = new RecordingEventsPoller(
            new FailRecordingEventsListener()
            {
                public void onStop(final long rId, final long joinPosition, final long endPosition)
                {
                    assertThat(rId, is(recordingId));
                }
            },
            recordingEvents,
            1);

        waitFor(() -> recordingEventsPoller.poll() != 0);

        verifyDescriptorListOngoingArchive(archiveProxy, termBufferLength);

        println("Recording id: " + recordingId);
        println("Meta data file printout: ");
        validateArchiveFile(messageCount, recordingId);

        validateReplay(
            archiveProxy,
            messageCount,
            initialTermId,
            maxPayloadLength,
            termBufferLength);
    }

    private void prePublicationActionsAndVerifications(
        final Aeron aeron,
        final ArchiveProxy archiveProxy,
        final Publication controlPublication,
        final Subscription recordingEvents)
    {
        awaitPublicationIsConnected(controlPublication);
        awaitSubscriptionIsConnected(recordingEvents);
        println("Archive service connected");

        controlResponse = publishingClient.addSubscription(CONTROL_URI, CONTROL_STREAM_ID);
        assertTrue(archiveProxy.connect(CONTROL_URI, CONTROL_STREAM_ID));
        awaitSubscriptionIsConnected(controlResponse);
        println("Client connected");

        verifyEmptyDescriptorList(archiveProxy);
        final long startRecordingCorrelationId = this.correlationId++;
        waitFor(() -> archiveProxy.startRecording(
            recordingUri(publishUri),
            PUBLISH_STREAM_ID,
            startRecordingCorrelationId));
        println("Recording requested");
        waitForOk(controlResponse, startRecordingCorrelationId);

        startChannelDrainingSubscription(aeron, this.publishUri, PUBLISH_STREAM_ID);
    }

    public static String recordingUri(final String channel)
    {
        if (channel.contains("ipc"))
        {
            return channel;
        }
        else
        {
            return SPY_PREFIX + channel;
        }
    }

    public static void startChannelDrainingSubscription(final Aeron aeron, final String channel, final int streamId)
    {
        if (channel.contains("ipc"))
        {
            return;
        }

        final Thread t = new Thread(() ->
        {
            try (Subscription subscription = aeron.addSubscription(channel, streamId))
            {
                while (subscription.imageCount() == 0)
                {
                    LockSupport.parkNanos(1);
                }

                while (!subscription.isClosed())
                {
                    if (0 == subscription.poll((buffer1, offset, length, header) -> {}, Integer.MAX_VALUE))
                    {
                        LockSupport.parkNanos(1);
                    }
                }
            }
            catch (final Exception e)
            {
                e.printStackTrace();
            }
        });

        t.setDaemon(true);
        t.setName("eager-subscriber");
        t.start();
    }

    private void verifyEmptyDescriptorList(final ArchiveProxy client)
    {
        final long requestRecordingsCorrelationId = this.correlationId++;
        client.listRecordings(0, 100, requestRecordingsCorrelationId);
        TestUtil.waitForFail(controlResponse, requestRecordingsCorrelationId);
    }

    private void verifyDescriptorListOngoingArchive(
        final ArchiveProxy archiveProxy, final int publicationTermBufferLength)
    {
        final long requestRecordingsCorrelationId = this.correlationId++;
        archiveProxy.listRecordings(recordingId, 1, requestRecordingsCorrelationId);
        println("Await result");

        final ControlResponsePoller controlResponsePoller = new ControlResponsePoller(
            new FailControlResponseListener()
            {
                public void onRecordingDescriptor(
                    final long correlationId,
                    final long recordingId,
                    final long joinTimestamp,
                    final long endTimestamp,
                    final long joinPosition,
                    final long endPosition,
                    final int initialTermId,
                    final int termBufferLength,
                    final int mtuLength,
                    final int segmentFileLength,
                    final int sessionId,
                    final int streamId,
                    final String channel,
                    final String sourceIdentity)
                {
                    assertThat(recordingId, is(ArchiverSystemTest.this.recordingId));
                    assertThat(termBufferLength, is(publicationTermBufferLength));

                    assertThat(streamId, is(PUBLISH_STREAM_ID));

                    assertThat(correlationId, is(requestRecordingsCorrelationId));
                }

            },
            controlResponse,
            1
        );

        waitFor(() -> controlResponsePoller.poll() != 0);
    }

    private int prepAndSendMessages(final Subscription recordingEvents, final Publication publication)
    {
        final int messageCount = 5000 + rnd.nextInt(10000);
        final CountDownLatch waitForData = new CountDownLatch(1);
        prepFragmentsAndListener(recordingEvents, messageCount, waitForData);
        publishDataToRecorded(publication, messageCount);
        await(waitForData);

        return messageCount;
    }

    private int prepAndSendMessages(final Subscription recordingEvents, final ExclusivePublication publication)
    {
        final int messageCount = 5000 + rnd.nextInt(10000);
        final CountDownLatch waitForData = new CountDownLatch(1);
        prepFragmentsAndListener(recordingEvents, messageCount, waitForData);
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
        catch (final InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void prepFragmentsAndListener(
        final Subscription recordingEvents,
        final int messageCount,
        final CountDownLatch waitForData)
    {
        fragmentLength = new int[messageCount];
        for (int i = 0; i < messageCount; i++)
        {
            final int messageLength = 64 + rnd.nextInt(MAX_FRAGMENT_SIZE - 64) - HEADER_LENGTH;
            fragmentLength[i] = messageLength + HEADER_LENGTH;
            totalDataLength += BitUtil.align(fragmentLength[i], FrameDescriptor.FRAME_ALIGNMENT);
        }

        printf("Sending %d messages, total length=%d %n", messageCount, totalDataLength);

        trackRecordingProgress(recordingEvents, waitForData);
    }

    private void publishDataToRecorded(final Publication publication, final int messageCount)
    {
        joinPosition = publication.position();

        buffer.setMemory(0, 1024, (byte)'z');
        buffer.putStringAscii(32, "TEST");

        for (int i = 0; i < messageCount; i++)
        {
            final int dataLength = fragmentLength[i] - HEADER_LENGTH;
            buffer.putInt(0, i);
            printf("Sending: index=%d length=%d %n", i, dataLength);
            TestUtil.offer(publication, buffer, dataLength);
        }

        final long position = publication.position();
        totalRecordingLength = position - joinPosition;
        endPosition = position;
    }

    private void publishDataToRecorded(final ExclusivePublication publication, final int messageCount)
    {
        joinPosition = publication.position();

        buffer.setMemory(0, 1024, (byte)'z');
        buffer.putStringAscii(32, "TEST");

        for (int i = 0; i < messageCount; i++)
        {
            final int dataLength = fragmentLength[i] - HEADER_LENGTH;
            buffer.putInt(0, i);
            printf("Sending: index=%d length=%d %n", i, dataLength);
            TestUtil.offer(publication, buffer, dataLength);
        }

        final long position = publication.position();
        totalRecordingLength = position - joinPosition;
        endPosition = position;
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

            waitFor(() -> archiveProxy.replay(
                recordingId,
                joinPosition,
                totalRecordingLength,
                REPLAY_URI,
                REPLAY_STREAM_ID,
                replayCorrelationId));

            waitForOk(controlResponse, replayCorrelationId);

            awaitSubscriptionIsConnected(replay);
            final Image image = replay.images().get(0);
            assertThat(image.initialTermId(), is(initialTermId));
            assertThat(image.mtuLength(), is(maxPayloadLength + HEADER_LENGTH));
            assertThat(image.termBufferLength(), is(termBufferLength));
            assertThat(image.position(), is(joinPosition));

            fragmentCount = 0;
            remaining = totalDataLength;

            while (remaining > 0)
            {
                printf("Fragment [%d of %d]%n", fragmentCount + 1, fragmentLength.length);
                poll(replay, this::validateFragment2);
            }

            assertThat(fragmentCount, is(messageCount));
            assertThat(remaining, is(0L));
        }
    }

    private void validateArchiveFile(final int messageCount, final long recordingId) throws IOException
    {
        remaining = totalDataLength;
        try (Catalog catalog = new Catalog(archiveDir);
             RecordingFragmentReader archiveDataFileReader =
                 newRecordingFragmentReader(catalog.wrapDescriptor(recordingId), archiveDir))
        {
            fragmentCount = 0;
            remaining = totalDataLength;
            while (!archiveDataFileReader.isDone())
            {
                archiveDataFileReader.controlledPoll(this::validateFragment1, messageCount);
            }
            assertThat(remaining, is(0L));
            assertThat(fragmentCount, is(messageCount));
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

        final int expectedLength = fragmentLength[fragmentCount] - HEADER_LENGTH;
        assertThat("on fragment[" + fragmentCount + "]", length, is(expectedLength));
        assertThat(buffer.getInt(offset), is(fragmentCount));
        assertThat(buffer.getByte(offset + 4), is((byte)'z'));

        remaining -= BitUtil.align(fragmentLength[fragmentCount], FrameDescriptor.FRAME_ALIGNMENT);
        fragmentCount++;

        return true;
    }

    private void validateFragment2(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        @SuppressWarnings("unused") final Header header)
    {
        assertThat(length, is(fragmentLength[fragmentCount] - HEADER_LENGTH));
        assertThat(buffer.getInt(offset), is(fragmentCount));
        assertThat(buffer.getByte(offset + 4), is((byte)'z'));
        remaining -= BitUtil.align(fragmentLength[fragmentCount], FrameDescriptor.FRAME_ALIGNMENT);
        fragmentCount++;
        printf("Fragment2: offset=%d length=%d %n", offset, length);
    }

    private void trackRecordingProgress(final Subscription recordingEvents, final CountDownLatch waitForData)
    {
        final RecordingEventsPoller recordingEventsPoller = new RecordingEventsPoller(
            new FailRecordingEventsListener()
            {
                public void onProgress(
                    final long recordingId0,
                    final long joinPosition,
                    final long position)
                {
                    assertThat(recordingId0, is(recordingId));
                    recorded = position - joinPosition;
                    printf("a=%d total=%d %n", recorded, totalRecordingLength);
                }
            },
            recordingEvents,
            1);

        final Thread t = new Thread(
            () ->
            {
                try
                {
                    recorded = 0;
                    long start = System.currentTimeMillis();
                    long startBytes = remaining;

                    while (endPosition == -1 || recorded < totalRecordingLength)
                    {
                        waitFor(() -> recordingEventsPoller.poll() != 0);

                        final long end = System.currentTimeMillis();
                        final long deltaTime = end - start;
                        if (deltaTime > TestUtil.TIMEOUT)
                        {
                            start = end;
                            final long deltaBytes = remaining - startBytes;
                            startBytes = remaining;
                            final double rate = ((deltaBytes * 1000.0) / deltaTime) / MEGABYTE;
                            printf("Archive reported rate: %f MB/s %n", rate);
                        }
                    }
                    final long end = System.currentTimeMillis();
                    final long deltaTime = end - start;

                    final long deltaBytes = remaining - startBytes;
                    final double rate = ((deltaBytes * 1000.0) / deltaTime) / MEGABYTE;
                    printf("Archive reported rate: %f MB/s %n", rate);
                }
                catch (final Throwable throwable)
                {
                    throwable.printStackTrace();
                    trackerError = throwable;
                }

                waitForData.countDown();
            });

        t.setDaemon(true);
        t.start();
    }
}
