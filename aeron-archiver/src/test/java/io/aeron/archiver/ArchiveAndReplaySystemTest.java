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
import io.aeron.archiver.codecs.*;
import io.aeron.driver.*;
import io.aeron.logbuffer.*;
import io.aeron.protocol.*;
import org.agrona.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.*;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static io.aeron.archiver.ArchiveFileUtil.archiveMetaFileFormatDecoder;
import static io.aeron.archiver.ArchiveFileUtil.archiveMetaFileName;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

@Ignore
public class ArchiveAndReplaySystemTest
{
    private static final int TIMEOUT = 5000;
    private static final double MEGABYTE = 1024.0d * 1024.0d;
    private static final int SLEEP_TIME_NS = 5000;

    private static final boolean DEBUG = false;
    private static final String REPLY_URI = "aeron:udp?endpoint=127.0.0.1:54327";
    private static final String REPLAY_URI = "aeron:udp?endpoint=127.0.0.1:54326";
    private static final String PUBLISH_URI = "aeron:udp?endpoint=127.0.0.1:54325";
    private static final int PUBLISH_STREAM_ID = 1;
    private static final int MAX_FRAGMENT_SIZE = 1024;
    private final MediaDriver.Context driverCtx = new MediaDriver.Context();
    private final Archiver.Context archiverCtx = new Archiver.Context();
    private Aeron publishingClient;
    private Archiver archiver;
    private MediaDriver driver;
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);
    private File archiveFolder;
    private int streamInstanceId;
    private String source;
    private long remaining;
    private int nextFragmentOffset;
    private int fragmentCount;
    private int[] fragmentLength;
    private long totalDataLength;
    private long totalArchiveLength;
    private long archived;
    private volatile int lastTermId = -1;
    private Throwable trackerError;
    private Random rnd = new Random();
    private long seed;
    private int replyStreamId = 100;

    @Rule
    public TestWatcher testWatcher = new TestWatcher()
    {
        protected void failed(final Throwable t, final Description description)
        {
            System.err.println(
                "ArchiveAndReplaySystemTest failed with random seed: " + ArchiveAndReplaySystemTest.this.seed);
        }
    };

    @Before
    public void setUp() throws Exception
    {
        seed = System.nanoTime();
        rnd.setSeed(seed);

        driverCtx
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .errorHandler(LangUtil::rethrowUnchecked)
            .dirsDeleteOnStart(true);

        driver = MediaDriver.launch(driverCtx);
        archiveFolder = TestUtil.makeTempFolder();
        archiverCtx.archiveFolder(archiveFolder);
        archiver = Archiver.launch(archiverCtx);
        println("Archiver started, folder: " + archiverCtx.archiveFolder().getAbsolutePath());
        publishingClient = Aeron.connect();
    }

    @After
    public void closeEverything() throws Exception
    {
        CloseHelper.close(publishingClient);
        CloseHelper.close(archiver);
        CloseHelper.close(driver);

        if (null != archiveFolder)
        {
            IoUtil.delete(archiveFolder, false);
        }

        driverCtx.deleteAeronDirectory();
    }

    @Test(timeout = 60000)
    public void archiveAndReplay() throws IOException, InterruptedException
    {
        try (Publication archiverServiceRequest = publishingClient.addPublication(
                archiverCtx.serviceRequestChannel(), archiverCtx.serviceRequestStreamId());
             Subscription archiverNotifications = publishingClient.addSubscription(
                archiverCtx.archiverNotificationsChannel(), archiverCtx.archiverNotificationsStreamId()))
        {
            final ArchiverClient client = new ArchiverClient(archiverServiceRequest, archiverNotifications);

            awaitPublicationIsConnected(archiverServiceRequest);
            awaitSubscriptionIsConnected(archiverNotifications);

            println("Archive service connected");
            verifyEmptyDescriptorList(client);

            wait(() -> client.requestArchiveStart(PUBLISH_URI, PUBLISH_STREAM_ID));

            println("Archive requested");
            final Publication publication = publishingClient.addPublication(PUBLISH_URI, PUBLISH_STREAM_ID);
            awaitPublicationIsConnected(publication);

            // awaitArchiveForPublicationStartedNotification
            wait(() -> (client.pollNotifications(new ArchiverClient.ArchiverNotificationListener()
            {
                public void onStart(
                    final int iStreamInstanceId,
                    final int sessionId,
                    final int streamId,
                    final String iSource,
                    final String channel)
                {
                    streamInstanceId = iStreamInstanceId;
                    assertThat(streamId, is(PUBLISH_STREAM_ID));
                    assertThat(sessionId, is(publication.sessionId()));

                    source = iSource;
                    assertThat(channel, is(PUBLISH_URI));
                    println("Archive started. source: " + source);
                }
                public void onProgress(final int s, final int i1, final int i2, final int t1, final int t2)
                {
                    fail();
                }
                public void onStop(final int s)
                {
                    fail();
                }
            }, 1) != 0));

            verifyDescriptorListOngoingArchive(client, publication, 0);
            final int messageCount = prepAndSendMessages(client, publication);
            verifyDescriptorListOngoingArchive(client, publication, totalArchiveLength);

            assertNull(trackerError);
            println("All data arrived");

            println("Request stop archive");
            wait(() -> client.requestArchiveStop(PUBLISH_URI, PUBLISH_STREAM_ID));

            // awaitArchiveStoppedNotification
            wait(() -> (client.pollNotifications(new ArchiverClient.ArchiverNotificationListener()
            {
                public void onProgress(final int s, final int i1, final int i2, final int t1, final int t2)
                {
                    fail();
                }
                public void onStart(final int s1, final int s2, final int s3, final String s4, final String c)
                {
                    fail();
                }
                public void onStop(final int s)
                {
                    assertThat(s, is(streamInstanceId));
                }
            }, 1) != 0));

            verifyDescriptorListOngoingArchive(client, publication, totalArchiveLength);

            println("Stream instance id: " + streamInstanceId);
            println("Meta data file printout: ");

            validateMetaDataFile(publication);
            validateArchiveFile(messageCount, streamInstanceId);
            validateArchiveFileChunked(messageCount, streamInstanceId);

            validateReplay(client, publication, messageCount);
        }
    }

    private void verifyEmptyDescriptorList(final ArchiverClient client)
    {
        final int replyStreamId = this.replyStreamId++;
        try (Subscription archiverListReply = publishingClient.addSubscription(REPLY_URI, replyStreamId))
        {
            client.requestListStreamInstances(REPLY_URI, replyStreamId, 0, 100);
            awaitSubscriptionIsConnected(archiverListReply);
            poll(
                archiverListReply,
                (b, offset, length, header) ->
                {
                    final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(b, offset);
                    assertThat(hDecoder.templateId(), is(ArchiverResponseDecoder.TEMPLATE_ID));
                }
            );
        }
    }

    private void verifyDescriptorListOngoingArchive(
        final ArchiverClient client,
        final Publication publication,
        final long archiveLength)
    {
        final int replyStreamId = this.replyStreamId++;
        try (Subscription archiverListReply = publishingClient.addSubscription(REPLY_URI, replyStreamId))
        {
            client.requestListStreamInstances(REPLY_URI, replyStreamId, streamInstanceId, streamInstanceId);
            awaitSubscriptionIsConnected(archiverListReply);
            println("Await result");

            poll(
                archiverListReply,
                (b, offset, length, header) ->
                {
                    final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(b, offset);
                    assertThat(hDecoder.templateId(), is(ArchiveDescriptorDecoder.TEMPLATE_ID));

                    final ArchiveDescriptorDecoder decoder = new ArchiveDescriptorDecoder();
                    decoder.wrap(b,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        hDecoder.blockLength(),
                        hDecoder.version());

                    assertThat(decoder.streamInstanceId(), is(streamInstanceId));
                    assertThat(decoder.streamId(), is(PUBLISH_STREAM_ID));
                    assertThat(decoder.imageInitialTermId(), is(publication.initialTermId()));

                    final long archiveFullLength = ArchiveFileUtil.archiveFullLength(decoder);
                    assertThat(archiveFullLength, is(archiveLength));
                    //....
                }
            );
        }
    }

    private int prepAndSendMessages(
        final ArchiverClient client,
        final Publication publication)
        throws InterruptedException
    {
        final int messageCount = 128 + rnd.nextInt(10000);
        fragmentLength = new int[messageCount];
        for (int i = 0; i < messageCount; i++)
        {
            final int messageLength = 64 + rnd.nextInt(MAX_FRAGMENT_SIZE - 64) - DataHeaderFlyweight.HEADER_LENGTH;
            fragmentLength[i] = messageLength + DataHeaderFlyweight.HEADER_LENGTH;
            totalDataLength += fragmentLength[i];
        }

        final CountDownLatch waitForData = new CountDownLatch(1);
        printf("Sending %d messages, total length=%d %n", messageCount, totalDataLength);

        trackArchiveProgress(client, publication.termBufferLength(), waitForData);
        publishDataToBeArchived(publication, messageCount);
        waitForData.await();

        return messageCount;
    }

    private void validateMetaDataFile(final Publication publication) throws IOException
    {
        final File metaFile = new File(archiveFolder, archiveMetaFileName(streamInstanceId));
        assertTrue(metaFile.exists());

        if (DEBUG)
        {
            ArchiveFileUtil.printMetaFile(metaFile);
        }

        final ArchiveDescriptorDecoder decoder = archiveMetaFileFormatDecoder(metaFile);
        assertThat(decoder.initialTermId(), is(publication.initialTermId()));
        assertThat(decoder.sessionId(), is(publication.sessionId()));
        assertThat(decoder.streamId(), is(publication.streamId()));
        assertThat(decoder.termBufferLength(), is(publication.termBufferLength()));

        assertThat(ArchiveFileUtil.archiveFullLength(decoder), is(totalArchiveLength));
        // length might exceed data sent due to padding
        assertThat(totalDataLength, lessThanOrEqualTo(totalArchiveLength));

        IoUtil.unmap(decoder.buffer().byteBuffer());
    }

    private void publishDataToBeArchived(final Publication publication, final int messageCount)
    {
        final int positionBitsToShift = Integer.numberOfTrailingZeros(publication.termBufferLength());
        final long initialPosition = publication.position();
        final int initialTermOffset = LogBufferDescriptor.computeTermOffsetFromPosition(
            initialPosition, positionBitsToShift);
        // clear out the buffer we write
        for (int i = 0; i < 1024; i++)
        {
            buffer.putByte(i, (byte)'z');
        }
        buffer.putStringAscii(32, "TEST");

        for (int i = 0; i < messageCount; i++)
        {
            final int dataLength = fragmentLength[i] - DataHeaderFlyweight.HEADER_LENGTH;
            buffer.putInt(0, i);
            printf("Sending: index=%d length=%d %n", i, dataLength);
            offer(publication, buffer, dataLength);
        }

        final int lastTermOffset = LogBufferDescriptor.computeTermOffsetFromPosition(
            publication.position(), positionBitsToShift);
        final int termIdFromPosition = LogBufferDescriptor.computeTermIdFromPosition(
            publication.position(), positionBitsToShift, publication.initialTermId());
        totalArchiveLength =
            (termIdFromPosition - publication.initialTermId()) * publication.termBufferLength() +
            (lastTermOffset - initialTermOffset);

        assertThat(publication.position() - initialPosition, is(totalArchiveLength));
        lastTermId = termIdFromPosition;
    }

    private void validateReplay(
        final ArchiverClient client,
        final Publication publication,
        final int messageCount)
    {
        // request replay
        final int replayStreamId = this.replyStreamId++;
        final int controlStreamId = this.replyStreamId++;
        wait(() -> client.requestReplay(
            streamInstanceId,
            publication.initialTermId(),
            0,
            totalArchiveLength,
            REPLAY_URI,
            replayStreamId,
            REPLAY_URI,
            controlStreamId));

        try (Subscription replay = publishingClient.addSubscription(REPLAY_URI, replayStreamId);
             Subscription control = publishingClient.addSubscription(REPLAY_URI, controlStreamId))
        {
            awaitSubscriptionIsConnected(replay);
            awaitSubscriptionIsConnected(control);
            // wait for OK message from control
            poll(
                control,
                (buffer, offset, length, header) ->
                {
                    final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(buffer, offset);
                    assertThat(hDecoder.templateId(), is(ArchiverResponseDecoder.TEMPLATE_ID));

                    final ArchiverResponseDecoder mDecoder = new ArchiverResponseDecoder().wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        hDecoder.blockLength(),
                        hDecoder.version());

                    assertThat(mDecoder.err(), is(""));
                }
            );

            nextFragmentOffset = 0;
            fragmentCount = 0;
            remaining = totalDataLength;

            while (remaining > 0)
            {
                poll(replay, this::validateFragment2);
            }

            assertThat(fragmentCount, is(messageCount));
            assertThat(remaining, is(0L));
        }
    }

    private void validateArchiveFile(final int messageCount, final int streamInstanceId) throws IOException
    {
        try (ArchiveStreamFragmentReader archiveDataFileReader = new ArchiveStreamFragmentReader(
            streamInstanceId, archiveFolder))
        {
            fragmentCount = 0;
            remaining = totalDataLength;
            archiveDataFileReader.controlledPoll(this::validateFragment1, messageCount);

            assertThat(remaining, is(0L));
            assertThat(fragmentCount, is(messageCount));
        }
    }

    private boolean validateFragment1(
        final DirectBuffer buffer,
        final int offset, final int length,
        @SuppressWarnings("unused") final DataHeaderFlyweight header)
    {
        assertThat(length, is(fragmentLength[fragmentCount] - DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(buffer.getInt(offset), is(fragmentCount));
        assertThat(buffer.getByte(offset + 4), is((byte)'z'));

        remaining -= fragmentLength[fragmentCount];
        fragmentCount++;

        return true;
    }
    private void validateFragment2(
        final DirectBuffer buffer,
        final int offset, final int length,
        @SuppressWarnings("unused") final Header header)
    {
        assertThat(length, is(fragmentLength[fragmentCount] - DataHeaderFlyweight.HEADER_LENGTH));
        assertThat(buffer.getInt(offset), is(fragmentCount));
        assertThat(buffer.getByte(offset + 4), is((byte)'z'));

        remaining -= fragmentLength[fragmentCount];
        fragmentCount++;
    }
    private void validateArchiveFileChunked(final int messageCount, final int streamInstanceId) throws IOException
    {
        final ArchiveDescriptorDecoder decoder = archiveMetaFileFormatDecoder(
            new File(archiveFolder, archiveMetaFileName(streamInstanceId)));
        final long archiveFullLength = ArchiveFileUtil.archiveFullLength(decoder);
        final int initialTermId = decoder.initialTermId();
        final int termBufferLength = decoder.termBufferLength();
        final int initialTermOffset = decoder.initialTermOffset();

        IoUtil.unmap(decoder.buffer().byteBuffer());
        try (ArchiveStreamChunkReader cursor = new ArchiveStreamChunkReader(
            streamInstanceId,
            archiveFolder,
            initialTermId,
            termBufferLength,
            initialTermId,
            initialTermOffset,
            archiveFullLength,
            128 * 1024 * 1024))
        {
            fragmentCount = 0;
            final HeaderFlyweight mHeader = new HeaderFlyweight();
            nextFragmentOffset = 0;
            remaining = totalDataLength;

            while (!cursor.isDone())
            {
                cursor.readChunk(
                    (termBuffer, termOffset, chunkLength) ->
                    {
                        validateFragmentsInChunk(
                            mHeader,
                            messageCount,
                            termBuffer,
                            termOffset,
                            chunkLength);
                        return true;
                    },
                    4096 - DataHeaderFlyweight.HEADER_LENGTH);
            }
        }

        assertThat(fragmentCount, is(messageCount));
        assertThat(remaining, is(0L));
    }

    private void validateFragmentsInChunk(
        final HeaderFlyweight mHeader,
        final int messageCount,
        final DirectBuffer termBuffer,
        final int termOffset,
        final int chunkLength)
    {
        printf("Chunk: length=%d \t, offset=%d%n", chunkLength, termOffset);

        int messageStart;
        int frameLength;
        while (nextFragmentOffset < chunkLength)
        {
            messageStart = termOffset + nextFragmentOffset;
            mHeader.wrap(termBuffer, messageStart, HeaderFlyweight.HEADER_LENGTH);
            frameLength = mHeader.frameLength();

            if (mHeader.headerType() == DataHeaderFlyweight.HDR_TYPE_DATA)
            {
                assertThat("Fragments exceed messages", fragmentCount, lessThan(messageCount));
                assertThat("Fragment:" + fragmentCount, frameLength, is(fragmentLength[fragmentCount]));

                if (messageStart + 32 < termOffset + chunkLength)
                {
                    final int index = termBuffer.getInt(messageStart + DataHeaderFlyweight.HEADER_LENGTH);
                    assertThat(String.format(
                        "Fragment: length=%d, foffset=%d, getInt(0)=%d, toffset=%d",
                        frameLength, (nextFragmentOffset % chunkLength), index, termOffset),
                        index, is(fragmentCount));
                    printf("Fragment: length=%d \t, offset=%d \t, getInt(0)=%d %n",
                        frameLength, (nextFragmentOffset % chunkLength), index);
                }

                remaining -= frameLength;
                fragmentCount++;
            }

            final int alignedLength = BitUtil.align(frameLength, FRAME_ALIGNMENT);
            nextFragmentOffset += alignedLength;
        }

        nextFragmentOffset -= chunkLength;
    }

    private void trackArchiveProgress(
        final ArchiverClient client,
        final int termBufferLength,
        final CountDownLatch waitForData)
    {
        final Thread t = new Thread(
            () ->
            {
                try
                {
                    archived = 0;
                    long start = System.currentTimeMillis();
                    long startBytes = remaining;
                    // each message is fragmentLength[fragmentCount]
                    while (lastTermId == -1 || archived < totalArchiveLength)
                    {
                        wait(() -> (client.pollNotifications(new ArchiverClient.ArchiverNotificationListener()
                        {
                            @Override
                            public void onProgress(
                                final int iStreamInstanceId,
                                final int initialTermId,
                                final int initialTermOffset,
                                final int termId,
                                final int termOffset)
                            {
                                assertThat(iStreamInstanceId, is(streamInstanceId));
                                archived = termBufferLength *
                                    (termId - initialTermId) +
                                    (termOffset - initialTermOffset);
                                printf("a=%d total=%d %n", archived, totalArchiveLength);
                            }

                            @Override
                            public void onStart(
                                final int streamInstanceId,
                                final int sessionId,
                                final int streamId,
                                final String source,
                                final String channel)
                            {
                                fail();
                            }

                            @Override
                            public void onStop(final int streamInstanceId)
                            {
                                fail();
                            }
                        }, 1)) != 0);


                        final long end = System.currentTimeMillis();
                        final long deltaTime = end - start;
                        if (deltaTime > TIMEOUT)
                        {
                            start = end;
                            final long deltaBytes = remaining - startBytes;
                            startBytes = remaining;
                            final double mbps = ((deltaBytes * 1000.0) / deltaTime) / MEGABYTE;
                            printf("Archive reported speed: %f MB/s %n", mbps);
                        }
                    }
                    final long end = System.currentTimeMillis();
                    final long deltaTime = end - start;

                    final long deltaBytes = remaining - startBytes;
                    final double mbps = ((deltaBytes * 1000.0) / deltaTime) / MEGABYTE;
                    printf("Archive reported speed: %f MB/s %n", mbps);
                }
                catch (final Throwable throwable)
                {
                    trackerError = throwable;
                }

                waitForData.countDown();
            });

        t.setDaemon(true);
        t.start();
    }

    private void poll(final Subscription subscription, final FragmentHandler handler)
    {
        wait(() -> (0 >= subscription.poll(handler, 1)));
    }

    private void offer(
        final Publication publication,
        final UnsafeBuffer buffer,
        final int length)
    {
        wait(() -> (publication.offer(buffer, 0, length) > 0));
    }

    private void wait(final BooleanSupplier forIt)
    {
        final long limit = System.currentTimeMillis() + TIMEOUT;
        while (!forIt.getAsBoolean())
        {
            LockSupport.parkNanos(SLEEP_TIME_NS);
            if (limit < System.currentTimeMillis())
            {
                fail();
            }
        }
    }

    private void awaitSubscriptionIsConnected(final Subscription subscription)
    {
        wait(() -> (subscription.imageCount() > 0));
    }

    private void awaitPublicationIsConnected(final Publication publication)
    {
        wait(publication::isConnected);
    }

    private void printf(final String s, final Object... args)
    {
        if (DEBUG)
        {
            System.out.printf(s, args);
        }
    }

    private void println(final String s)
    {
        if (DEBUG)
        {
            System.out.println(s);
        }
    }
}
