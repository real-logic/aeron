/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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
import io.aeron.archiver.messages.*;
import io.aeron.driver.*;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

public class SystemTest
{
    private static final boolean DEBUG = false;
    private static final String REPLAY_URI = "aeron:udp?endpoint=127.0.0.1:54326";
    private static final String PUBLISH_URI = "aeron:udp?endpoint=127.0.0.1:54325";
    private static final int PUBLISH_STREAM_ID = 1;

    private static final ThreadingMode THREADING_MODE = ThreadingMode.DEDICATED;

    private final MediaDriver.Context driverCtx = new MediaDriver.Context();
    private final Archiver.Context archiverCtx = new Archiver.Context();

    private Aeron publishingClient;
    private Archiver archiver;
    private MediaDriver driver;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[4096]);

    private File archiveFolder;
    private int streamInstanceId;
    private String source;
    private int delivered;
    private int nextMessage;
    private int fragmentCount;

    @Before
    public void setUp() throws Exception
    {
        driverCtx.threadingMode(THREADING_MODE);

        driver = MediaDriver.launch(driverCtx);
        archiveFolder = ImageArchivingSessionTest.makeTempFolder();
        archiverCtx.archiveFolder(archiveFolder);
        archiver = Archiver.launch(archiverCtx);
        println("Archiver started, folder: " + archiverCtx.archiveFolder().getAbsolutePath());
        publishingClient = Aeron.connect();
    }

    private void requestArchive(final Publication archiverServiceRequest, final String channel, final int streamId)
    {
        new MessageHeaderEncoder()
            .wrap(buffer, 0)
            .templateId(ArchiveStartRequestEncoder.TEMPLATE_ID)
            .blockLength(ArchiveStartRequestEncoder.BLOCK_LENGTH)
            .schemaId(ArchiveStartRequestEncoder.SCHEMA_ID)
            .version(ArchiveStartRequestEncoder.SCHEMA_VERSION);

        new ArchiveStartRequestEncoder()
            .wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .channel(channel)
            .streamId(streamId);

        offer(archiverServiceRequest, buffer, 1000);
    }


    private void requestArchiveStop(final Publication archiverServiceRequest, final String channel, final int streamId)
    {
        new MessageHeaderEncoder()
            .wrap(buffer, 0)
            .templateId(ArchiveStopRequestEncoder.TEMPLATE_ID)
            .blockLength(ArchiveStopRequestEncoder.BLOCK_LENGTH)
            .schemaId(ArchiveStopRequestEncoder.SCHEMA_ID)
            .version(ArchiveStopRequestEncoder.SCHEMA_VERSION);

        new ArchiveStopRequestEncoder()
            .wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .channel(channel)
            .streamId(streamId);

        offer(archiverServiceRequest, buffer, 1000);
    }

    private void requestReplay(
        final Publication archiverServiceRequest,
        final String source,
        final int sessionId,
        final String channel,
        final int streamId,
        final int termId,
        final int termOffset,
        final int length,
        final String replyChannel,
        final int replayStreamId,
        final int controlStreamId)
    {
        new MessageHeaderEncoder()
            .wrap(buffer, 0)
            .templateId(ReplayRequestEncoder.TEMPLATE_ID)
            .blockLength(ReplayRequestEncoder.BLOCK_LENGTH)
            .schemaId(ReplayRequestEncoder.SCHEMA_ID)
            .version(ReplayRequestEncoder.SCHEMA_VERSION);

        final ReplayRequestEncoder mEncoder = new ReplayRequestEncoder()
            .wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH)
            .source(source)
            .sessionId(sessionId)
            .channel(channel)
            .streamId(streamId)
            .termId(termId)
            .termOffset(termOffset)
            .length(length)
            .replyChannel(replyChannel)
            .replayStreamId(replayStreamId)
            .controlStreamId(controlStreamId);

        println(mEncoder.toString());
        offer(archiverServiceRequest, buffer, 1000);
    }

    @After
    public void closeEverything() throws Exception
    {
        CloseHelper.quietClose(publishingClient);
        CloseHelper.quietClose(archiver);
        CloseHelper.quietClose(driver);

        driverCtx.deleteAeronDirectory();
    }

    @Test(timeout = 10000)
    public void archiveAndReplay() throws IOException, InterruptedException
    {
        final Publication archiverServiceRequest = publishingClient.addPublication(
            archiverCtx.serviceRequestChannel(), archiverCtx.serviceRequestStreamId());

        final Subscription archiverNotifications = publishingClient.addSubscription(
            archiverCtx.archiverNotificationsChannel(), archiverCtx.archiverNotificationsStreamId());

        awaitPublicationIsConnected(archiverServiceRequest, 1000);
        awaitSubscriptionIsConnected(archiverNotifications, 1000);
        println("Archive service connected");

        requestArchive(archiverServiceRequest, PUBLISH_URI, PUBLISH_STREAM_ID);
        println("Archive requested");

        final Publication publication = publishingClient.addPublication(PUBLISH_URI, PUBLISH_STREAM_ID);
        awaitPublicationIsConnected(publication, 1000);


        // the archiver has subscribed to the publication, now we wait for the archive start message
        poll(archiverNotifications,
            (buffer, offset, length, header) ->
            {
                final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(buffer, offset);
                Assert.assertEquals(ArchiveStartedNotificationDecoder.TEMPLATE_ID, hDecoder.templateId());

                final ArchiveStartedNotificationDecoder mDecoder = new ArchiveStartedNotificationDecoder()
                    .wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        hDecoder.blockLength(),
                        hDecoder.version());

                streamInstanceId = mDecoder.streamInstanceId();
                Assert.assertEquals(mDecoder.streamId(), PUBLISH_STREAM_ID);
                Assert.assertEquals(mDecoder.sessionId(), publication.sessionId());
                source = mDecoder.source();
                final String channel = mDecoder.channel();
                Assert.assertEquals(channel, PUBLISH_URI);
                println("Archive started. source: " + source);
            }, 1, 1000);

        final int messageCount = 128;
        final CountDownLatch waitForData = new CountDownLatch(1);

        trackArchiveProgress(publication, archiverNotifications, messageCount, waitForData);
        publishDataToBeArchived(publication, messageCount);


        waitForData.await();
        println("All data arrived");

        requestArchiveStop(archiverServiceRequest, PUBLISH_URI, PUBLISH_STREAM_ID);
        println("Request stop archive");

        // wait for the archive stopped message
        awaitArchiveStoppedNotification(archiverNotifications);

        println("stream instance id: " + streamInstanceId);
        println("Meta data file printout: ");

        final File metaFile = new File(archiveFolder, ArchiveFileUtil.archiveMetaFileName(streamInstanceId));
        Assert.assertTrue(metaFile.exists());

        if (DEBUG)
        {
            ArchiveFileUtil.printMetaFile(metaFile);
        }

        validateArchiveFile(messageCount, streamInstanceId);
        validateReplay(archiverServiceRequest, publication);
    }

    private void awaitArchiveStoppedNotification(final Subscription archiverNotifications)
    {
        poll(archiverNotifications,
            (buffer, offset, length, header) ->
            {
                final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(buffer, offset);
                Assert.assertEquals(ArchiveStoppedNotificationDecoder.TEMPLATE_ID, hDecoder.templateId());

                final ArchiveStoppedNotificationDecoder mDecoder = new ArchiveStoppedNotificationDecoder()
                    .wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        hDecoder.blockLength(),
                        hDecoder.version());

                Assert.assertEquals(mDecoder.streamInstanceId(), streamInstanceId);
            }, 1, 1000);

        println("Archive stopped");
    }

    private void publishDataToBeArchived(final Publication publication, final int messageCount)
    {
        // clear out the buffer we write
        for (int i = 0; i < 1024; i++)
        {
            buffer.putByte(i, (byte)'z');
        }
        buffer.putStringAscii(32, "TEST");
        buffer.putStringAscii(1024 - DataHeaderFlyweight.HEADER_LENGTH - 6, "\r\n");

        for (int i = 0; i < messageCount; i++)
        {
            buffer.putInt(0, (byte)i);
            offer(publication, buffer, 0, 1024 - DataHeaderFlyweight.HEADER_LENGTH, 1000);
            if (i % (1024 * 128) == 0)
            {
                println("Sent out " + (i / 1024) + "K messages");
            }
        }
    }

    private void validateReplay(final Publication archiverServiceRequest, final Publication publication)
    {
        // request replay
        requestReplay(
            archiverServiceRequest,
            source,
            publication.sessionId(),
            PUBLISH_URI,
            PUBLISH_STREAM_ID,
            publication.initialTermId(),
            0,
            delivered,
            REPLAY_URI,
            1,
            2);

        final Subscription replay = publishingClient.addSubscription(REPLAY_URI, 1);
        final Subscription control = publishingClient.addSubscription(REPLAY_URI, 2);
        poll(control,
            (buffer, offset, length, header) ->
            {
                final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(buffer, offset);
                Assert.assertEquals(ArchiverResponseDecoder.TEMPLATE_ID, hDecoder.templateId());

                final ArchiverResponseDecoder mDecoder = new ArchiverResponseDecoder()
                    .wrap(
                        buffer,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        hDecoder.blockLength(),
                        hDecoder.version());
                Assert.assertEquals(mDecoder.err(), "");
            }, 1, 1000);

        // break replay back into data
        final DataHeaderFlyweight dHeader = new DataHeaderFlyweight();
        this.nextMessage = 0;
        while (delivered > 0)
        {
            poll(replay,
                (directBuffer, offset, length, header) ->
                {
                    int messageStart;
                    int frameLength;
                    do
                    {
                        messageStart = offset + (this.nextMessage % length);
                        dHeader.wrap(directBuffer, messageStart, length);

                        frameLength = dHeader.frameLength();
                        Assert.assertEquals(1024, frameLength);
                        if (messageStart + 32 < offset + length)
                        {
                            final int index = directBuffer.getInt(messageStart + 32);
                            Assert.assertEquals(this.fragmentCount, index);
                            printf("Fragment: length=%d \t, offset=%d \t, getInt(0)=%d %n",
                                frameLength, (this.nextMessage % length), index);
                        }

                        this.fragmentCount++;
                        this.nextMessage += frameLength;
                    }
                    while (messageStart + frameLength < offset + length);

                    delivered -= length;
                }, 1, 1000);
        }
    }

    private void validateArchiveFile(final int messageCount, final int streamInstanceId) throws IOException
    {
        final File archiveFile1 = new File(
            archiveFolder, ArchiveFileUtil.archiveDataFileName(streamInstanceId, 0));
        Assert.assertTrue(archiveFile1.exists());

        // validate file data
        final ByteBuffer bb = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
        final RandomAccessFile randomAccessFile = new RandomAccessFile(archiveFile1, "r");
        final FileChannel channel = randomAccessFile.getChannel();
        for (int i = 0; i < messageCount; i++)
        {
            channel.read(bb);
            Assert.assertEquals(i, bb.getInt(32));
            Assert.assertEquals('z', bb.get(36));
            bb.clear();
        }
    }

    private void trackArchiveProgress(
        final Publication publication,
        final Subscription archiverNotifications,
        final int messageCount,
        final CountDownLatch waitForData)
    {
        final Thread t = new Thread(
            () ->
            {
                delivered = 0;
                long start = System.currentTimeMillis();
                long startBytes = delivered;
                // each message is 1024
                while (delivered < messageCount * 1024)
                {
                    poll(archiverNotifications,
                        (buffer, offset, length, header) ->
                        {
                            final MessageHeaderDecoder hDecoder = new MessageHeaderDecoder().wrap(buffer, offset);
                            Assert.assertEquals(ArchiveProgressNotificationDecoder.TEMPLATE_ID, hDecoder.templateId());

                            final ArchiveProgressNotificationDecoder mDecoder =
                                new ArchiveProgressNotificationDecoder()
                                    .wrap(
                                        buffer,
                                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                                        hDecoder.blockLength(),
                                        hDecoder.version());
                            Assert.assertEquals(streamInstanceId, mDecoder.streamInstanceId());
                            Assert.assertEquals(publication.initialTermId(), mDecoder.initialTermId());
                            Assert.assertEquals(0, mDecoder.initialTermOffset());

                            delivered = publication.termBufferLength() *
                                (mDecoder.termId() - mDecoder.initialTermId()) +
                                (mDecoder.termOffset() - mDecoder.initialTermOffset());
                        }, 1, 1000);

                    final long end = System.currentTimeMillis();
                    final long deltaTime = end - start;
                    if (deltaTime > 1000)
                    {
                        start = end;
                        final long deltaBytes = delivered - startBytes;
                        startBytes = delivered;
                        final double mbps = ((deltaBytes * 1000.0) / deltaTime) / (1024.0 * 1024.0);
                        printf("Archive reported speed: %f MB/s %n", mbps);
                    }
                }
                final long end = System.currentTimeMillis();
                final long deltaTime = end - start;

                final long deltaBytes = delivered - startBytes;
                final double mbps = ((deltaBytes * 1000.0) / deltaTime) / (1024.0 * 1024.0);
                printf("Archive reported speed: %f MB/s %n", mbps);

                waitForData.countDown();
            });

        t.setDaemon(true);
        t.start();
    }

    private void poll(final Subscription s, final FragmentHandler f, final int count, final long timeout)
    {
        final long limit = System.currentTimeMillis() + timeout;
        while (0 >= s.poll(f, count))
        {
            LockSupport.parkNanos(1000);
            if (limit < System.currentTimeMillis())
            {
                Assert.fail("Poll has timed out");
            }
        }
    }

    private void offer(final Publication publication, final UnsafeBuffer buffer, final long timeout)
    {
        final long limit = System.currentTimeMillis() + timeout;
        while (publication.offer(buffer) < 0)
        {
            LockSupport.parkNanos(1000);
            if (limit < System.currentTimeMillis())
            {
                Assert.fail("Offer has timed out");
            }
        }
    }

    private void offer(
        final Publication publication,
        final UnsafeBuffer buffer,
        final int offset,
        final int length,
        final long timeout)
    {
        final long limit = System.currentTimeMillis() + timeout;
        while (publication.offer(buffer, offset, length) < 0)
        {
            LockSupport.parkNanos(1000);
            if (limit < System.currentTimeMillis())
            {
                Assert.fail("Offer has timed out");
            }
        }
    }

    private void awaitSubscriptionIsConnected(final Subscription subscription, final long timeout)
    {
        final long limit = System.currentTimeMillis() + timeout;
        while (subscription.imageCount() == 0)
        {
            LockSupport.parkNanos(1000);
            if (limit < System.currentTimeMillis())
            {
                Assert.fail("awaitSubscriptionIsConnected has timed out");
            }
        }
    }

    private void awaitPublicationIsConnected(final Publication publication, final long timeout)
    {
        final long limit = System.currentTimeMillis() + timeout;
        while (!publication.isConnected())
        {
            LockSupport.parkNanos(1000);
            if (limit < System.currentTimeMillis())
            {
                Assert.fail("awaitPublicationIsConnected has timed out");
            }
        }
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
