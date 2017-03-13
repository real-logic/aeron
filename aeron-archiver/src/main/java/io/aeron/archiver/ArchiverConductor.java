/*
 * Copyright 2017 Real Logic Ltd.
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
import io.aeron.logbuffer.Header;
import org.agrona.*;
import org.agrona.collections.*;
import org.agrona.concurrent.*;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderEncoder;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Consumer;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

class ArchiverConductor implements Agent
{
    interface Session
    {
        void abort();
        boolean isDone();
        int doWork();
    }

    private final Aeron aeron;
    private final Subscription serviceRequests;
    private final Publication archiverNotifications;

    private final ArrayList<Session> sessions = new ArrayList<>();

    private final Int2ObjectHashMap<ReplaySession> image2ReplaySession = new Int2ObjectHashMap<>();
    private final Int2ObjectHashMap<ImageArchivingSession> instance2ArchivingSession = new Int2ObjectHashMap<>();

    private final ObjectHashSet<Subscription> archiveSubscriptions = new ObjectHashSet<>(128);

    private final ArchiveIndex archiveIndex;

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final ArchiveStartRequestDecoder archiveStartRequestDecoder = new ArchiveStartRequestDecoder();
    private final ArchiveStopRequestDecoder archiveStopRequestDecoder = new ArchiveStopRequestDecoder();
    private final ListStreamInstancesRequestDecoder requestDecoder =
        new ListStreamInstancesRequestDecoder();

    private final UnsafeBuffer outboundBuffer =
        new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, CACHE_LINE_LENGTH));

    private final MessageHeaderEncoder outboundHeaderEncoder = new MessageHeaderEncoder();
    private final ArchiverResponseEncoder responseEncoder = new ArchiverResponseEncoder();
    private final ArchiveStartedNotificationEncoder archiveStartedNotificationEncoder =
        new ArchiveStartedNotificationEncoder();
    private final ArchiveProgressNotificationEncoder archiveProgressNotificationEncoder =
        new ArchiveProgressNotificationEncoder();
    private final ArchiveStoppedNotificationEncoder archiveStoppedNotificationEncoder =
        new ArchiveStoppedNotificationEncoder();

    // TODO: arguably this is a good fit for a linked array queue so that we can have minimal footprint
    // TODO: this makes for awkward construction as we need Aeron setup before the archiver.
    // TODO: The image listener would be easier to setup on the subscription level
    private final ManyToOneConcurrentArrayQueue<Image> imageNotifications;
    private final File archiveFolder;
    private final IdleStrategy idleStrategy;
    private final EpochClock epochClock;

    private final Consumer<Image> handleNewImageNotification = this::handleNewImageNotification;

    ArchiverConductor(
        final Aeron aeron,
        final ManyToOneConcurrentArrayQueue<Image> imageNotifications,
        final Archiver.Context ctx)
    {
        this.aeron = aeron;
        this.imageNotifications = imageNotifications;
        serviceRequests = aeron.addSubscription(ctx.serviceRequestChannel(), ctx.serviceRequestStreamId());
        archiverNotifications = aeron.addPublication(
            ctx.archiverNotificationsChannel(), ctx.archiverNotificationsStreamId());
        this.archiveFolder = ctx.archiveFolder();
        this.idleStrategy = ctx.idleStrategy();
        this.epochClock = ctx.epochClock();

        archiveIndex = new ArchiveIndex(archiveFolder);

        outboundHeaderEncoder.wrap(outboundBuffer, 0);
        responseEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        archiveStartedNotificationEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        archiveProgressNotificationEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        archiveStoppedNotificationEncoder.wrap(outboundBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
    }

    public String roleName()
    {
        return "ArchiverConductor";
    }

    public int doWork() throws Exception
    {
        int workDone = 0;

        // TODO: control tasks balance? shard/distribute tasks across threads?
        workDone += imageNotifications.drain(handleNewImageNotification);
        workDone += doServiceRequestsWork();
        workDone += doSessionsWork();

        return workDone;
    }

    public void onClose()
    {
        for (final Session session : sessions)
        {
            session.abort();
        }
        doSessionsWork();

        if (!sessions.isEmpty())
        {
            System.err.println("ERROR: expected empty sessions");
        }

        if (!image2ReplaySession.isEmpty())
        {
            System.err.println("ERROR: expected empty image2ReplaySession");
        }

        if (!instance2ArchivingSession.isEmpty())
        {
            System.err.println("ERROR: expected empty instance2ArchivingSession");
        }

        for (final Subscription subscription : archiveSubscriptions)
        {
            subscription.close();
        }
        archiveSubscriptions.clear();
        imageNotifications.clear();

        archiverNotifications.close();
        serviceRequests.close();
        CloseHelper.quietClose(archiveIndex);
    }

    private void handleNewImageNotification(final Image image)
    {
        if (archiveSubscriptions.contains(image.subscription()))
        {
            final ImageArchivingSession session = new ImageArchivingSession(this, image, this.epochClock);
            sessions.add(session);
            instance2ArchivingSession.put(session.streamInstanceId(), session);
        }
    }

    private int doServiceRequestsWork()
    {
        return serviceRequests.poll((buffer, offset, length, header) ->
        {
            headerDecoder.wrap(buffer, offset);
            final int templateId = headerDecoder.templateId();

            // TODO: handle message versions

            switch (templateId)
            {
                case ReplayRequestDecoder.TEMPLATE_ID:
                    onReplayRequest(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH, header);
                    break;

                case ArchiveStartRequestDecoder.TEMPLATE_ID:
                    onArchiveStartRequest(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH);
                    break;

                case ArchiveStopRequestDecoder.TEMPLATE_ID:
                    onArchiveStopRequest(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH);
                    break;

                case AbortReplayRequestDecoder.TEMPLATE_ID:
                    onAbortReplay(header);
                    break;

                case ListStreamInstancesRequestDecoder.TEMPLATE_ID:
                    onListStreamInstances(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH);
                    break;

                default:
                    throw new IllegalArgumentException("Unexpected template id:" + templateId);
            }
        }, 16);
    }

    private void onListStreamInstances(final DirectBuffer buffer, final int offset)
    {
        requestDecoder.wrap(
                buffer,
                offset,
                headerDecoder.blockLength(),
                headerDecoder.version());
        final int from = requestDecoder.from();
        final int to = requestDecoder.to();
        final String channel = requestDecoder.replyChannel();
        final Publication reply = aeron.addPublication(channel, requestDecoder.replyStreamId());
        final Session listSession =
            new ListDescriptorsSession(reply, from, to, archiveIndex, instance2ArchivingSession);

        sessions.add(listSession);
    }

    private void onAbortReplay(final Header header)
    {
        // TODO: replace with correlation id
        final ReplaySession session = image2ReplaySession.get(header.sessionId());
        if (session == null)
        {
            throw new IllegalStateException("Trying to abort an unknown replay session:" + header.sessionId());
        }
        session.abort();
    }

    private void onReplayRequest(final DirectBuffer buffer, final int offset, final Header header)
    {
        // validate image single use
        if (image2ReplaySession.containsKey(header.sessionId()))
        {
            throw new IllegalStateException(
                "Trying to request a second replay from same session:" + header.sessionId());
        }

        final Image image = serviceRequests.imageBySessionId(header.sessionId());

        replayRequestDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

        final int replyStreamId = replayRequestDecoder.replyStreamId();
        final String replyChannel = replayRequestDecoder.replyChannel();

        // TODO: need to control construction of publications to handle errors
        final Publication replyPublication = aeron.addPublication(replyChannel, replyStreamId);


        final ReplaySession replaySession = new ReplaySession(
            replayRequestDecoder.streamInstanceId(),
            replayRequestDecoder.termId(),
            replayRequestDecoder.termOffset(),
            replayRequestDecoder.length(),
            replyPublication,
            image,
            this);

        image2ReplaySession.put(header.sessionId(), replaySession);
        sessions.add(replaySession);
    }

    private void onArchiveStartRequest(final DirectBuffer buffer, final int offset)
    {
        archiveStartRequestDecoder.wrap(
            buffer,
            offset,
            headerDecoder.blockLength(),
            headerDecoder.version());

        final String channel = archiveStartRequestDecoder.channel();
        final int streamId = archiveStartRequestDecoder.streamId();

        final Subscription archiveSubscription = aeron.addSubscription(channel, streamId);

        // as subscription images are created they will get picked up and archived
        archiveSubscriptions.add(archiveSubscription);
    }

    private void onArchiveStopRequest(final DirectBuffer buffer, final int offset)
    {
        archiveStopRequestDecoder.wrap(
            buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

        final String channel = archiveStopRequestDecoder.channel();
        final int streamId = archiveStopRequestDecoder.streamId();

        for (final Subscription archiveSubscription : archiveSubscriptions)
        {
            if (archiveSubscription.streamId() == streamId &&
                archiveSubscription.channel().equals(channel))
            {
                archiveSubscription.close();
                archiveSubscriptions.remove(archiveSubscription);
                // image archiving sessions will sort themselves out naturally
            }
        }
    }

    private int doSessionsWork()
    {
        int workDone = 0;
        final ArrayList<Session> sessions = this.sessions;
        for (int lastIndex = sessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Session session = sessions.get(i);
            workDone += session.doWork();
            if (session.isDone())
            {
                ArrayListUtil.fastUnorderedRemove(sessions, i, lastIndex);
                lastIndex--;
            }
        }

        return workDone;
    }

    void sendResponse(final Publication responsePublication, final String err)
    {
        outboundHeaderEncoder
            .blockLength(ArchiverResponseEncoder.BLOCK_LENGTH)
            .templateId(ArchiverResponseEncoder.TEMPLATE_ID)
            .schemaId(ArchiverResponseEncoder.SCHEMA_ID)
            .version(ArchiverResponseEncoder.SCHEMA_VERSION);

        // reset encoder limit is required for varible length messages
        responseEncoder.limit(MessageHeaderEncoder.ENCODED_LENGTH + ArchiverResponseEncoder.BLOCK_LENGTH);
        if (!Strings.isEmpty(err))
        {
            responseEncoder.err(err);
        }


        final int length = MessageHeaderEncoder.ENCODED_LENGTH + responseEncoder.encodedLength();
        while (true)
        {
            final long result = responsePublication.offer(outboundBuffer, 0, length);
            if (result > 0)
            {
                idleStrategy.reset();
                break;
            }
            if (result == Publication.NOT_CONNECTED || result == Publication.CLOSED)
            {
                throw new IllegalStateException("Response channel is down: " + responsePublication);
            }
            idleStrategy.idle();
        }
    }

    File archiveFolder()
    {
        return archiveFolder;
    }

    int notifyArchiveStarted(
        final String source,
        final int sessionId,
        final String channel,
        final int streamId,
        final int termBufferLength,
        final int imageInitialTermId)
    {
        final int instanceId =
            archiveIndex.addNewStreamInstance(
                new StreamInstance(source, sessionId, channel, streamId),
                termBufferLength,
                imageInitialTermId);

        outboundHeaderEncoder
            .blockLength(ArchiveStartedNotificationEncoder.BLOCK_LENGTH)
            .templateId(ArchiveStartedNotificationEncoder.TEMPLATE_ID)
            .schemaId(ArchiveStartedNotificationEncoder.SCHEMA_ID)
            .version(ArchiveStartedNotificationEncoder.SCHEMA_VERSION);

        // reset encoder limit is required for variable length messages
        responseEncoder.limit(MessageHeaderEncoder.ENCODED_LENGTH + ArchiveStartedNotificationEncoder.BLOCK_LENGTH);
        archiveStartedNotificationEncoder
            .streamInstanceId(instanceId)
            .sessionId(sessionId)
            .streamId(streamId)
            .source(source)
            .channel(channel);


        sendNotification(archiveStartedNotificationEncoder.encodedLength());
        return instanceId;
    }

    void notifyArchiveProgress(
        final int instanceId,
        final int initialTermId,
        final int initialTermOffset,
        final int termId,
        final int endTermOffset)
    {
        outboundHeaderEncoder
            .blockLength(ArchiveProgressNotificationEncoder.BLOCK_LENGTH)
            .templateId(ArchiveProgressNotificationEncoder.TEMPLATE_ID)
            .schemaId(ArchiveProgressNotificationEncoder.SCHEMA_ID)
            .version(ArchiveProgressNotificationEncoder.SCHEMA_VERSION);

        archiveProgressNotificationEncoder
            .streamInstanceId(instanceId)
            .initialTermId(initialTermId)
            .initialTermOffset(initialTermOffset)
            .termId(termId)
            .termOffset(endTermOffset);

        sendNotification(archiveProgressNotificationEncoder.encodedLength());
    }

    void notifyArchiveStopped(final int instanceId)
    {
        outboundHeaderEncoder
            .blockLength(ArchiveStoppedNotificationEncoder.BLOCK_LENGTH)
            .templateId(ArchiveStoppedNotificationEncoder.TEMPLATE_ID)
            .schemaId(ArchiveStoppedNotificationEncoder.SCHEMA_ID)
            .version(ArchiveStoppedNotificationEncoder.SCHEMA_VERSION);

        archiveStoppedNotificationEncoder.streamInstanceId(instanceId);
        sendNotification(archiveStoppedNotificationEncoder.encodedLength());
    }

    private void sendNotification(final int length)
    {
        final Publication publication = this.archiverNotifications;
        while (true)
        {
            final long result = publication.offer(
                outboundBuffer, 0, MessageHeaderEncoder.ENCODED_LENGTH + length);
            if (result > 0 || result == Publication.NOT_CONNECTED)
            {
                idleStrategy.reset();
                break;
            }
            else if (result == Publication.CLOSED)
            {
                throw new IllegalStateException();
            }
            idleStrategy.idle();
        }
    }

    void removeArchivingSession(final int streamInstanceId)
    {
        instance2ArchivingSession.remove(streamInstanceId);
    }

    void removeReplaySession(final int sessionId)
    {
        image2ReplaySession.remove(sessionId);
    }

    void updateIndexFromMeta(final int streamInstanceId, final ByteBuffer byteBuffer) throws IOException
    {
        archiveIndex.updateIndexFromMeta(streamInstanceId, byteBuffer);
    }


}
