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
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.Strings;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.ObjectHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderEncoder;

import java.io.File;
import java.util.ArrayList;

import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

public class ArchiverConductor implements Agent
{
    private final Aeron aeron;
    private final Subscription serviceRequests;
    private final Publication archiverNotifications;

    private final ArrayList<ReplaySession> replaySessions = new ArrayList<>();
    private final Int2ObjectHashMap<ReplaySession> image2ReplaySession = new Int2ObjectHashMap<>();

    private final ArrayList<ImageArchivingSession> archivingSessions = new ArrayList<>();
    private final Int2ObjectHashMap<ImageArchivingSession> image2ArchivingSession = new Int2ObjectHashMap<>();

    private final ObjectHashSet<Subscription> archiveSubscriptions = new ObjectHashSet<>(128);

    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final ArchiveStartRequestDecoder archiveStartRequestDecoder = new ArchiveStartRequestDecoder();
    private final ArchiveStopRequestDecoder archiveStopRequestDecoder = new ArchiveStopRequestDecoder();

    private final UnsafeBuffer responseBuffer =
            new UnsafeBuffer(BufferUtil.allocateDirectAligned(4096, CACHE_LINE_LENGTH));
    private final MessageHeaderEncoder responseHeaderEncoder = new MessageHeaderEncoder();

    // TODO: arguably this is a good fit for a linked array queue so that we can have minimal footprint
    // TODO: this makes for awkward construction as we need Aeron setup before the archiver. The image listener would be easier
    // to setup on the subscription level
    private final ManyToOneConcurrentArrayQueue<Image> imageNotifications;
    private final File archiveFolder;

    public ArchiverConductor(Aeron aeron, ManyToOneConcurrentArrayQueue<Image> imageNotifications,
                             Archiver.Context ctx)
    {
        this.aeron = aeron;
        this.imageNotifications = imageNotifications;
        serviceRequests = aeron.addSubscription(ctx.serviceRequestChannel(), ctx.serviceRequestStreamId());
        archiverNotifications = aeron.addPublication(ctx.archiverNotificationsChannel(), ctx.archiverNotificationsStreamId());
        this.archiveFolder = ctx.archiveFolder();
    }

    public String roleName()
    {
        return "ArchiverConductor";
    }

    public int doWork() throws Exception
    {
        int workDone = 0;

        workDone += imageNotifications.drain(image -> handleNewImageNotification(image));
        workDone += doServiceRequestsWork();
        workDone += doReplaySessionsWork();
        workDone += doArchivingSessionsWork();

        return workDone;
    }

    public void onClose()
    {
        for (ReplaySession session : replaySessions)
        {
            session.close();
        }
        doReplaySessionsWork();

        if (!replaySessions.isEmpty())
        {
            System.err.println("ERROR: expected empty replaySessions");
        }
        if (!image2ReplaySession.isEmpty())
        {
            System.err.println("ERROR: expected empty image2ReplaySession");
        }
        for (ImageArchivingSession session : archivingSessions)
        {
            session.close();
        }
        doArchivingSessionsWork();
        if (!archivingSessions.isEmpty())
        {
            System.err.println("ERROR: expected empty archivingSessions");
        }
        if (!image2ArchivingSession.isEmpty())
        {
            System.err.println("ERROR: expected empty image2ArchivingSession");
        }

        for (Subscription subscription : archiveSubscriptions)
        {
            subscription.close();
        }
        archiveSubscriptions.clear();
        imageNotifications.clear();

        archiverNotifications.close();
        serviceRequests.close();
    }

    private void handleNewImageNotification(Image image)
    {
        if (archiveSubscriptions.contains(image.subscription()))
        {
            final ImageArchivingSession session = new ImageArchivingSession(this, image);
            archivingSessions.add(session);
            image2ArchivingSession.put(image.sessionId(), session);
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
                {
                    onReplayRequest(buffer, offset  + MessageHeaderDecoder.ENCODED_LENGTH, header);
                    break;
                }
                case ArchiveStartRequestDecoder.TEMPLATE_ID:
                {
                    onArchiveStartRequest(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH);
                    break;
                }
                case ArchiveStopRequestDecoder.TEMPLATE_ID:
                {
                    onArchiveStopRequest(buffer, offset + MessageHeaderDecoder.ENCODED_LENGTH);
                    break;
                }
                case AbortReplayRequestDecoder.TEMPLATE_ID:
                {
                    onAbortReplay(header);
                    break;
                }

                default:
                    //err
            }
        }, 16);
    }

    private void onAbortReplay(Header header)
    {
        final ReplaySession session = image2ReplaySession.get(header.sessionId());
        if (session == null)
        {
            throw new IllegalArgumentException("bugger this for a lark");
        }
        session.close();
    }

    void onReplayRequest(DirectBuffer buffer, int offset, Header header)
    {

        // validate image single use
        if (image2ReplaySession.containsKey(header.sessionId()))
        {
            throw new IllegalArgumentException("bugger this for a lark");
        }

        final Image image = serviceRequests.imageBySessionId(header.sessionId());

        replayRequestDecoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

        final StreamInstance streamInstance =
                new StreamInstance(replayRequestDecoder.source(), replayRequestDecoder.sessionId(),
                                   replayRequestDecoder.channel(), replayRequestDecoder.streamId());

        final int replayStreamId  = replayRequestDecoder.replayStreamId();
        final int controlStreamId = replayRequestDecoder.controlStreamId();
        final String replyChannel = replayRequestDecoder.replyChannel();


        // create requested publications to replay/respond at
        /* TODO: need a hook for requesting a special ReplayPublication from driver, need non-public way to expose
                 driver side capability */
        // TODO: need to control construction of publications to handle errors
        final Publication replayPublication  = aeron.addPublication(replyChannel, replayStreamId);
        final Publication controlPublication = aeron.addPublication(replyChannel, controlStreamId);


        final ReplaySession replaySession = new ReplaySession(
                streamInstance,
                replayRequestDecoder.termId(),
                replayRequestDecoder.termOffset(), (long) replayRequestDecoder.length(),
                replayPublication, controlPublication, image, this);

        image2ReplaySession.put(header.sessionId(), replaySession);
        replaySessions.add(replaySession);
    }


    private void onArchiveStartRequest(DirectBuffer buffer, int offset)
    {
        archiveStartRequestDecoder.wrap(buffer, offset,
                                        headerDecoder.blockLength(),
                                        headerDecoder.version());

        final String channel = archiveStartRequestDecoder.channel();
        final int streamId = archiveStartRequestDecoder.streamId();

        final Subscription archiveSubscription = aeron.addSubscription(channel, streamId);

        // as subscription images are created they will get picked up and archived
        archiveSubscriptions.add(archiveSubscription);
    }

    private void onArchiveStopRequest(DirectBuffer buffer, int offset)
    {
        archiveStopRequestDecoder.wrap(buffer, offset, headerDecoder.blockLength(),
                                       headerDecoder.version());

        final String channel = archiveStopRequestDecoder.channel();
        final int streamId = archiveStopRequestDecoder.streamId();

        for (Subscription archiveSubscription : archiveSubscriptions)
        {
            if (archiveSubscription.streamId() == streamId &&
                archiveSubscription.channel().equals(channel))
            {
                archiveSubscription.close();
                archiveSubscriptions.remove(archiveSubscription);
                // image archiving will sort itself out
            }
        }
    }

    private int doReplaySessionsWork()
    {
        int workDone = 0;
        final ArrayList<ReplaySession> replaySessions = this.replaySessions;
        for (int lastIndex = replaySessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ReplaySession session = replaySessions.get(i);
            workDone += session.doWork();
            if (session.state() == ReplaySession.State.DONE)
            {
                image2ReplaySession.remove(session.image().sessionId());
                ArrayListUtil.fastUnorderedRemove(replaySessions, i, lastIndex);
                lastIndex--;
            }
        }
        return workDone;
    }

    private int doArchivingSessionsWork()
    {
        int workDone = 0;
        final ArrayList<ImageArchivingSession> archivingSessions = this.archivingSessions;
        for (int lastIndex = archivingSessions.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final ImageArchivingSession session = archivingSessions.get(i);
            workDone += session.doWork();
            if (session.state() == ImageArchivingSession.State.DONE)
            {
                image2ArchivingSession.remove(session.image().sessionId());
                ArrayListUtil.fastUnorderedRemove(archivingSessions, i, lastIndex);
                lastIndex--;
            }
        }
        return workDone;
    }

    void sendResponse(Publication control, String err)
    {
        final ArchiverResponseEncoder responseEncoder = new ArchiverResponseEncoder();
        responseHeaderEncoder.wrap(responseBuffer, 0).
                blockLength(ArchiverResponseEncoder.BLOCK_LENGTH).
                             templateId(ArchiverResponseEncoder.TEMPLATE_ID).
                             schemaId(ArchiverResponseEncoder.SCHEMA_ID).
                             version(ArchiverResponseEncoder.SCHEMA_VERSION);

        if (!Strings.isEmpty(err))
        {
            responseEncoder.wrap(responseBuffer, MessageHeaderEncoder.ENCODED_LENGTH).
                    err(err);
        }


        long offer;
        do
        {
            offer = control.offer(responseBuffer, 0,
                                  MessageHeaderEncoder.ENCODED_LENGTH + responseEncoder.encodedLength());
            if (offer == Publication.NOT_CONNECTED || offer == Publication.CLOSED)
            {
                // TODO: backoff? fail nicely?
                throw new IllegalStateException();
            }
        }
        while (offer < 0);
    }

    File archiveFolder()
    {
        return archiveFolder;
    }

    int notifyArchiveStarted(String source, int sessionId, String channel, int streamId)
    {
        // TODO: persistent index for instances, as well as an index file.
        final int instanceId = 0;
        final ArchiveStartedNotificationEncoder mEncoder = new ArchiveStartedNotificationEncoder();
        responseHeaderEncoder.wrap(responseBuffer, 0).
                blockLength(ArchiveStartedNotificationEncoder.BLOCK_LENGTH).
                             templateId(ArchiveStartedNotificationEncoder.TEMPLATE_ID).
                             schemaId(ArchiveStartedNotificationEncoder.SCHEMA_ID).
                             version(ArchiveStartedNotificationEncoder.SCHEMA_VERSION);
        mEncoder.wrap(responseBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        mEncoder.streamInstanceId(instanceId).sessionId(sessionId).streamId(streamId).channel(channel).source(source);


        final long result = archiverNotifications.offer(responseBuffer, 0,
                                                        MessageHeaderEncoder.ENCODED_LENGTH + mEncoder.encodedLength());
        if (result > 0 || result == Publication.NOT_CONNECTED)
        {
            return instanceId;
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    void notifyArchiveProgress(int instanceId, int initialTermId, int initialTermOffset, int termId, int endTermOffset)
    {
        final ArchiveProgressNotificationEncoder mEncoder = new ArchiveProgressNotificationEncoder();
        responseHeaderEncoder.wrap(responseBuffer, 0).
                blockLength(ArchiveProgressNotificationEncoder.BLOCK_LENGTH).
                templateId(ArchiveProgressNotificationEncoder.TEMPLATE_ID).
                schemaId(ArchiveProgressNotificationEncoder.SCHEMA_ID).
                version(ArchiveProgressNotificationEncoder.SCHEMA_VERSION);
        mEncoder.wrap(responseBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        mEncoder.streamInstanceId(instanceId).
                initialTermId(initialTermId).
                initialTermOffset(initialTermOffset).
                termId(termId).
                termOffset(endTermOffset);

        final long result = archiverNotifications.offer(responseBuffer, 0,
                                                        MessageHeaderEncoder.ENCODED_LENGTH + mEncoder.encodedLength());
        if (result < 0 && result != Publication.NOT_CONNECTED)
        {
            throw new IllegalStateException();
        }
    }

    void notifyArchiveStopped(int instanceId)
    {
        final ArchiveStoppedNotificationEncoder mEncoder = new ArchiveStoppedNotificationEncoder();
        responseHeaderEncoder.wrap(responseBuffer, 0).
                blockLength(ArchiveStoppedNotificationEncoder.BLOCK_LENGTH).
                templateId(ArchiveStoppedNotificationEncoder.TEMPLATE_ID).
                schemaId(ArchiveStoppedNotificationEncoder.SCHEMA_ID).
                version(ArchiveStoppedNotificationEncoder.SCHEMA_VERSION);
        mEncoder.wrap(responseBuffer, MessageHeaderEncoder.ENCODED_LENGTH);
        mEncoder.streamInstanceId(instanceId);

        final long result = archiverNotifications.offer(responseBuffer, 0,
                                                        MessageHeaderEncoder.ENCODED_LENGTH + mEncoder.encodedLength());
        if (result < 0 && result != Publication.NOT_CONNECTED)
        {
            throw new IllegalStateException();
        }
    }

    public int findStreamInstanceId(StreamInstance streamInstance)
    {
        return 0;
    }
}
