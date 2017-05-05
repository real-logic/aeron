/*
 * Copyright 2014 - 2017 Real Logic Ltd.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.aeron.archiver;

import io.aeron.*;
import io.aeron.archiver.codecs.*;
import org.agrona.*;

public class ArchiveClient
{
    public static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    private final Publication archiveServiceRequest;
    private final Subscription recordingEvents;
    private final MessageHeaderEncoder messageHeaderEncoder;
    private final ConnectRequestEncoder connectRequestEncoder;
    private final StartRecordingRequestEncoder startRecordingRequestEncoder;
    private final ReplayRequestEncoder replayRequestEncoder;
    private final StopRecordingRequestEncoder stopRecordingRequestEncoder;
    private final ListRecordingsRequestEncoder listRecordingsRequestEncoder;
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final RecordingStartedDecoder recordingStartedDecoder = new RecordingStartedDecoder();
    private final RecordingProgressDecoder recordingProgressDecoder = new RecordingProgressDecoder();
    private final RecordingStoppedDecoder recordingStoppedDecoder = new RecordingStoppedDecoder();

    public ArchiveClient(
        final Publication archiveServiceRequest,
        final Subscription recordingEvents)
    {
        this.archiveServiceRequest = archiveServiceRequest;
        this.recordingEvents = recordingEvents;

        messageHeaderEncoder = new MessageHeaderEncoder().wrap(buffer, 0);
        connectRequestEncoder = new ConnectRequestEncoder().wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH);
        startRecordingRequestEncoder = new StartRecordingRequestEncoder().wrap(buffer, HEADER_LENGTH);
        replayRequestEncoder = new ReplayRequestEncoder().wrap(buffer, HEADER_LENGTH);
        stopRecordingRequestEncoder = new StopRecordingRequestEncoder().wrap(buffer, HEADER_LENGTH);
        listRecordingsRequestEncoder = new ListRecordingsRequestEncoder()
            .wrap(buffer, MessageHeaderEncoder.ENCODED_LENGTH);
    }

    public boolean connect(
        final String channel,
        final int streamId)
    {
        messageHeaderEncoder
            .templateId(ConnectRequestEncoder.TEMPLATE_ID)
            .blockLength(ConnectRequestEncoder.BLOCK_LENGTH)
            .schemaId(ConnectRequestEncoder.SCHEMA_ID)
            .version(ConnectRequestEncoder.SCHEMA_VERSION);

        connectRequestEncoder.limit(ConnectRequestEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        connectRequestEncoder
            .responseStreamId(streamId)
            .responseChannel(channel);

        return offer(connectRequestEncoder.encodedLength());
    }

    public boolean startRecording(
        final String channel,
        final int streamId)
    {
        messageHeaderEncoder
            .templateId(StartRecordingRequestEncoder.TEMPLATE_ID)
            .blockLength(StartRecordingRequestEncoder.BLOCK_LENGTH)
            .schemaId(StartRecordingRequestEncoder.SCHEMA_ID)
            .version(StartRecordingRequestEncoder.SCHEMA_VERSION);

        startRecordingRequestEncoder.limit(StartRecordingRequestEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        startRecordingRequestEncoder
            .channel(channel)
            .streamId(streamId);

        return offer(startRecordingRequestEncoder.encodedLength());
    }

    public boolean stopRecording(
        final String channel,
        final int streamId)
    {
        messageHeaderEncoder
            .templateId(StopRecordingRequestEncoder.TEMPLATE_ID)
            .blockLength(StopRecordingRequestEncoder.BLOCK_LENGTH)
            .schemaId(StopRecordingRequestEncoder.SCHEMA_ID)
            .version(StopRecordingRequestEncoder.SCHEMA_VERSION);

        startRecordingRequestEncoder.limit(StopRecordingRequestEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        stopRecordingRequestEncoder
            .channel(channel)
            .streamId(streamId);

        return offer(stopRecordingRequestEncoder.encodedLength());
    }

    public boolean replay(
        final int recordingId,
        final int termId,
        final int termOffset,
        final long length,
        final String replayChannel,
        final int replayStreamId)
    {
        messageHeaderEncoder
            .templateId(ReplayRequestEncoder.TEMPLATE_ID)
            .blockLength(ReplayRequestEncoder.BLOCK_LENGTH)
            .schemaId(ReplayRequestEncoder.SCHEMA_ID)
            .version(ReplayRequestEncoder.SCHEMA_VERSION);

        replayRequestEncoder.limit(ReplayRequestEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        replayRequestEncoder
            .recordingId(recordingId)
            .termId(termId)
            .termOffset(termOffset)
            .length(length)
            .replayStreamId(replayStreamId)
            .replayChannel(replayChannel);

        return offer(replayRequestEncoder.encodedLength());
    }

    public boolean listRecordings(
        final int fromId,
        final int toId)
    {
        messageHeaderEncoder
            .templateId(ListRecordingsRequestEncoder.TEMPLATE_ID)
            .blockLength(ListRecordingsRequestEncoder.BLOCK_LENGTH)
            .schemaId(ListRecordingsRequestEncoder.SCHEMA_ID)
            .version(ListRecordingsRequestEncoder.SCHEMA_VERSION);

        listRecordingsRequestEncoder.limit(ListRecordingsRequestEncoder.BLOCK_LENGTH + HEADER_LENGTH);

        listRecordingsRequestEncoder
            .fromId(fromId)
            .toId(toId);

        return offer(listRecordingsRequestEncoder.encodedLength());
    }

    interface RecordingProgressListener
    {
        void onProgress(
            int recordingId,
            int initialTermId,
            int initialTermOffset,
            int termId,
            int termOffset);

        void onStart(
            int recordingId,
            int sessionId,
            int streamId,
            String source,
            String channel);

        void onStop(int recordingId);
    }

    public int pollEvents(final RecordingProgressListener progressListener, final int count)
    {
        return recordingEvents.poll((b, offset, length, header) ->
        {
            messageHeaderDecoder.wrap(b, offset);

            switch (messageHeaderDecoder.templateId())
            {
                case RecordingProgressDecoder.TEMPLATE_ID:
                {
                    recordingProgressDecoder.wrap(
                        b,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version());
                    progressListener.onProgress(
                        recordingProgressDecoder.recordingId(),
                        recordingProgressDecoder.initialTermId(),
                        recordingProgressDecoder.initialTermOffset(),
                        recordingProgressDecoder.termId(),
                        recordingProgressDecoder.termOffset()
                    );
                    break;
                }

                case RecordingStartedDecoder.TEMPLATE_ID:
                {
                    recordingStartedDecoder.wrap(
                        b,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version());

                    progressListener.onStart(
                        recordingStartedDecoder.recordingId(),
                        recordingStartedDecoder.sessionId(),
                        recordingStartedDecoder.streamId(),
                        recordingStartedDecoder.channel(),
                        recordingStartedDecoder.source());
                    break;
                }

                case RecordingStoppedDecoder.TEMPLATE_ID:
                {
                    recordingStoppedDecoder.wrap(
                        b,
                        offset + MessageHeaderDecoder.ENCODED_LENGTH,
                        messageHeaderDecoder.blockLength(),
                        messageHeaderDecoder.version());

                    progressListener.onStop(recordingStoppedDecoder.recordingId());
                    break;
                }

                default:
                    throw new IllegalStateException();
            }
        }, count);
    }

    private boolean offer(final int length)
    {
        final long newPosition = archiveServiceRequest.offer(buffer, 0, length + HEADER_LENGTH);

        return newPosition >= 0;
    }
}
