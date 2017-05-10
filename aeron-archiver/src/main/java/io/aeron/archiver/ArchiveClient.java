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
    interface ResponseListener
    {
        void onResponse(
            String err,
            int correlationId);

        void onReplayStarted(
            int replayId,
            int correlationId);

        void onReplayAborted(
            int lastTermId,
            int lastTermOffset,
            int correlationId);

        void onRecordingStarted(
            int recordingId,
            String source,
            int sessionId,
            String channel,
            int streamId,
            int correlationId);

        void onRecordingStopped(
            int recordingId,
            int lastTermId,
            int lastTermOffset,
            int correlationId);

        void onRecordingDescriptor(
            int recordingId,
            int segmentFileLength,
            int termBufferLength,
            long startTime,
            int initialTermId,
            int initialTermOffset,
            long endTime,
            int lastTermId,
            int lastTermOffset,
            String source,
            int sessionId,
            String channel,
            int streamId,
            int correlationId);

        void onRecordingNotFound(
            int recordingId,
            int maxRecordingId,
            int correlationId);
    }

    interface RecordingEventsListener
    {
        void onProgress(
            int recordingId,
            int initialTermId,
            int initialTermOffset,
            int termId,
            int termOffset);

        void onStart(
            int recordingId,
            String source,
            int sessionId,
            String channel,
            int streamId);

        void onStop(int recordingId);
    }

    private static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;

    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    private final Publication archiveServiceRequest;
    private final Subscription recordingEvents;
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final ConnectRequestEncoder connectRequestEncoder = new ConnectRequestEncoder();
    private final StartRecordingRequestEncoder startRecordingRequestEncoder = new StartRecordingRequestEncoder();
    private final ReplayRequestEncoder replayRequestEncoder = new ReplayRequestEncoder();
    private final AbortReplayRequestEncoder abortReplayRequestEncoder = new AbortReplayRequestEncoder();
    private final StopRecordingRequestEncoder stopRecordingRequestEncoder = new StopRecordingRequestEncoder();
    private final ListRecordingsRequestEncoder listRecordingsRequestEncoder = new ListRecordingsRequestEncoder();

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final RecordingStartedDecoder recordingStartedDecoder = new RecordingStartedDecoder();
    private final RecordingProgressDecoder recordingProgressDecoder = new RecordingProgressDecoder();
    private final RecordingStoppedDecoder recordingStoppedDecoder = new RecordingStoppedDecoder();
    private final ReplayAbortedDecoder replayAbortedDecoder = new ReplayAbortedDecoder();
    private final ReplayStartedDecoder replayStartedDecoder = new ReplayStartedDecoder();
    private final RecordingDescriptorDecoder recordingDescriptorDecoder = new RecordingDescriptorDecoder();
    private final ArchiverResponseDecoder archiverResponseDecoder = new ArchiverResponseDecoder();
    private final RecordingNotFoundResponseDecoder recordingNotFoundResponseDecoder =
        new RecordingNotFoundResponseDecoder();

    public ArchiveClient(
        final Publication archiveServiceRequest,
        final Subscription recordingEvents)
    {
        this.archiveServiceRequest = archiveServiceRequest;
        this.recordingEvents = recordingEvents;
    }

    public boolean connect(
        final String channel,
        final int streamId)
    {
        connectRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .responseStreamId(streamId)
            .responseChannel(channel);

        return offer(connectRequestEncoder.encodedLength());
    }

    public boolean startRecording(
        final String channel,
        final int streamId,
        final int correlationId)
    {
        startRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .channel(channel)
            .streamId(streamId)
            .correlationId(correlationId);

        return offer(startRecordingRequestEncoder.encodedLength());
    }

    public boolean stopRecording(
        final String channel,
        final int streamId,
        final int correlationId)
    {
        stopRecordingRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .channel(channel)
            .streamId(streamId)
            .correlationId(correlationId);

        return offer(stopRecordingRequestEncoder.encodedLength());
    }

    public boolean replay(
        final int recordingId,
        final int termId,
        final int termOffset,
        final long length,
        final String replayChannel,
        final int replayStreamId,
        final int correlationId)
    {
        replayRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .recordingId(recordingId)
            .termId(termId)
            .termOffset(termOffset)
            .length(length)
            .replayStreamId(replayStreamId)
            .replayChannel(replayChannel)
            .correlationId(correlationId);

        return offer(replayRequestEncoder.encodedLength());
    }

    public boolean abortReplay(final int replayId, final int correlationId)
    {
        abortReplayRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .replayId(replayId)
            .correlationId(correlationId);

        return offer(abortReplayRequestEncoder.encodedLength());
    }

    public boolean listRecordings(
        final int fromId,
        final int toId,
        final int correlationId)
    {
        listRecordingsRequestEncoder
            .wrapAndApplyHeader(buffer, 0, messageHeaderEncoder)
            .fromId(fromId)
            .toId(toId)
            .correlationId(correlationId);

        return offer(listRecordingsRequestEncoder.encodedLength());
    }

    public int pollResponses(
        final Subscription reply,
        final ResponseListener responseListener,
        final int count)
    {
        return reply.poll(
            (buffer, offset, length, header) ->
            {
                messageHeaderDecoder.wrap(buffer, offset);

                switch (messageHeaderDecoder.templateId())
                {
                    case ArchiverResponseDecoder.TEMPLATE_ID:
                        handleArchiverResponse(responseListener, buffer, offset);
                        break;

                    case RecordingStartedDecoder.TEMPLATE_ID:
                        handleRecordingStarted(responseListener, buffer, offset);
                        break;

                    case RecordingStoppedDecoder.TEMPLATE_ID:
                        handleRecordingStopped(responseListener, buffer, offset);
                        break;

                    case ReplayAbortedDecoder.TEMPLATE_ID:
                        handleReplayAborted(responseListener, buffer, offset);
                        break;

                    case ReplayStartedDecoder.TEMPLATE_ID:
                        handleReplayStarted(responseListener, buffer, offset);
                        break;

                    case RecordingDescriptorDecoder.TEMPLATE_ID:
                        handleRecordingDescriptor(responseListener, buffer, offset);
                        break;

                    case RecordingNotFoundResponseDecoder.TEMPLATE_ID:
                        handleRecordingNotFoundResponse(responseListener, buffer, offset);
                        break;

                    default:
                        throw new IllegalStateException();
                }
            }, count);
    }

    private void handleRecordingNotFoundResponse(
        final ResponseListener responseListener,
        final DirectBuffer buffer,
        final int offset)
    {
        recordingNotFoundResponseDecoder.wrap(
            buffer,
            offset + HEADER_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        responseListener.onRecordingNotFound(
            recordingNotFoundResponseDecoder.recordingId(),
            recordingNotFoundResponseDecoder.maxRecordingId(),
            recordingNotFoundResponseDecoder.correlationId());
    }

    private void handleArchiverResponse(
        final ResponseListener responseListener,
        final DirectBuffer buffer,
        final int offset)
    {
        archiverResponseDecoder.wrap(
            buffer,
            offset + HEADER_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        responseListener.onResponse(
            archiverResponseDecoder.err(),
            archiverResponseDecoder.correlationId());
    }

    private void handleRecordingStarted(
        final ResponseListener responseListener,
        final DirectBuffer buffer,
        final int offset)
    {
        recordingStartedDecoder.wrap(
            buffer,
            offset + HEADER_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        responseListener.onRecordingStarted(
            recordingStartedDecoder.recordingId(),
            recordingStartedDecoder.channel(), recordingStartedDecoder.sessionId(),
            recordingStartedDecoder.source(), recordingStartedDecoder.streamId(),
            recordingStartedDecoder.correlationId());
    }

    private void handleRecordingStopped(
        final ResponseListener responseListener,
        final DirectBuffer buffer,
        final int offset)
    {
        recordingStoppedDecoder.wrap(
            buffer,
            offset + HEADER_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        responseListener.onRecordingStopped(
            recordingStoppedDecoder.recordingId(),
            recordingStoppedDecoder.lastTermId(),
            recordingStoppedDecoder.lastTermOffset(),
            recordingStoppedDecoder.correlationId());
    }

    private void handleReplayAborted(
        final ResponseListener responseListener,
        final DirectBuffer buffer,
        final int offset)
    {
        replayAbortedDecoder.wrap(
            buffer,
            offset + HEADER_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        responseListener.onReplayAborted(
            replayAbortedDecoder.lastTermId(),
            replayAbortedDecoder.lastTermOffset(),
            replayAbortedDecoder.correlationId());
    }

    private void handleReplayStarted(
        final ResponseListener responseListener,
        final DirectBuffer buffer,
        final int offset)
    {
        replayStartedDecoder.wrap(
            buffer,
            offset + HEADER_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        responseListener.onReplayStarted(
            replayStartedDecoder.replayId(),
            replayStartedDecoder.correlationId());
    }

    private void handleRecordingDescriptor(
        final ResponseListener responseListener,
        final DirectBuffer buffer,
        final int offset)
    {
        recordingDescriptorDecoder.wrap(
            buffer,
            offset + HEADER_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        responseListener.onRecordingDescriptor(
            recordingDescriptorDecoder.recordingId(),
            recordingDescriptorDecoder.segmentFileLength(),
            recordingDescriptorDecoder.termBufferLength(),
            recordingDescriptorDecoder.startTime(),
            recordingDescriptorDecoder.initialTermId(),
            recordingDescriptorDecoder.initialTermOffset(),
            recordingDescriptorDecoder.endTime(),
            recordingDescriptorDecoder.lastTermId(),
            recordingDescriptorDecoder.lastTermOffset(),
            recordingDescriptorDecoder.source(),
            recordingDescriptorDecoder.sessionId(),
            recordingDescriptorDecoder.channel(),
            recordingDescriptorDecoder.streamId(),
            recordingDescriptorDecoder.correlationId()
        );
    }

    public int pollEvents(final RecordingEventsListener progressListener, final int count)
    {
        return recordingEvents.poll(
            (buffer, offset, length, header) ->
            {
                messageHeaderDecoder.wrap(buffer, offset);

                switch (messageHeaderDecoder.templateId())
                {
                    case RecordingProgressDecoder.TEMPLATE_ID:
                        recordingProgressDecoder.wrap(
                            buffer,
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

                    case RecordingStartedDecoder.TEMPLATE_ID:
                        recordingStartedDecoder.wrap(
                            buffer,
                            offset + MessageHeaderDecoder.ENCODED_LENGTH,
                            messageHeaderDecoder.blockLength(),
                            messageHeaderDecoder.version());

                        progressListener.onStart(
                            recordingStartedDecoder.recordingId(),
                            recordingStartedDecoder.channel(), recordingStartedDecoder.sessionId(),
                            recordingStartedDecoder.source(), recordingStartedDecoder.streamId()
                        );
                        break;

                    case RecordingStoppedDecoder.TEMPLATE_ID:
                        recordingStoppedDecoder.wrap(
                            buffer,
                            offset + MessageHeaderDecoder.ENCODED_LENGTH,
                            messageHeaderDecoder.blockLength(),
                            messageHeaderDecoder.version());

                        progressListener.onStop(recordingStoppedDecoder.recordingId());
                        break;

                    default:
                        throw new IllegalStateException();
                }
            }, count);
    }

    private boolean offer(final int length)
    {
        final long newPosition = archiveServiceRequest.offer(buffer, 0, HEADER_LENGTH + length);

        return newPosition >= 0;
    }
}
