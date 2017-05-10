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

import io.aeron.archiver.codecs.*;
import io.aeron.logbuffer.*;
import org.agrona.DirectBuffer;

class ArchiveRequestAdapter implements FragmentHandler
{
    private final ArchiveRequestListener listener;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final AbortReplayRequestDecoder abortReplayRequestDecoder = new AbortReplayRequestDecoder();
    private final StartRecordingRequestDecoder startRecordingRequestDecoder = new StartRecordingRequestDecoder();
    private final StopRecordingRequestDecoder stopRecordingRequestDecoder = new StopRecordingRequestDecoder();
    private final ListRecordingsRequestDecoder listRecordingsRequestDecoder = new ListRecordingsRequestDecoder();
    private final ConnectRequestDecoder connectRequestDecoder = new ConnectRequestDecoder();

    ArchiveRequestAdapter(final ArchiveRequestListener listener)
    {
        this.listener = listener;
    }

    @SuppressWarnings("MethodLength")
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();

        switch (templateId)
        {
            case ConnectRequestDecoder.TEMPLATE_ID:
                connectRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onConnect(
                    connectRequestDecoder.responseChannel(),
                    connectRequestDecoder.responseStreamId()
                );
                break;

            case ReplayRequestDecoder.TEMPLATE_ID:
                replayRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onStartReplay(
                    replayRequestDecoder.correlationId(),
                    replayRequestDecoder.replayStreamId(),
                    replayRequestDecoder.replayChannel(),
                    replayRequestDecoder.recordingId(),
                    replayRequestDecoder.termId(),
                    replayRequestDecoder.termOffset(),
                    replayRequestDecoder.length());
                break;

            case StartRecordingRequestDecoder.TEMPLATE_ID:
                startRecordingRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onStartRecording(
                    startRecordingRequestDecoder.correlationId(),
                    startRecordingRequestDecoder.channel(),
                    startRecordingRequestDecoder.streamId());
                break;

            case StopRecordingRequestDecoder.TEMPLATE_ID:
                stopRecordingRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onStopRecording(
                    stopRecordingRequestDecoder.correlationId(),
                    stopRecordingRequestDecoder.channel(),
                    stopRecordingRequestDecoder.streamId());
                break;

            case AbortReplayRequestDecoder.TEMPLATE_ID:
                abortReplayRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onAbortReplay(
                    abortReplayRequestDecoder.correlationId());
                break;

            case ListRecordingsRequestDecoder.TEMPLATE_ID:
                listRecordingsRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onListRecordings(
                    listRecordingsRequestDecoder.correlationId(),
                    listRecordingsRequestDecoder.fromId(),
                    listRecordingsRequestDecoder.toId());
                break;

            default:
                throw new IllegalArgumentException("Unexpected template id:" + templateId);
        }
    }
}
