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

import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.*;
import org.agrona.DirectBuffer;

class ControlRequestAdapter implements FragmentHandler
{
    private final ControlRequestListener listener;
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final ConnectRequestDecoder connectRequestDecoder = new ConnectRequestDecoder();
    private final CloseSessionRequestDecoder closeSessionRequestDecoder = new CloseSessionRequestDecoder();
    private final StartRecordingRequestDecoder startRecordingRequestDecoder = new StartRecordingRequestDecoder();
    private final StopRecordingRequestDecoder stopRecordingRequestDecoder = new StopRecordingRequestDecoder();
    private final ReplayRequestDecoder replayRequestDecoder = new ReplayRequestDecoder();
    private final ListRecordingsRequestDecoder listRecordingsRequestDecoder = new ListRecordingsRequestDecoder();
    private final ListRecordingsForUriRequestDecoder listRecordingsForUriRequestDecoder =
        new ListRecordingsForUriRequestDecoder();

    ControlRequestAdapter(final ControlRequestListener listener)
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
                    connectRequestDecoder.correlationId(),
                    connectRequestDecoder.responseChannel(),
                    connectRequestDecoder.responseStreamId());
                break;

            case CloseSessionRequestDecoder.TEMPLATE_ID:
                closeSessionRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onCloseSession(closeSessionRequestDecoder.controlSessionId());
                break;

            case StartRecordingRequestDecoder.TEMPLATE_ID:
                startRecordingRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onStartRecording(
                    startRecordingRequestDecoder.controlSessionId(),
                    startRecordingRequestDecoder.correlationId(),
                    startRecordingRequestDecoder.channel(),
                    startRecordingRequestDecoder.streamId(),
                    startRecordingRequestDecoder.sourceLocation());
                break;

            case StopRecordingRequestDecoder.TEMPLATE_ID:
                stopRecordingRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onStopRecording(
                    stopRecordingRequestDecoder.controlSessionId(),
                    stopRecordingRequestDecoder.correlationId(),
                    stopRecordingRequestDecoder.channel(),
                    stopRecordingRequestDecoder.streamId()
                );
                break;

            case ReplayRequestDecoder.TEMPLATE_ID:
                replayRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onStartReplay(
                    replayRequestDecoder.controlSessionId(),
                    replayRequestDecoder.correlationId(),
                    replayRequestDecoder.replayStreamId(),
                    replayRequestDecoder.replayChannel(),
                    replayRequestDecoder.recordingId(),
                    replayRequestDecoder.position(),
                    replayRequestDecoder.length());
                break;

            case ListRecordingsRequestDecoder.TEMPLATE_ID:
                listRecordingsRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onListRecordings(
                    listRecordingsRequestDecoder.controlSessionId(),
                    listRecordingsRequestDecoder.correlationId(),
                    listRecordingsRequestDecoder.fromRecordingId(),
                    listRecordingsRequestDecoder.recordCount());
                break;

            case ListRecordingsForUriRequestDecoder.TEMPLATE_ID:
                listRecordingsForUriRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onListRecordingsForUri(
                    listRecordingsForUriRequestDecoder.controlSessionId(),
                    listRecordingsForUriRequestDecoder.correlationId(),
                    listRecordingsForUriRequestDecoder.fromRecordingId(),
                    listRecordingsForUriRequestDecoder.recordCount(),
                    listRecordingsForUriRequestDecoder.channel(),
                    listRecordingsForUriRequestDecoder.streamId());
                break;

            default:
                throw new IllegalArgumentException("Unexpected template id:" + templateId);
        }
    }
}
