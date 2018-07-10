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
    private final StopReplayRequestDecoder stopReplayRequestDecoder = new StopReplayRequestDecoder();
    private final ListRecordingsRequestDecoder listRecordingsRequestDecoder = new ListRecordingsRequestDecoder();
    private final ListRecordingsForUriRequestDecoder listRecordingsForUriRequestDecoder =
        new ListRecordingsForUriRequestDecoder();
    private final ListRecordingRequestDecoder listRecordingRequestDecoder = new ListRecordingRequestDecoder();
    private final ExtendRecordingRequestDecoder extendRecordingRequestDecoder = new ExtendRecordingRequestDecoder();
    private final RecordingPositionRequestDecoder recordingPositionRequestDecoder =
        new RecordingPositionRequestDecoder();
    private final TruncateRecordingRequestDecoder truncateRecordingRequestDecoder =
        new TruncateRecordingRequestDecoder();
    private final StopRecordingSubscriptionRequestDecoder stopRecordingSubscriptionRequestDecoder =
        new StopRecordingSubscriptionRequestDecoder();

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
                    startRecordingRequestDecoder.streamId(),
                    startRecordingRequestDecoder.channel(),
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
                    stopRecordingRequestDecoder.streamId(),
                    stopRecordingRequestDecoder.channel());
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
                    replayRequestDecoder.recordingId(),
                    replayRequestDecoder.position(),
                    replayRequestDecoder.length(),
                    replayRequestDecoder.replayStreamId(),
                    replayRequestDecoder.replayChannel());
                break;

            case StopReplayRequestDecoder.TEMPLATE_ID:
                stopReplayRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onStopReplay(
                    stopReplayRequestDecoder.controlSessionId(),
                    stopReplayRequestDecoder.correlationId(),
                    stopReplayRequestDecoder.replaySessionId());
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
                    listRecordingsForUriRequestDecoder.streamId(),
                    listRecordingsForUriRequestDecoder.channel());
                break;

            case ListRecordingRequestDecoder.TEMPLATE_ID:
                listRecordingRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onListRecording(
                    listRecordingRequestDecoder.controlSessionId(),
                    listRecordingRequestDecoder.correlationId(),
                    listRecordingRequestDecoder.recordingId());
                break;

            case ExtendRecordingRequestDecoder.TEMPLATE_ID:
                extendRecordingRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onExtendRecording(
                    extendRecordingRequestDecoder.controlSessionId(),
                    extendRecordingRequestDecoder.correlationId(),
                    extendRecordingRequestDecoder.recordingId(),
                    extendRecordingRequestDecoder.streamId(),
                    extendRecordingRequestDecoder.channel(),
                    extendRecordingRequestDecoder.sourceLocation());
                break;

            case RecordingPositionRequestDecoder.TEMPLATE_ID:
                recordingPositionRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onGetRecordingPosition(
                    recordingPositionRequestDecoder.controlSessionId(),
                    recordingPositionRequestDecoder.correlationId(),
                    recordingPositionRequestDecoder.recordingId());
                break;

            case TruncateRecordingRequestDecoder.TEMPLATE_ID:
                truncateRecordingRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onTruncateRecording(
                    truncateRecordingRequestDecoder.controlSessionId(),
                    truncateRecordingRequestDecoder.correlationId(),
                    truncateRecordingRequestDecoder.recordingId(),
                    truncateRecordingRequestDecoder.position());
                break;

            case StopRecordingSubscriptionRequestDecoder.TEMPLATE_ID:
                stopRecordingSubscriptionRequestDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    headerDecoder.blockLength(),
                    headerDecoder.version());

                listener.onStopRecordingSubscription(
                    stopRecordingSubscriptionRequestDecoder.controlSessionId(),
                    stopRecordingSubscriptionRequestDecoder.correlationId(),
                    stopRecordingSubscriptionRequestDecoder.subscriptionId());
                break;

            default:
                throw new IllegalArgumentException("unexpected template id:" + templateId);
        }
    }
}
