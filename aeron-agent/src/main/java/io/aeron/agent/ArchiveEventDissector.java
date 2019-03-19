/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.agent;

import io.aeron.archive.codecs.*;
import org.agrona.MutableDirectBuffer;

final class ArchiveEventDissector
{
    private static final MessageHeaderDecoder HEADER_DECODER = new MessageHeaderDecoder();
    private static final ConnectRequestDecoder CONNECT_REQUEST_DECODER = new ConnectRequestDecoder();
    private static final CloseSessionRequestDecoder CLOSE_SESSION_REQUEST_DECODER = new CloseSessionRequestDecoder();
    private static final StartRecordingRequestDecoder START_RECORDING_REQUEST_DECODER =
        new StartRecordingRequestDecoder();
    private static final StopRecordingRequestDecoder STOP_RECORDING_REQUEST_DECODER = new StopRecordingRequestDecoder();
    private static final ReplayRequestDecoder REPLAY_REQUEST_DECODER = new ReplayRequestDecoder();
    private static final StopReplayRequestDecoder STOP_REPLAY_REQUEST_DECODER = new StopReplayRequestDecoder();
    private static final ListRecordingsRequestDecoder LIST_RECORDINGS_REQUEST_DECODER =
        new ListRecordingsRequestDecoder();
    private static final ListRecordingsForUriRequestDecoder LIST_RECORDINGS_FOR_URI_REQUEST_DECODER =
        new ListRecordingsForUriRequestDecoder();
    private static final ListRecordingRequestDecoder LIST_RECORDING_REQUEST_DECODER = new ListRecordingRequestDecoder();
    private static final ExtendRecordingRequestDecoder EXTEND_RECORDING_REQUEST_DECODER =
        new ExtendRecordingRequestDecoder();
    private static final RecordingPositionRequestDecoder RECORDING_POSITION_REQUEST_DECODER =
        new RecordingPositionRequestDecoder();
    private static final TruncateRecordingRequestDecoder TRUNCATE_RECORDING_REQUEST_DECODER =
        new TruncateRecordingRequestDecoder();
    private static final StopRecordingSubscriptionRequestDecoder STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER =
        new StopRecordingSubscriptionRequestDecoder();
    private static final StopPositionRequestDecoder STOP_POSITION_REQUEST_DECODER = new StopPositionRequestDecoder();
    private static final FindLastMatchingRecordingRequestDecoder FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER =
        new FindLastMatchingRecordingRequestDecoder();
    private static final ListRecordingSubscriptionsRequestDecoder LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER =
        new ListRecordingSubscriptionsRequestDecoder();

    @SuppressWarnings("MethodLength")
    static void controlRequest(
        final ArchiveEventCode event,
        final MutableDirectBuffer buffer,
        final int offset,
        final StringBuilder builder)
    {
        HEADER_DECODER.wrap(buffer, offset);

        switch (event)
        {
            case CMD_IN_CONNECT:
                CONNECT_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendConnect(builder);
                break;

            case CMD_IN_CLOSE_SESSION:
                CLOSE_SESSION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendCloseSession(builder);
                break;

            case CMD_IN_START_RECORDING:
                START_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStartRecording(builder);
                break;

            case CMD_IN_STOP_RECORDING:
                STOP_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopRecording(builder);
                break;

            case CMD_IN_REPLAY:
                REPLAY_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendReplay(builder);
                break;

            case CMD_IN_STOP_REPLAY:
                STOP_REPLAY_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopReplay(builder);
                break;

            case CMD_IN_LIST_RECORDINGS:
                LIST_RECORDINGS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendListRecordings(builder);
                break;

            case CMD_IN_LIST_RECORDINGS_FOR_URI:
                LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendListRecordingsForUri(builder);
                break;

            case CMD_IN_LIST_RECORDING:
                LIST_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendListRecording(builder);
                break;
            case CMD_IN_EXTEND_RECORDING:
                EXTEND_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendExtendRecording(builder);
                break;

            case CMD_IN_RECORDING_POSITION:
                RECORDING_POSITION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendRecordingPosition(builder);
                break;

            case CMD_IN_TRUNCATE_RECORDING:
                TRUNCATE_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendTruncateRecording(builder);
                break;

            case CMD_IN_STOP_RECORDING_SUBSCRIPTION:
                STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopRecordingSubscription(builder);
                break;

            case CMD_IN_STOP_POSITION:
                STOP_POSITION_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopPosition(builder);
                break;

            case CMD_IN_FIND_LAST_MATCHING_RECORD:
                FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendFindLastMatchingRecord(builder);
                break;

            case CMD_IN_LIST_RECORDING_SUBSCRIPTIONS:
                LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendListRecordingSubscriptions(builder);
                break;

            default:
                builder.append("ARCHIVE: COMMAND UNKNOWN: ").append(event);
        }
    }

    private static void appendListRecordingSubscriptions(final StringBuilder builder)
    {
        builder.append("ARCHIVE: LIST_RECORDING_SUBSCRIPTIONS ")
            .append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.channel()).append(' ')
            .append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.streamId()).append(' ')
            .append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.applyStreamId()).append(' ')
            .append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.pseudoIndex()).append(' ')
            .append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendFindLastMatchingRecord(final StringBuilder builder)
    {
        builder.append("ARCHIVE: FIND_LAST_MATCHING_RECORDING ")
            .append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.streamId()).append(' ')
            .append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.sessionId()).append(' ')
            .append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.minRecordingId()).append(' ')
            .append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendStopPosition(final StringBuilder builder)
    {
        builder.append("ARCHIVE: STOP_POSITION ")
            .append(STOP_POSITION_REQUEST_DECODER.recordingId()).append(' ')
            .append(STOP_POSITION_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(STOP_POSITION_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendStopRecordingSubscription(final StringBuilder builder)
    {
        builder.append("ARCHIVE: STOP_RECORDING_SUBSCRIPTION ")
            .append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.subscriptionId()).append(' ')
            .append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendTruncateRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: TRUNCATE_RECORDING ")
            .append(TRUNCATE_RECORDING_REQUEST_DECODER.recordingId()).append(' ')
            .append(TRUNCATE_RECORDING_REQUEST_DECODER.position()).append(' ')
            .append(TRUNCATE_RECORDING_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(TRUNCATE_RECORDING_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendRecordingPosition(final StringBuilder builder)
    {
        builder.append("ARCHIVE: RECORDING_POSITION ")
            .append(RECORDING_POSITION_REQUEST_DECODER.recordingId()).append(' ')
            .append(RECORDING_POSITION_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(RECORDING_POSITION_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendExtendRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: EXTEND_RECORDING ")
            .append(EXTEND_RECORDING_REQUEST_DECODER.channel()).append(' ')
            .append(EXTEND_RECORDING_REQUEST_DECODER.streamId()).append(' ')
            .append(EXTEND_RECORDING_REQUEST_DECODER.recordingId()).append(' ')
            .append(EXTEND_RECORDING_REQUEST_DECODER.sourceLocation()).append(' ')
            .append(EXTEND_RECORDING_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(EXTEND_RECORDING_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendListRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: LIST_RECORDING ")
            .append(LIST_RECORDING_REQUEST_DECODER.recordingId()).append(' ')
            .append(LIST_RECORDING_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(LIST_RECORDING_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendListRecordingsForUri(final StringBuilder builder)
    {
        builder.append("ARCHIVE: LIST_RECORDINGS_FOR_URI ")
            .append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.channel()).append(' ')
            .append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.fromRecordingId()).append(' ')
            .append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendListRecordings(final StringBuilder builder)
    {
        builder.append("ARCHIVE: LIST_RECORDINGS ")
            .append(LIST_RECORDINGS_REQUEST_DECODER.fromRecordingId()).append(' ')
            .append(LIST_RECORDINGS_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(LIST_RECORDINGS_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendStopReplay(final StringBuilder builder)
    {
        builder.append("ARCHIVE: STOP_REPLAY ").append(STOP_REPLAY_REQUEST_DECODER.replaySessionId()).append(' ')
            .append(STOP_REPLAY_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(STOP_REPLAY_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendReplay(final StringBuilder builder)
    {
        builder.append("ARCHIVE: REPLAY ").append(REPLAY_REQUEST_DECODER.replayChannel()).append(' ')
            .append(REPLAY_REQUEST_DECODER.replayStreamId()).append(' ')
            .append(REPLAY_REQUEST_DECODER.recordingId()).append(' ')
            .append(REPLAY_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(REPLAY_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendStopRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: STOP_RECORDING ").append(STOP_RECORDING_REQUEST_DECODER.channel()).append(' ')
            .append(STOP_RECORDING_REQUEST_DECODER.streamId()).append(' ')
            .append(STOP_RECORDING_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(STOP_RECORDING_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendStartRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: START_RECORDING ").append(START_RECORDING_REQUEST_DECODER.channel()).append(' ')
            .append(START_RECORDING_REQUEST_DECODER.streamId()).append(' ')
            .append(START_RECORDING_REQUEST_DECODER.controlSessionId()).append(" [")
            .append(START_RECORDING_REQUEST_DECODER.correlationId()).append(']');
    }

    private static void appendCloseSession(final StringBuilder builder)
    {
        builder.append("ARCHIVE: CLOSE_SESSION ").append(CLOSE_SESSION_REQUEST_DECODER.controlSessionId());
    }

    private static void appendConnect(final StringBuilder builder)
    {
        builder.append("ARCHIVE: CONNECT ").append(CONNECT_REQUEST_DECODER.responseChannel()).append(' ')
            .append(CONNECT_REQUEST_DECODER.responseStreamId()).append(" [")
            .append(CONNECT_REQUEST_DECODER.correlationId()).append("][")
            .append(CONNECT_REQUEST_DECODER.version()).append(']');
    }
}