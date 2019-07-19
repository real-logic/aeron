/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
    private static final BoundedReplayRequestDecoder BOUNDED_REPLAY_REQUEST_DECODER = new BoundedReplayRequestDecoder();
    private static final StopAllReplaysRequestDecoder STOP_ALL_REPLAYS_REQUEST_DECODER =
        new StopAllReplaysRequestDecoder();

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

            case CMD_IN_START_BOUNDED_REPLAY:
                BOUNDED_REPLAY_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStartBoundedReplay(builder);
                break;

            case CMD_IN_STOP_ALL_REPLAYS:
                STOP_ALL_REPLAYS_REQUEST_DECODER.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    HEADER_DECODER.blockLength(),
                    HEADER_DECODER.version());
                appendStopAllReplays(builder);
                break;

            default:
                builder.append("ARCHIVE: COMMAND UNKNOWN: ").append(event);
        }
    }

    private static void appendConnect(final StringBuilder builder)
    {
        builder.append("ARCHIVE: CONNECT")
            .append(", correlationId=").append(CONNECT_REQUEST_DECODER.correlationId())
            .append(", responseStreamId=").append(CONNECT_REQUEST_DECODER.responseStreamId())
            .append(", version=").append(CONNECT_REQUEST_DECODER.version())
            .append(", responseChannel=");

        CONNECT_REQUEST_DECODER.getResponseChannel(builder);
    }

    private static void appendCloseSession(final StringBuilder builder)
    {
        builder.append("ARCHIVE: CLOSE_SESSION")
            .append(", controlSessionId=").append(CLOSE_SESSION_REQUEST_DECODER.controlSessionId());
    }

    private static void appendStartRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: START_RECORDING")
            .append(", controlSessionId=").append(START_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(START_RECORDING_REQUEST_DECODER.correlationId())
            .append(", streamId=").append(START_RECORDING_REQUEST_DECODER.streamId())
            .append(", sourceLocation=").append(START_RECORDING_REQUEST_DECODER.sourceLocation())
            .append(", channel=");

        START_RECORDING_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendStopRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: STOP_RECORDING")
            .append(", controlSessionId=").append(STOP_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(STOP_RECORDING_REQUEST_DECODER.correlationId())
            .append(", streamId=").append(STOP_RECORDING_REQUEST_DECODER.streamId())
            .append(", channel=");

        STOP_RECORDING_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendReplay(final StringBuilder builder)
    {
        builder.append("ARCHIVE: REPLAY")
            .append(", controlSessionId=").append(REPLAY_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(REPLAY_REQUEST_DECODER.correlationId())
            .append(", recordingId=").append(REPLAY_REQUEST_DECODER.recordingId())
            .append(", position=").append(REPLAY_REQUEST_DECODER.position())
            .append(", length=").append(REPLAY_REQUEST_DECODER.length())
            .append(", replayStreamId=").append(REPLAY_REQUEST_DECODER.replayStreamId())
            .append(", replayChannel=");

        REPLAY_REQUEST_DECODER.getReplayChannel(builder);
    }

    private static void appendStopReplay(final StringBuilder builder)
    {
        builder.append("ARCHIVE: STOP_REPLAY")
            .append(", controlSessionId=").append(STOP_REPLAY_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(STOP_REPLAY_REQUEST_DECODER.correlationId())
            .append(", recordingId=").append(STOP_REPLAY_REQUEST_DECODER.replaySessionId());
    }

    private static void appendListRecordings(final StringBuilder builder)
    {
        builder.append("ARCHIVE: LIST_RECORDINGS")
            .append(", controlSessionId=").append(LIST_RECORDINGS_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(LIST_RECORDINGS_REQUEST_DECODER.correlationId())
            .append(", fromRecordingId=").append(LIST_RECORDINGS_REQUEST_DECODER.fromRecordingId())
            .append(", recordCount=").append(LIST_RECORDINGS_REQUEST_DECODER.recordCount());
    }

    private static void appendListRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: LIST_RECORDING")
            .append(", controlSessionId=").append(LIST_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(LIST_RECORDING_REQUEST_DECODER.correlationId())
            .append(", recordingId=").append(LIST_RECORDING_REQUEST_DECODER.recordingId());
    }

    private static void appendListRecordingsForUri(final StringBuilder builder)
    {
        builder.append("ARCHIVE: LIST_RECORDINGS_FOR_URI ")
            .append(", controlSessionId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.correlationId())
            .append(", fromRecordingId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.fromRecordingId())
            .append(", recordCount=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.recordCount())
            .append(", streamId=").append(LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.streamId())
            .append(", channel=");

        LIST_RECORDINGS_FOR_URI_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendExtendRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: EXTEND_RECORDING")
            .append(", controlSessionId=").append(EXTEND_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(EXTEND_RECORDING_REQUEST_DECODER.correlationId())
            .append(", recordingId=").append(EXTEND_RECORDING_REQUEST_DECODER.recordingId())
            .append(", streamId=").append(EXTEND_RECORDING_REQUEST_DECODER.streamId())
            .append(", sourceLocation=").append(EXTEND_RECORDING_REQUEST_DECODER.sourceLocation())
            .append(", channel=");

        EXTEND_RECORDING_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendRecordingPosition(final StringBuilder builder)
    {
        builder.append("ARCHIVE: RECORDING_POSITION")
            .append(", controlSessionId=").append(RECORDING_POSITION_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(RECORDING_POSITION_REQUEST_DECODER.correlationId())
            .append(", recordingId=").append(RECORDING_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void appendTruncateRecording(final StringBuilder builder)
    {
        builder.append("ARCHIVE: TRUNCATE_RECORDING")
            .append(", controlSessionId=").append(TRUNCATE_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(TRUNCATE_RECORDING_REQUEST_DECODER.correlationId())
            .append(", recordingId=").append(TRUNCATE_RECORDING_REQUEST_DECODER.recordingId())
            .append(", position=").append(TRUNCATE_RECORDING_REQUEST_DECODER.position());
    }

    private static void appendStopRecordingSubscription(final StringBuilder builder)
    {
        builder.append("ARCHIVE: STOP_RECORDING_SUBSCRIPTION")
            .append(", controlSessionId=").append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.correlationId())
            .append(", subscriptionId=").append(STOP_RECORDING_SUBSCRIPTION_REQUEST_DECODER.subscriptionId());
    }

    private static void appendStopPosition(final StringBuilder builder)
    {
        builder.append("ARCHIVE: STOP_POSITION")
            .append(", controlSessionId=").append(STOP_POSITION_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(STOP_POSITION_REQUEST_DECODER.correlationId())
            .append(", recordingId=").append(STOP_POSITION_REQUEST_DECODER.recordingId());
    }

    private static void appendFindLastMatchingRecord(final StringBuilder builder)
    {
        builder.append("ARCHIVE: FIND_LAST_MATCHING_RECORDING")
            .append(", controlSessionId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.correlationId())
            .append(", minRecordingId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.minRecordingId())
            .append(", sessionId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.sessionId())
            .append(", streamId=").append(FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.streamId())
            .append(", channel=");

        FIND_LAST_MATCHING_RECORDING_REQUEST_DECODER.getChannel(builder);
    }

    private static void appendListRecordingSubscriptions(final StringBuilder builder)
    {
        builder.append("ARCHIVE: LIST_RECORDING_SUBSCRIPTIONS ")
            .append(", controlSessionId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.correlationId())
            .append(", pseudoIndex=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.pseudoIndex())
            .append(", applyStreamId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.applyStreamId())
            .append(", subscriptionCount=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.subscriptionCount())
            .append(", streamId=").append(LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.streamId())
            .append(", channel=");

        LIST_RECORDING_SUBSCRIPTIONS_REQUEST_DECODER.getChannel(builder);
    }


    private static void appendStartBoundedReplay(final StringBuilder builder)
    {
        builder.append("ARCHIVE: START_BOUNDED_REPLAY")
            .append(", controlSessionId=").append(BOUNDED_REPLAY_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(BOUNDED_REPLAY_REQUEST_DECODER.correlationId())
            .append(", recordingId=").append(BOUNDED_REPLAY_REQUEST_DECODER.recordingId())
            .append(", position=").append(BOUNDED_REPLAY_REQUEST_DECODER.position())
            .append(", length=").append(BOUNDED_REPLAY_REQUEST_DECODER.length())
            .append(", limitCounterId=").append(BOUNDED_REPLAY_REQUEST_DECODER.limitCounterId())
            .append(", replayStreamId=").append(BOUNDED_REPLAY_REQUEST_DECODER.replayStreamId())
            .append(", replayChannel=");

        BOUNDED_REPLAY_REQUEST_DECODER.getReplayChannel(builder);
    }

    private static void appendStopAllReplays(final StringBuilder builder)
    {
        builder.append("ARCHIVE: STOP_ALL_REPLAYS")
            .append(", controlSessionId=").append(STOP_ALL_REPLAYS_REQUEST_DECODER.controlSessionId())
            .append(", correlationId=").append(STOP_ALL_REPLAYS_REQUEST_DECODER.correlationId())
            .append(", recordingId=").append(STOP_ALL_REPLAYS_REQUEST_DECODER.recordingId());
    }
}
