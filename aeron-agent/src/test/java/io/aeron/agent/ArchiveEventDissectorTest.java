/*
 * Copyright 2014-2020 Real Logic Limited.
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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static io.aeron.agent.ArchiveEventCode.*;
import static io.aeron.agent.ArchiveEventDissector.CONTEXT;
import static io.aeron.agent.ArchiveEventDissector.controlRequest;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.CommonEventEncoder.internalEncodeLogHeader;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static io.aeron.archive.codecs.ControlResponseCode.NULL_VAL;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ArchiveEventDissectorTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);
    private final StringBuilder builder = new StringBuilder();
    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();

    @Test
    void controlResponse()
    {
        internalEncodeLogHeader(buffer, 0, 100, 100, () -> 1_250_000_000);
        final ControlResponseEncoder responseEncoder = new ControlResponseEncoder();
        responseEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(13)
            .correlationId(42)
            .relevantId(8)
            .code(NULL_VAL)
            .version(111)
            .errorMessage("the %ERR% msg");

        ArchiveEventDissector.controlResponse(buffer, 0, builder);

        assertEquals("[1.25] " + CONTEXT + ": " + CMD_OUT_RESPONSE.name() + " [100/100]: " +
            "controlSessionId=13" +
            ", correlationId=42" +
            ", relevantId=8" +
            ", code=" + NULL_VAL +
            ", version=111" +
            ", errorMessage=the %ERR% msg",
            builder.toString());
    }

    @Test
    void controlRequestConnect()
    {
        internalEncodeLogHeader(buffer, 0, 32, 64, () -> 5_600_000_000L);
        final ConnectRequestEncoder requestEncoder = new ConnectRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .correlationId(88)
            .responseStreamId(42)
            .version(-10)
            .responseChannel("call me maybe");

        controlRequest(CMD_IN_CONNECT, buffer, 0, builder);

        assertEquals("[5.6] " + CONTEXT + ": " + CMD_IN_CONNECT.name() + " [32/64]: " +
            "correlationId=88" +
            ", responseStreamId=42" +
            ", version=-10" +
            ", responseChannel=call me maybe",
            builder.toString());
    }

    @Test
    void controlRequestCloseSession()
    {
        internalEncodeLogHeader(buffer, 0, 32, 64, () -> 5_600_000_000L);
        final CloseSessionRequestEncoder requestEncoder = new CloseSessionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(-1);

        controlRequest(CMD_IN_CLOSE_SESSION, buffer, 0, builder);

        assertEquals("[5.6] " + CONTEXT + ": " + CMD_IN_CLOSE_SESSION.name() + " [32/64]: controlSessionId=-1",
            builder.toString());
    }

    @Test
    void controlRequestStartRecording()
    {
        internalEncodeLogHeader(buffer, 0, 32, 64, () -> 5_600_000_000L);
        final StartRecordingRequestEncoder requestEncoder = new StartRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(5)
            .correlationId(13)
            .streamId(7)
            .sourceLocation(SourceLocation.REMOTE)
            .channel("foo");

        controlRequest(CMD_IN_START_RECORDING, buffer, 0, builder);

        assertEquals("[5.6] " + CONTEXT + ": " + CMD_IN_START_RECORDING.name() + " [32/64]:" +
            " controlSessionId=5" +
            ", correlationId=13" +
            ", streamId=7" +
            ", sourceLocation=" + SourceLocation.REMOTE +
            ", channel=foo",
            builder.toString());
    }

    @Test
    void controlRequestStopRecording()
    {
        internalEncodeLogHeader(buffer, 0, 32, 64, () -> 5_600_000_000L);
        final StopRecordingRequestEncoder requestEncoder = new StopRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(5)
            .correlationId(42)
            .streamId(7)
            .channel("bar");

        controlRequest(CMD_IN_STOP_RECORDING, buffer, 0, builder);

        assertEquals("[5.6] " + CONTEXT + ": " + CMD_IN_STOP_RECORDING.name() + " [32/64]:" +
            " controlSessionId=5" +
            ", correlationId=42" +
            ", streamId=7" +
            ", channel=bar",
            builder.toString());
    }

    @Test
    void controlRequestReplay()
    {
        internalEncodeLogHeader(buffer, 0, 90, 90, () -> 1_125_000_000L);
        final ReplayRequestEncoder requestEncoder = new ReplayRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(5)
            .correlationId(42)
            .recordingId(178)
            .position(Long.MAX_VALUE)
            .length(2000)
            .replayStreamId(99)
            .replayChannel("replay channel");

        controlRequest(CMD_IN_REPLAY, buffer, 0, builder);

        assertEquals("[1.125] " + CONTEXT + ": " + CMD_IN_REPLAY.name() + " [90/90]:" +
            " controlSessionId=5" +
            ", correlationId=42" +
            ", recordingId=178" +
            ", position=" + Long.MAX_VALUE +
            ", length=2000" +
            ", replayStreamId=99" +
            ", replayChannel=replay channel",
            builder.toString());
    }

    @Test
    void controlRequestStopReplay()
    {
        internalEncodeLogHeader(buffer, 0, 90, 90, () -> 1_125_000_000L);
        final StopReplayRequestEncoder requestEncoder = new StopReplayRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(5)
            .correlationId(42)
            .replaySessionId(66);

        controlRequest(CMD_IN_STOP_REPLAY, buffer, 0, builder);

        assertEquals("[1.125] " + CONTEXT + ": " + CMD_IN_STOP_REPLAY.name() + " [90/90]:" +
            " controlSessionId=5" +
            ", correlationId=42" +
            ", replaySessionId=66",
            builder.toString());
    }

    @Test
    void controlRequestListRecordings()
    {
        internalEncodeLogHeader(buffer, 0, 32, 32, () -> 100_000_000L);
        final ListRecordingsRequestEncoder requestEncoder = new ListRecordingsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(9)
            .correlationId(78)
            .fromRecordingId(45)
            .recordCount(10);

        controlRequest(CMD_IN_LIST_RECORDINGS, buffer, 0, builder);

        assertEquals("[0.1] " + CONTEXT + ": " + CMD_IN_LIST_RECORDINGS.name() + " [32/32]:" +
            " controlSessionId=9" +
            ", correlationId=78" +
            ", fromRecordingId=45" +
            ", recordCount=10",
            builder.toString());
    }

    @Test
    void controlRequestListRecordingsForUri()
    {
        internalEncodeLogHeader(buffer, 0, 32, 32, () -> 100_000_000L);
        final ListRecordingsForUriRequestEncoder requestEncoder = new ListRecordingsForUriRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(9)
            .correlationId(78)
            .fromRecordingId(45)
            .recordCount(10)
            .streamId(200)
            .channel("CH");

        controlRequest(CMD_IN_LIST_RECORDINGS_FOR_URI, buffer, 0, builder);

        assertEquals("[0.1] " + CONTEXT + ": " + CMD_IN_LIST_RECORDINGS_FOR_URI.name() + " [32/32]:" +
            " controlSessionId=9" +
            ", correlationId=78" +
            ", fromRecordingId=45" +
            ", recordCount=10" +
            ", streamId=200" +
            ", channel=CH",
            builder.toString());
    }

    @Test
    void controlRequestListRecording()
    {
        internalEncodeLogHeader(buffer, 0, 32, 32, () -> 100_000_000L);
        final ListRecordingRequestEncoder requestEncoder = new ListRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(19)
            .correlationId(178)
            .recordingId(1010101);

        controlRequest(CMD_IN_LIST_RECORDING, buffer, 0, builder);

        assertEquals("[0.1] " + CONTEXT + ": " + CMD_IN_LIST_RECORDING.name() + " [32/32]:" +
            " controlSessionId=19" +
            ", correlationId=178" +
            ", recordingId=1010101",
            builder.toString());
    }

    @Test
    void controlRequestExtendRecording()
    {
        internalEncodeLogHeader(buffer, 0, 12, 32, () -> 10_000_000_000L);
        final ExtendRecordingRequestEncoder requestEncoder = new ExtendRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(9)
            .correlationId(78)
            .recordingId(1010101)
            .streamId(43)
            .sourceLocation(SourceLocation.LOCAL)
            .channel("extend me");

        controlRequest(CMD_IN_EXTEND_RECORDING, buffer, 0, builder);

        assertEquals("[10.0] " + CONTEXT + ": " + CMD_IN_EXTEND_RECORDING.name() + " [12/32]:" +
            " controlSessionId=9" +
            ", correlationId=78" +
            ", recordingId=1010101" +
            ", streamId=43" +
            ", sourceLocation=" + SourceLocation.LOCAL +
            ", channel=extend me",
            builder.toString());
    }

    @Test
    void controlRequestRecordingPosition()
    {
        internalEncodeLogHeader(buffer, 0, 12, 32, () -> 10_000_000_000L);
        final RecordingPositionRequestEncoder requestEncoder = new RecordingPositionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(2)
            .correlationId(3)
            .recordingId(6);

        controlRequest(CMD_IN_RECORDING_POSITION, buffer, 0, builder);

        assertEquals("[10.0] " + CONTEXT + ": " + CMD_IN_RECORDING_POSITION.name() + " [12/32]:" +
            " controlSessionId=2" +
            ", correlationId=3" +
            ", recordingId=6",
            builder.toString());
    }

    @Test
    void controlRequestTruncateRecording()
    {
        internalEncodeLogHeader(buffer, 0, 12, 32, () -> 10_000_000_000L);
        final TruncateRecordingRequestEncoder requestEncoder = new TruncateRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(2)
            .correlationId(3)
            .recordingId(8)
            .position(1_000_000);

        controlRequest(CMD_IN_TRUNCATE_RECORDING, buffer, 0, builder);

        assertEquals("[10.0] " + CONTEXT + ": " + CMD_IN_TRUNCATE_RECORDING.name() + " [12/32]:" +
            " controlSessionId=2" +
            ", correlationId=3" +
            ", recordingId=8" +
            ", position=1000000",
            builder.toString());
    }

    @Test
    void controlRequestStopRecordingSubscription()
    {
        internalEncodeLogHeader(buffer, 0, 12, 32, () -> 10_000_000_000L);
        final StopRecordingSubscriptionRequestEncoder requestEncoder = new StopRecordingSubscriptionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(22)
            .correlationId(33)
            .subscriptionId(888);

        controlRequest(CMD_IN_STOP_RECORDING_SUBSCRIPTION, buffer, 0, builder);

        assertEquals("[10.0] " + CONTEXT + ": " + CMD_IN_STOP_RECORDING_SUBSCRIPTION.name() + " [12/32]:" +
            " controlSessionId=22" +
            ", correlationId=33" +
            ", subscriptionId=888",
            builder.toString());
    }

    @Test
    void controlRequestStopPosition()
    {
        internalEncodeLogHeader(buffer, 0, 12, 32, () -> 10_000_000_000L);
        final StopPositionRequestEncoder requestEncoder = new StopPositionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(22)
            .correlationId(33)
            .recordingId(44);

        controlRequest(CMD_IN_STOP_POSITION, buffer, 0, builder);

        assertEquals("[10.0] " + CONTEXT + ": " + CMD_IN_STOP_POSITION.name() + " [12/32]:" +
            " controlSessionId=22" +
            ", correlationId=33" +
            ", recordingId=44",
            builder.toString());
    }

    @Test
    void controlRequestFindLastMatchingRecording()
    {
        internalEncodeLogHeader(buffer, 0, 90, 90, () -> 10_325_000_000L);
        final FindLastMatchingRecordingRequestEncoder requestEncoder = new FindLastMatchingRecordingRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(1)
            .correlationId(2)
            .minRecordingId(3)
            .sessionId(4)
            .streamId(5)
            .channel("this is a channel");

        controlRequest(CMD_IN_FIND_LAST_MATCHING_RECORD, buffer, 0, builder);

        assertEquals("[10.325] " + CONTEXT + ": " + CMD_IN_FIND_LAST_MATCHING_RECORD.name() + " [90/90]:" +
            " controlSessionId=1" +
            ", correlationId=2" +
            ", minRecordingId=3" +
            ", sessionId=4" +
            ", streamId=5" +
            ", channel=this is a channel",
            builder.toString());
    }

    @Test
    void controlRequestListRecordingSubscriptions()
    {
        internalEncodeLogHeader(buffer, 0, 90, 90, () -> 10_325_000_000L);
        final ListRecordingSubscriptionsRequestEncoder requestEncoder = new ListRecordingSubscriptionsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(1)
            .correlationId(2)
            .pseudoIndex(1111111)
            .applyStreamId(BooleanType.TRUE)
            .subscriptionCount(777)
            .streamId(555)
            .channel("ch2");

        controlRequest(CMD_IN_LIST_RECORDING_SUBSCRIPTIONS, buffer, 0, builder);

        assertEquals("[10.325] " + CONTEXT + ": " + CMD_IN_LIST_RECORDING_SUBSCRIPTIONS.name() + " [90/90]:" +
            " controlSessionId=1" +
            ", correlationId=2" +
            ", pseudoIndex=1111111" +
            ", applyStreamId=" + BooleanType.TRUE +
            ", subscriptionCount=777" +
            ", streamId=555" +
            ", channel=ch2",
            builder.toString());
    }

    @Test
    void controlRequestStartBoundedReplay()
    {
        internalEncodeLogHeader(buffer, 0, 90, 90, () -> 10_325_000_000L);
        final BoundedReplayRequestEncoder requestEncoder = new BoundedReplayRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(10)
            .correlationId(20)
            .recordingId(30)
            .position(40)
            .length(50)
            .limitCounterId(-123)
            .replayStreamId(14)
            .replayChannel("rep ch");

        controlRequest(CMD_IN_START_BOUNDED_REPLAY, buffer, 0, builder);

        assertEquals("[10.325] " + CONTEXT + ": " + CMD_IN_START_BOUNDED_REPLAY.name() + " [90/90]:" +
            " controlSessionId=10" +
            ", correlationId=20" +
            ", recordingId=30" +
            ", position=40" +
            ", length=50" +
            ", limitCounterId=-123" +
            ", replayStreamId=14" +
            ", replayChannel=rep ch",
            builder.toString());
    }

    @Test
    void controlRequestStopAllReplays()
    {
        internalEncodeLogHeader(buffer, 0, 90, 90, () -> 10_325_000_000L);
        final StopAllReplaysRequestEncoder requestEncoder = new StopAllReplaysRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(10)
            .correlationId(20)
            .recordingId(30);

        controlRequest(CMD_IN_STOP_ALL_REPLAYS, buffer, 0, builder);

        assertEquals("[10.325] " + CONTEXT + ": " + CMD_IN_STOP_ALL_REPLAYS.name() + " [90/90]:" +
            " controlSessionId=10" +
            ", correlationId=20" +
            ", recordingId=30",
            builder.toString());
    }

    @Test
    void controlRequestReplicate()
    {
        internalEncodeLogHeader(buffer, 0, 1000, 1000, () -> 500_000_000L);
        final ReplicateRequestEncoder requestEncoder = new ReplicateRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(2)
            .correlationId(5)
            .srcRecordingId(17)
            .dstRecordingId(2048)
            .srcControlStreamId(10)
            .srcControlChannel("CTRL ch")
            .liveDestination("live destination");

        controlRequest(CMD_IN_REPLICATE, buffer, 0, builder);

        assertEquals("[0.5] " + CONTEXT + ": " + CMD_IN_REPLICATE.name() + " [1000/1000]:" +
            " controlSessionId=2" +
            ", correlationId=5" +
            ", srcRecordingId=17" +
            ", dstRecordingId=2048" +
            ", srcControlStreamId=10" +
            ", srcControlChannel=CTRL ch" +
            ", liveDestination=live destination",
            builder.toString());
    }

    @Test
    void controlRequestStopReplication()
    {
        internalEncodeLogHeader(buffer, 0, 1000, 1000, () -> 500_000_000L);
        final StopReplicationRequestEncoder requestEncoder = new StopReplicationRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(-2)
            .correlationId(-5)
            .replicationId(-999);

        controlRequest(CMD_IN_STOP_REPLICATION, buffer, 0, builder);

        assertEquals("[0.5] " + CONTEXT + ": " + CMD_IN_STOP_REPLICATION.name() + " [1000/1000]:" +
            " controlSessionId=-2" +
            ", correlationId=-5" +
            ", replicationId=-999",
            builder.toString());
    }

    @Test
    void controlRequestStartPosition()
    {
        internalEncodeLogHeader(buffer, 0, 1000, 1000, () -> 500_000_000L);
        final StartPositionRequestEncoder requestEncoder = new StartPositionRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(3)
            .correlationId(16)
            .recordingId(1);

        controlRequest(CMD_IN_START_POSITION, buffer, 0, builder);

        assertEquals("[0.5] " + CONTEXT + ": " + CMD_IN_START_POSITION.name() + " [1000/1000]:" +
            " controlSessionId=3" +
            ", correlationId=16" +
            ", recordingId=1",
            builder.toString());
    }

    @Test
    void controlRequestDetachSegments()
    {
        internalEncodeLogHeader(buffer, 0, 1000, 1000, () -> 500_000_000L);
        final DetachSegmentsRequestEncoder requestEncoder = new DetachSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(3)
            .correlationId(16)
            .recordingId(1);

        controlRequest(CMD_IN_DETACH_SEGMENTS, buffer, 0, builder);

        assertEquals("[0.5] " + CONTEXT + ": " + CMD_IN_DETACH_SEGMENTS.name() + " [1000/1000]:" +
            " controlSessionId=3" +
            ", correlationId=16" +
            ", recordingId=1",
            builder.toString());
    }

    @Test
    void controlRequestDeleteDetachedSegments()
    {
        internalEncodeLogHeader(buffer, 0, 1000, 1000, () -> 500_000_000L);
        final DeleteDetachedSegmentsRequestEncoder requestEncoder = new DeleteDetachedSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(53)
            .correlationId(516)
            .recordingId(51);

        controlRequest(CMD_IN_DELETE_DETACHED_SEGMENTS, buffer, 0, builder);

        assertEquals("[0.5] " + CONTEXT + ": " + CMD_IN_DELETE_DETACHED_SEGMENTS.name() + " [1000/1000]:" +
            " controlSessionId=53" +
            ", correlationId=516" +
            ", recordingId=51",
            builder.toString());
    }

    @Test
    void controlRequestPurgeSegments()
    {
        internalEncodeLogHeader(buffer, 0, 1000, 1000, () -> 500_000_000L);
        final PurgeSegmentsRequestEncoder requestEncoder = new PurgeSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(3)
            .correlationId(56)
            .recordingId(15)
            .newStartPosition(100);

        controlRequest(CMD_IN_PURGE_SEGMENTS, buffer, 0, builder);

        assertEquals("[0.5] " + CONTEXT + ": " + CMD_IN_PURGE_SEGMENTS.name() + " [1000/1000]:" +
            " controlSessionId=3" +
            ", correlationId=56" +
            ", recordingId=15" +
            ", newStartPosition=100",
            builder.toString());
    }

    @Test
    void controlRequestAttachSegments()
    {
        internalEncodeLogHeader(buffer, 0, 1000, 1000, () -> 500_000_000L);
        final AttachSegmentsRequestEncoder requestEncoder = new AttachSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(30)
            .correlationId(560)
            .recordingId(50);

        controlRequest(CMD_IN_ATTACH_SEGMENTS, buffer, 0, builder);

        assertEquals("[0.5] " + CONTEXT + ": " + CMD_IN_ATTACH_SEGMENTS.name() + " [1000/1000]:" +
            " controlSessionId=30" +
            ", correlationId=560" +
            ", recordingId=50",
            builder.toString());
    }

    @Test
    void controlRequestMigrateSegments()
    {
        internalEncodeLogHeader(buffer, 0, 1000, 1000, () -> 500_000_000L);
        final MigrateSegmentsRequestEncoder requestEncoder = new MigrateSegmentsRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(7)
            .correlationId(6)
            .srcRecordingId(1)
            .dstRecordingId(21902);

        controlRequest(CMD_IN_MIGRATE_SEGMENTS, buffer, 0, builder);

        assertEquals("[0.5] " + CONTEXT + ": " + CMD_IN_MIGRATE_SEGMENTS.name() + " [1000/1000]:" +
            " controlSessionId=7" +
            ", correlationId=6" +
            ", srcRecordingId=1" +
            ", dstRecordingId=21902",
            builder.toString());
    }

    @Test
    void controlRequestAuthConnect()
    {
        internalEncodeLogHeader(buffer, 0, 3, 6, () -> 5_500_000_000L);
        final AuthConnectRequestEncoder requestEncoder = new AuthConnectRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .correlationId(16)
            .responseStreamId(19)
            .version(2)
            .responseChannel("English Channel")
            .putEncodedCredentials("hello".getBytes(US_ASCII), 0, 5);

        controlRequest(CMD_IN_AUTH_CONNECT, buffer, 0, builder);

        assertEquals("[5.5] " + CONTEXT + ": " + CMD_IN_AUTH_CONNECT.name() + " [3/6]:" +
            " correlationId=16" +
            ", responseStreamId=19" +
            ", version=2" +
            ", responseChannel=English Channel" +
            ", encodedCredentialsLength=5",
            builder.toString());
    }

    @Test
    void controlRequestKeepAlive()
    {
        internalEncodeLogHeader(buffer, 0, 3, 6, () -> 5_500_000_000L);
        final KeepAliveRequestEncoder requestEncoder = new KeepAliveRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(31)
            .correlationId(119);

        controlRequest(CMD_IN_KEEP_ALIVE, buffer, 0, builder);

        assertEquals("[5.5] " + CONTEXT + ": " + CMD_IN_KEEP_ALIVE.name() + " [3/6]:" +
            " controlSessionId=31" +
            ", correlationId=119",
            builder.toString());
    }

    @Test
    void controlRequestTaggedReplicate()
    {
        internalEncodeLogHeader(buffer, 0, 3, 6, () -> 5_500_000_000L);
        final TaggedReplicateRequestEncoder requestEncoder = new TaggedReplicateRequestEncoder();
        requestEncoder.wrapAndApplyHeader(buffer, LOG_HEADER_LENGTH, headerEncoder)
            .controlSessionId(1)
            .correlationId(-10)
            .srcRecordingId(9)
            .dstRecordingId(31)
            .channelTagId(4)
            .subscriptionTagId(7)
            .srcControlStreamId(15)
            .srcControlChannel("src")
            .liveDestination("alive and well");

        controlRequest(CMD_IN_TAGGED_REPLICATE, buffer, 0, builder);

        assertEquals("[5.5] " + CONTEXT + ": " + CMD_IN_TAGGED_REPLICATE.name() + " [3/6]:" +
            " controlSessionId=1" +
            ", correlationId=-10" +
            ", srcRecordingId=9" +
            ", dstRecordingId=31" +
            ", channelTagId=4" +
            ", subscriptionTagId=7" +
            ", srcControlStreamId=15" +
            ", srcControlChannel=src" +
            ", liveDestination=alive and well",
            builder.toString());
    }

    @Test
    void controlRequestUnknownCommand()
    {
        internalEncodeLogHeader(buffer, 0, 10, 20, () -> 2_500_000_000L);
        headerEncoder.wrap(buffer, LOG_HEADER_LENGTH).templateId(Integer.MIN_VALUE);

        controlRequest(CMD_OUT_RESPONSE, buffer, 0, builder);

        assertEquals("[2.5] " + CONTEXT + ": " + CMD_OUT_RESPONSE.name() + " [10/20]: unknown command",
            builder.toString());
    }
}
