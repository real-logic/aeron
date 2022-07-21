/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.Image;
import io.aeron.archive.codecs.*;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthorisationService;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ControlSessionDemuxerTest
{

    public static final long CONTROL_SESSION_ID = 928374L;
    private final ArchiveConductor mockConductor = mock(ArchiveConductor.class);
    private final Image mockImage = mock(Image.class);
    private final AuthorisationService mockAuthorisationService = mock(AuthorisationService.class);
    private final Header mockHeader = mock(Header.class);
    private final ControlSession mockSession = mock(ControlSession.class);

    public static final int SCHEMA_VERSION_6 = 6;

    @Test
    void shouldHandleReplicationRequest2()
    {
        final ControlSessionDemuxer controlSessionDemuxer = new ControlSessionDemuxer(
            new ControlRequestDecoders(), mockImage, mockConductor, mockAuthorisationService);
        setupControlSession(controlSessionDemuxer, CONTROL_SESSION_ID);

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final ReplicateRequest2Encoder replicateRequest2Encoder = new ReplicateRequest2Encoder();

        replicateRequest2Encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        final int fileIoMaxLength = Aeron.NULL_VALUE; // Since v7

        replicateRequest2Encoder
            .controlSessionId(928374L)
            .correlationId(9382475L)
            .srcRecordingId(1234234L)
            .dstRecordingId(2532453245L)
            .stopPosition(2315345L)
            .channelTagId(234L)
            .subscriptionTagId(235L)
            .srcControlStreamId(982374)
            .fileIoMaxLength(4096)
            .srcControlChannel("src")
            .liveDestination("live")
            .replicationChannel("replication");
        final int replicateRequestLength = replicateRequest2Encoder.encodedLength();

        controlSessionDemuxer.onFragment(buffer, 0, replicateRequestLength, mockHeader);

        final ReplicateRequest2Decoder expected = new ReplicateRequest2Decoder()
            .wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());
        verify(mockSession).onReplicate(
            expected.correlationId(),
            expected.srcRecordingId(),
            expected.dstRecordingId(),
            expected.stopPosition(),
            expected.channelTagId(),
            expected.subscriptionTagId(),
            expected.srcControlStreamId(),
            expected.fileIoMaxLength(),
            expected.srcControlChannel(),
            expected.liveDestination(),
            expected.replicationChannel());
    }

    @Test
    void shouldHandleReplayRequest()
    {
        final ControlSessionDemuxer controlSessionDemuxer = new ControlSessionDemuxer(
            new ControlRequestDecoders(), mockImage, mockConductor, mockAuthorisationService);
        setupControlSession(controlSessionDemuxer, CONTROL_SESSION_ID);

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final ReplayRequestEncoder replayRequestEncoder = new ReplayRequestEncoder();

        replayRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        replayRequestEncoder
            .controlSessionId(928374L)
            .correlationId(9382475L)
            .recordingId(9827345897L)
            .position(982374L)
            .fileIoMaxLength(4096)
            .replayStreamId(9832475)
            .replayChannel("replay");

        final int replicateRequestLength = replayRequestEncoder.encodedLength();

        controlSessionDemuxer.onFragment(buffer, 0, replicateRequestLength, mockHeader);

        final ReplayRequestDecoder expected = new ReplayRequestDecoder()
            .wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());

        verify(mockSession).onStartReplay(
            expected.correlationId(),
            expected.recordingId(),
            expected.position(),
            expected.length(),
            expected.fileIoMaxLength(),
            expected.replayStreamId(),
            expected.replayChannel());
    }

    @Test
    void shouldHandleBoundedReplayRequest()
    {
        final ControlSessionDemuxer controlSessionDemuxer = new ControlSessionDemuxer(
            new ControlRequestDecoders(), mockImage, mockConductor, mockAuthorisationService);
        setupControlSession(controlSessionDemuxer, CONTROL_SESSION_ID);

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final BoundedReplayRequestEncoder replayRequestEncoder = new BoundedReplayRequestEncoder();

        replayRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        replayRequestEncoder
            .controlSessionId(928374L)
            .correlationId(9382475L)
            .recordingId(9827345897L)
            .position(982374L)
            .limitCounterId(92734)
            .replayStreamId(9832475)
            .fileIoMaxLength(4096)
            .replayChannel("replay");

        final int replicateRequestLength = replayRequestEncoder.encodedLength();

        controlSessionDemuxer.onFragment(buffer, 0, replicateRequestLength, mockHeader);

        final BoundedReplayRequestDecoder expected = new BoundedReplayRequestDecoder()
            .wrapAndApplyHeader(buffer, 0, new MessageHeaderDecoder());

        verify(mockSession).onStartBoundedReplay(
            expected.correlationId(),
            expected.recordingId(),
            expected.position(),
            expected.length(),
            expected.limitCounterId(),
            expected.fileIoMaxLength(),
            expected.replayStreamId(),
            expected.replayChannel());
    }

    private void setupControlSession(final ControlSessionDemuxer controlSessionDemuxer, final long controlSessionId)
    {
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder2 = new MessageHeaderEncoder();
        final ConnectRequestEncoder connectRequestEncoder = new ConnectRequestEncoder();
        connectRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder2);
        connectRequestEncoder
            .correlationId(100)
            .responseStreamId(100)
            .version(SCHEMA_VERSION_6)
            .responseChannel("foo");
        final int connectRequestLength = connectRequestEncoder.encodedLength();

        doReturn(mockSession).when(mockConductor).newControlSession(anyLong(), anyInt(), anyInt(), any(), any(), any());
        doReturn(controlSessionId).when(mockSession).sessionId();
        doReturn(true).when(mockAuthorisationService).isAuthorised(anyInt(), anyInt(), any(), any());

        controlSessionDemuxer.onFragment(buffer, 0, connectRequestLength, mockHeader);
    }
}