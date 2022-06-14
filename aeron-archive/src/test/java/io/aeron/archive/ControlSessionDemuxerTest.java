package io.aeron.archive;

import io.aeron.Image;
import io.aeron.archive.codecs.ConnectRequestEncoder;
import io.aeron.archive.codecs.MessageHeaderEncoder;
import io.aeron.archive.codecs.ReplicateRequest2Encoder;
import io.aeron.logbuffer.Header;
import io.aeron.security.AuthorisationService;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ControlSessionDemuxerTest
{

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

        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
        final ReplicateRequest2Encoder replicateRequest2Encoder = new ReplicateRequest2Encoder();
        final ConnectRequestEncoder connectRequestEncoder = new ConnectRequestEncoder();
        final long sessionId = 928374L;

        connectRequestEncoder.wrapAndApplyHeader(buffer, 0, headerEncoder);
        connectRequestEncoder
            .correlationId(100)
            .responseStreamId(100)
            .version(SCHEMA_VERSION_6)
            .responseChannel("foo");
        final int connectRequestLength = connectRequestEncoder.encodedLength();

        doReturn(mockSession).when(mockConductor).newControlSession(anyLong(), anyInt(), anyInt(), any(), any(), any());
        doReturn(sessionId).when(mockSession).sessionId();
        doReturn(true).when(mockAuthorisationService).isAuthorised(anyInt(), anyInt(), any(), any());

        controlSessionDemuxer.onFragment(buffer, 0, connectRequestLength, mockHeader);

        replicateRequest2Encoder.wrapAndApplyHeader(buffer, 0, headerEncoder);

        final long correlationId = 9382475L;
        final long srcRecordingId = 1234234L;
        final long dstRecordingId = 2532453245L;
        final long stopPosition = 2315345L;
        final long channelTagId = 234L;
        final long subscriptionTagId = 235L;
        final int fileIoMaxLength = 4096;
        final int srcControlStreamId = 982374;
        final String srcControlChannel = "src";
        final String liveDestination = "live";
        final String replicationChannel = "replication";

        replicateRequest2Encoder
            .controlSessionId(sessionId)
            .correlationId(correlationId)
            .srcRecordingId(srcRecordingId)
            .dstRecordingId(dstRecordingId)
            .stopPosition(stopPosition)
            .channelTagId(channelTagId)
            .subscriptionTagId(subscriptionTagId)
            .fileIoMaxLength(fileIoMaxLength)
            .srcControlStreamId(srcControlStreamId)
            .srcControlChannel(srcControlChannel)
            .liveDestination(liveDestination)
            .replicationChannel(replicationChannel);

        final int replicateRequestLength = replicateRequest2Encoder.encodedLength();

        controlSessionDemuxer.onFragment(buffer, 0, replicateRequestLength, mockHeader);

        verify(mockSession).onReplicate(
            correlationId,
            srcRecordingId,
            dstRecordingId,
            stopPosition,
            channelTagId,
            subscriptionTagId,
            srcControlStreamId,
            fileIoMaxLength, srcControlChannel,
            liveDestination,
            replicationChannel
        );
    }
}