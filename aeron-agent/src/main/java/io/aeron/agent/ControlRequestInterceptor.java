package io.aeron.agent;

import io.aeron.logbuffer.Header;
import net.bytebuddy.asm.Advice;
import org.agrona.DirectBuffer;

import static io.aeron.agent.ArchiveEventLogger.LOGGER;

/**
 * Intercepts requests to the archive.
 */
final class ControlRequestInterceptor
{
    static class ControlRequest
    {
        @Advice.OnMethodEnter
        static void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            LOGGER.logControlRequest(buffer, offset, length);
        }
    }

    /*
    void onCloseSession(long controlSessionId);

    void onStartRecording(
        long controlSessionId,
        long correlationId,
        int streamId,
        String channel,
        SourceLocation sourceLocation);

    void onStopRecording(long controlSessionId, long correlationId, int streamId, String channel);

    void onStartReplay(
        long controlSessionId,
        long correlationId,
        long recordingId,
        long position,
        long length,
        int replayStreamId,
        String replayChannel);

    void onListRecordings(long controlSessionId, long correlationId, long fromRecordingId, int recordCount);

    void onListRecordingsForUri(
        long controlSessionId,
        long correlationId,
        long fromRecordingId,
        int recordCount,
        int streamId,
        byte[] channelFragment);

    void onListRecording(long controlSessionId, long correlationId, long recordingId);

    void onStopReplay(long controlSessionId, long correlationId, long replaySessionId);

    void onExtendRecording(
        long controlSessionId,
        long correlationId,
        long recordingId,
        int streamId,
        String channel,
        SourceLocation sourceLocation);

    void onGetRecordingPosition(long controlSessionId, long correlationId, long recordingId);

    void onTruncateRecording(long controlSessionId, long correlationId, long recordingId, long position);

    void onStopRecordingSubscription(long controlSessionId, long correlationId, long subscriptionId);

    void onGetStopPosition(long controlSessionId, long correlationId, long recordingId);

    void onFindLastMatchingRecording(
        long controlSessionId,
        long correlationId,
        long minRecordingId,
        int sessionId,
        int streamId,
        byte[] channelFragment);

    void onListRecordingSubscriptions(
        long controlSessionId,
        long correlationId,
        int pseudoIndex,
        int subscriptionCount,
        boolean applyStreamId,
        int streamId,
        String channelFragment);
        */

}