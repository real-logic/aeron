package io.aeron.cluster;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.archive.client.*;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingSignal;

import static io.aeron.cluster.ConsensusModuleSnapshotLoader.FRAGMENT_LIMIT;

final class LogReplication implements ControlEventListener, RecordingSignalConsumer
{
    private final AeronArchive archive;
    private final long replicateSessionId;
    private final RecordingSignalAdapter recordingSignalAdapter;

    private boolean isDone = false;
    private long replicatedLogPosition = AeronArchive.NULL_POSITION;
    private long logRecordingId;

    public LogReplication(
        final AeronArchive archive,
        final long srcRecordingId,
        final long dstRecordingId,
        final int srcArchiveStreamId,
        final String srcArchiveEndpoint,
        final long stopPosition)
    {
        this.archive = archive;

        final String srcArchiveChannel = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint(srcArchiveEndpoint)
            .build();

        replicateSessionId = archive.replicate(
            srcRecordingId, dstRecordingId, srcArchiveStreamId, srcArchiveChannel, null, stopPosition);

        recordingSignalAdapter = new RecordingSignalAdapter(
            archive.controlSessionId(), this, this, archive.controlResponsePoller().subscription(), FRAGMENT_LIMIT);
    }

    public boolean isDone()
    {
        return isDone;
    }

    public int doWork()
    {
        return recordingSignalAdapter.poll();
    }

    public long replicatedLogPosition()
    {
        return replicatedLogPosition;
    }

    public long recordingId()
    {
        return logRecordingId;
    }

    void close()
    {
        if (!isDone)
        {
            archive.stopRecording(replicateSessionId);
        }
    }

    public void onResponse(
        final long controlSessionId,
        final long correlationId,
        final long relevantId,
        final ControlResponseCode code,
        final String errorMessage)
    {

    }

    public void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {
        if (signal == RecordingSignal.STOP)
        {
            isDone = true;
            replicatedLogPosition = position;
            logRecordingId = recordingId;
        }
    }
}
