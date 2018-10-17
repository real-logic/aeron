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
package io.aeron.samples.archive;

import io.aeron.ChannelUri;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.samples.SampleConfiguration;
import io.aeron.samples.SamplesUtil;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.SigInt;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A basic subscriber application which requests a replay from the archive and consumes it.
 */
public class ReplayedBasicSubscriber
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;

    // Use a different stream id to avoid clash with live stream
    private static final int REPLAY_STREAM_ID = SampleConfiguration.STREAM_ID + 1;

    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    public static void main(final String[] args)
    {
        System.out.println("Subscribing to " + CHANNEL + " on stream Id " + STREAM_ID);

        final FragmentHandler fragmentHandler = SamplesUtil.printStringMessage(STREAM_ID);
        final AtomicBoolean running = new AtomicBoolean(true);

        SigInt.register(() -> running.set(false));

        // Create a unique response stream id so not to clash with other archive clients.
        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlResponseStreamId(AeronArchive.Configuration.controlResponseStreamId() + 2);

        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            final long recordingId = findLatestRecording(archive);
            final long position = 0L;
            final long length = Long.MAX_VALUE;

            final long sessionId = archive.startReplay(recordingId, position, length, CHANNEL, REPLAY_STREAM_ID);
            final String channel = ChannelUri.addSessionId(CHANNEL, (int)sessionId);

            try (Subscription subscription = archive.context().aeron().addSubscription(channel, REPLAY_STREAM_ID))
            {
                SamplesUtil.subscriberLoop(fragmentHandler, FRAGMENT_COUNT_LIMIT, running).accept(subscription);
                System.out.println("Shutting down...");
            }
        }
    }

    private static long findLatestRecording(final AeronArchive archive)
    {
        final MutableLong lastRecordingId = new MutableLong();

        final RecordingDescriptorConsumer consumer =
            (controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity) -> lastRecordingId.set(recordingId);

        final long fromRecordingId = 0L;
        final int recordCount = 100;

        final int foundCount = archive.listRecordingsForUri(fromRecordingId, recordCount, CHANNEL, STREAM_ID, consumer);

        if (foundCount == 0)
        {
            throw new IllegalStateException("no recordings found");
        }

        return lastRecordingId.get();
    }
}
