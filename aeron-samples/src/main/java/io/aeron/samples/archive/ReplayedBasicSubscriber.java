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
package io.aeron.samples.archive;

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
 * This is a Basic Aeron subscriber application to a recorded stream.
 * The application subscribes to a recorded of the default channel and stream ID.
 * These defaults can be overwritten by changing their value in {@link SampleConfiguration} or by
 * setting their corresponding Java system properties at the command line, e.g.:
 * -Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20
 * This application only handles non-fragmented data. A DataHandler method is called
 * for every received message or message fragment.
 * For an example that implements reassembly of large, fragmented messages, see
 * {link@ MultipleSubscribersWithFragmentAssembly}.
 */
public class ReplayedBasicSubscriber
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;

    // use a different stream id to avoid clashes
    private static final int REPLAY_STREAM_ID = SampleConfiguration.STREAM_ID + 1;

    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;

    public static void main(final String[] args)
    {
        System.out.println("Subscribing to " + CHANNEL + " on stream Id " + STREAM_ID);

        final FragmentHandler fragmentHandler = SamplesUtil.printStringMessage(STREAM_ID);
        final AtomicBoolean running = new AtomicBoolean(true);

        // Register a SIGINT handler for graceful shutdown.
        SigInt.register(() -> running.set(false));

        // Create an Aeron instance using the configured Context and create a
        // Subscription on that instance that subscribes to the configured
        // channel and stream ID.
        // The Aeron and Subscription classes implement "AutoCloseable" and will automatically
        // clean up resources when this try block is finished
        try (AeronArchive archive = AeronArchive.connect())
        {
            final long recordingId = findLatestRecording(archive, CHANNEL, STREAM_ID);
            final long position = 0L;
            final long length = Long.MAX_VALUE;

            try (Subscription subscription = archive.replay(recordingId, position, length, CHANNEL, REPLAY_STREAM_ID))
            {
                SamplesUtil.subscriberLoop(fragmentHandler, FRAGMENT_COUNT_LIMIT, running).accept(subscription);

                System.out.println("Shutting down...");
            }
        }
    }

    private static long findLatestRecording(
        final AeronArchive archive, final String expectedChannel, final int expectedStreamId)
    {
        final MutableLong foundRecordingId = new MutableLong();

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
            sourceIdentity) -> foundRecordingId.set(recordingId);

        final long fromRecordingId = 0L;
        final int recordCount = 100;

        final int foundCount = archive.listRecordingsForUri(
            fromRecordingId, recordCount, expectedChannel, expectedStreamId, consumer);

        if (foundCount == 0)
        {
            throw new IllegalStateException(
                "No recordings found for channel=" + expectedChannel + " streamId=" + expectedStreamId);
        }

        return foundRecordingId.get();
    }
}
