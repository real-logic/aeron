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

import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.samples.SampleConfiguration;
import org.agrona.BufferUtil;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Basic Aeron publisher application which is recorded in an archive.
 * This publisher sends a fixed number of messages on a channel and stream ID.
 * <p>
 * The default values for number of messages, channel, and stream ID are
 * defined in {@link SampleConfiguration} and can be overridden by
 * setting their corresponding properties via the command-line; e.g.:
 * {@code -Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20}
 */
public class RecordedBasicPublisher
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final long NUMBER_OF_MESSAGES = SampleConfiguration.NUMBER_OF_MESSAGES;

    private static final UnsafeBuffer BUFFER = new UnsafeBuffer(BufferUtil.allocateDirectAligned(256, 64));

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Publishing to " + CHANNEL + " on stream Id " + STREAM_ID);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        // Create a unique response stream id so not to clash with other archive clients.
        final AeronArchive.Context archiveCtx = new AeronArchive.Context()
            .controlResponseStreamId(AeronArchive.Configuration.controlResponseStreamId() + 1);

        try (AeronArchive archive = AeronArchive.connect(archiveCtx))
        {
            archive.startRecording(CHANNEL, STREAM_ID, SourceLocation.LOCAL);

            try (Publication publication = archive.context().aeron().addPublication(CHANNEL, STREAM_ID))
            {
                // Wait for recording to have started before publishing.
                final CountersReader counters = archive.context().aeron().countersReader();
                int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                while (CountersReader.NULL_COUNTER_ID == counterId)
                {
                    if (!running.get())
                    {
                        return;
                    }

                    Thread.yield();
                    counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
                }

                final long recordingId = RecordingPos.getRecordingId(counters, counterId);
                System.out.println("Recording started: recordingId = " + recordingId);

                for (int i = 0; i < NUMBER_OF_MESSAGES && running.get(); i++)
                {
                    final String message = "Hello World! " + i;
                    final byte[] messageBytes = message.getBytes();
                    BUFFER.putBytes(0, messageBytes);

                    System.out.print("Offering " + i + "/" + NUMBER_OF_MESSAGES + " - ");

                    final long result = publication.offer(BUFFER, 0, messageBytes.length);
                    checkResult(result);

                    final String errorMessage = archive.pollForErrorResponse();
                    if (null != errorMessage)
                    {
                        throw new IllegalStateException(errorMessage);
                    }

                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                }

                while (counters.getCounterValue(counterId) < publication.position())
                {
                    if (!RecordingPos.isActive(counters, counterId, recordingId))
                    {
                        throw new IllegalStateException("recording has stopped unexpectedly: " + recordingId);
                    }

                    Thread.yield();
                }
            }
            finally
            {
                System.out.println("Done sending.");
                archive.stopRecording(CHANNEL, STREAM_ID);
            }
        }
    }

    private static void checkResult(final long result)
    {
        if (result > 0)
        {
            System.out.println("yay!");
        }
        else if (result == Publication.BACK_PRESSURED)
        {
            System.out.println("Offer failed due to back pressure");
        }
        else if (result == Publication.ADMIN_ACTION)
        {
            System.out.println("Offer failed because of an administration action in the system");
        }
        else if (result == Publication.NOT_CONNECTED)
        {
            System.out.println("Offer failed because publisher is not connected to subscriber");
        }
        else if (result == Publication.CLOSED)
        {
            System.out.println("Offer failed publication is closed");
        }
        else if (result == Publication.MAX_POSITION_EXCEEDED)
        {
            throw new IllegalStateException("Offer failed due to publication reaching max position");
        }
        else
        {
            System.out.println("Offer failed due to unknown result code: " + result);
        }
    }
}

