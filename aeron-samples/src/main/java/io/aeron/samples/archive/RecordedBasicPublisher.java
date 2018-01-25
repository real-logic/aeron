/*
 * Copyright 2017 Real Logic Ltd.
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
import io.aeron.archive.status.RecordingPos;
import io.aeron.samples.SampleConfiguration;
import io.aeron.status.ReadableCounter;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.concurrent.TimeUnit;

/**
 * Basic Aeron publisher application which is recorded in an archive.
 * This publisher sends a fixed number of messages on a channel and stream ID.
 * <p>
 * The default values for number of messages, channel, and stream ID are
 * defined in {@link SampleConfiguration} and can be overridden by
 * setting their corresponding properties via the command-line; e.g.:
 * -Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20
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

        try (AeronArchive archive = AeronArchive.connect();
            Publication publication = archive.addRecordedPublication(CHANNEL, STREAM_ID))
        {
            // Wait for recording to have started before publishing.
            final CountersReader counters = archive.context().aeron().countersReader();
            int counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
            while (CountersReader.NULL_COUNTER_ID == counterId)
            {
                Thread.yield();
                counterId = RecordingPos.findCounterIdBySession(counters, publication.sessionId());
            }

            final long recordingId = RecordingPos.getRecordingId(counters, counterId);

            try
            {
                for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
                {
                    final String message = "Hello World! " + i;
                    final byte[] messageBytes = message.getBytes();
                    BUFFER.putBytes(0, messageBytes);

                    System.out.print("Offering " + i + "/" + NUMBER_OF_MESSAGES + " - ");

                    final long result = publication.offer(BUFFER, 0, messageBytes.length);

                    if (result < 0L)
                    {
                        if (result == Publication.BACK_PRESSURED)
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
                            break;
                        }
                        else if (result == Publication.MAX_POSITION_EXCEEDED)
                        {
                            System.out.println("Offer failed due to publication reaching max position");
                            break;
                        }
                        else
                        {
                            System.out.println("Offer failed due to unknown reason");
                        }
                    }
                    else
                    {
                        System.out.println("yay!");
                    }

                    if (!publication.isConnected())
                    {
                        System.out.println("No active subscribers detected");
                    }

                    Thread.sleep(TimeUnit.SECONDS.toMillis(1));
                }

                // Wait for the recording to complete before the recording is stopped.

                final ReadableCounter recordedPosition = new ReadableCounter(counters, counterId);

                final long publicationPosition = publication.position();
                while (recordedPosition.get() < publicationPosition)
                {
                    if (!RecordingPos.isActive(counters, counterId, recordingId))
                    {
                        throw new IllegalStateException("Recording has stopped unexpectedly: " + recordingId);
                    }

                    Thread.yield();
                }

                System.out.println("Done sending.");
            }
            finally
            {
                archive.stopRecording(publication);
            }
        }
    }
}

