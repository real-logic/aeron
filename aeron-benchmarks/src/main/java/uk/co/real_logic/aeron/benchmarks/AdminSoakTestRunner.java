/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.benchmarks;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.DataHandler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.ByteOrder.nativeOrder;

/**
 * Goal: test for resource leaks in the control/admin side of things.
 *
 * For infinite amount of time:
 *  1. Create new clients
 *  2. Send a few messages
 *  3. Close the clients down.
 */
public class AdminSoakTestRunner
{
    private static final String CHANNEL = "udp://localhost:40123";
    private static final int STREAM_ID = 10;
    private static final int MESSAGES_PER_RUN = 10;

    private static final ExecutorService executor = Executors.newFixedThreadPool(2);
    private static final AtomicBuffer publishingBuffer = new AtomicBuffer(ByteBuffer.allocateDirect(256));

    public static void main(String[] args) throws Exception
    {
        final MediaDriver driver = new MediaDriver();
        driver.invokeEmbedded();

        while (true)
        {
            exchangeMessagesBetweenClients();

            Thread.sleep(1);
        }
    }

    private static void exchangeMessagesBetweenClients()
    {
        publishingBuffer.setMemory(0, publishingBuffer.capacity(), (byte) 0);

        final Aeron publishingClient = Aeron.newClient(new Aeron.Context());
        final Aeron consumingClient = Aeron.newClient(new Aeron.Context());

        consumingClient.invoke(executor);
        publishingClient.invoke(executor);

        try (final Publication publication = publishingClient.addPublication(CHANNEL, STREAM_ID, 0))
        {
            AtomicInteger receivedCount = new AtomicInteger(0);

            final DataHandler handler =
                (buffer, offset, length, sessionId, flags) ->
                {
                    final int expectedValue = receivedCount.get();
                    final int value = buffer.getInt(offset, nativeOrder());
                    if (value != expectedValue)
                    {
                        System.err.println("Unexpected value: " + value);
                    }
                    receivedCount.incrementAndGet();
                };

            try (final Subscription subscription = consumingClient.addSubscription(CHANNEL, STREAM_ID, handler))
            {
                for (int i = 0; i < MESSAGES_PER_RUN; i++)
                {
                    publishingBuffer.putInt(0, i, nativeOrder());
                    if (!publication.offer(publishingBuffer))
                    {
                        System.err.println("Unable to send " + i);
                    }
                }

                int read = 0;
                do
                {
                    read += subscription.poll(MESSAGES_PER_RUN);
                }
                while (read < MESSAGES_PER_RUN);

                if (read > MESSAGES_PER_RUN)
                {
                    System.err.println("We've read too many messages: " + read);
                }

                if (receivedCount.get() != MESSAGES_PER_RUN)
                {
                    System.err.println("Data Handler has " + read + " messages");
                }
            }
        }

        shutdownAndClose(consumingClient);
        shutdownAndClose(publishingClient);
    }

    public static void shutdownAndClose(final Aeron aeron)
    {
        try
        {
            aeron.shutdown();
            aeron.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


}
