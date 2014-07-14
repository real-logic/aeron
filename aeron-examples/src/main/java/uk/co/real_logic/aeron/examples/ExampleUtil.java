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
package uk.co.real_logic.aeron.examples;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.mediadriver.MediaDriver;
import uk.co.real_logic.aeron.util.RateReporter;
import uk.co.real_logic.aeron.util.protocol.HeaderFlyweight;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Utility functions for examples
 */
public class ExampleUtil
{
    /**
     * Create an embedded {@link MediaDriver}
     *
     * @return {@link MediaDriver}
     * @throws Exception
     */
    public static MediaDriver createEmbeddedMediaDriver() throws Exception
    {
        final MediaDriver mediaDriver = new MediaDriver();

        mediaDriver.invokeEmbedded();

        return mediaDriver;
    }

    /**
     * Create an Aeron instance with the given context and use the given {@link ExecutorService} to spawn any
     * needed threads.
     *
     * @param context to use for instance creation
     * @param executor to use for invoking conductor thread
     * @return {@link Aeron} instance
     * @throws Exception
     */
    public static Aeron createAeron(final Aeron.ClientContext context, final ExecutorService executor) throws Exception
    {
        final Aeron aeron = Aeron.newSingleMediaDriver(context);

        aeron.invoke(executor);

        return aeron;
    }

    /**
     * Create an {@link Aeron} instance with the given context
     *
     * @param context to use for instance creation
     * @return {@link Aeron} instance
     * @throws Exception
     */
    public static Aeron createAeron(final Aeron.ClientContext context) throws Exception
    {
        final Aeron aeron = Aeron.newSingleMediaDriver(context);

        return aeron;
    }

    /**
     * Return a reusable, parameterized event loop that calls {@link Thread#yield()} when no messages are received
     *
     * @param limit passed to {@link Subscription#poll(int)}
     * @return loop function
     */
    public static Consumer<Subscription> consumerLoop(final int limit)
    {
        return (subscription) ->
        {
            try
            {
                while (true)
                {
                    final int messagesRead = subscription.poll(limit);
                    if (0 == messagesRead)
                    {
                        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100)); // if no data, then sleep for 100 millis
                    }
                }
            }
            catch (final Exception ex)
            {
                ex.printStackTrace();
            }
        };
    }

    /**
     * Return a reusable, parameterized {@link uk.co.real_logic.aeron.Subscription.DataHandler} that prints to stdout
     *
     * @param channelId to show when printing
     * @return subscription data handler function that prints the message contents
     */
    public static Subscription.DataHandler printStringMessage(final long channelId)
    {
        return (buffer, offset, length, sessionId, flags) ->
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);

            System.out.println(String.format("message to channel %d from session %d (%d@%d) <<%s>>",
                    channelId, sessionId, length, offset, new String(data)));
        };
    }

    /**
     * Return a reusable, parameteried {@link uk.co.real_logic.aeron.Subscription.DataHandler} that calls into a
     * {@link RateReporter}.
     *
     * @param reporter for the rate
     * @return {@link Subscription.DataHandler} that records the rate information
     */
    public static Subscription.DataHandler rateReporterHandler(final RateReporter reporter)
    {
        return (buffer, offset, length, sessionId, flags) -> reporter.onMessage(1, length);
    }

    /**
     * Generic error handler that just prints message to stdout.
     *
     * @param destination for the error
     * @param sessionId for the error, if source
     * @param channelId for the error
     * @param message indicating what the error was
     * @param cause of the error
     */
    public static void printError(final String destination,
                                  final long sessionId,
                                  final long channelId,
                                  final String message,
                                  final HeaderFlyweight cause)
    {
        System.out.println(message);
    }

    /**
     * Print the rates to stdout
     *
     * @param messagesPerSec being reported
     * @param bytesPerSec being reported
     */
    public static void printRate(final double messagesPerSec, final double bytesPerSec)
    {
        System.out.println(String.format("%.02g msgs/sec, %.02g bytes/sec", messagesPerSec, bytesPerSec));
    }
}
