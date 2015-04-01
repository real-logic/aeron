/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.samples;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.RateReporter;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.protocol.HeaderFlyweight;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Utility functions for samples
 */
public class SamplesUtil
{
    /**
     * Return a reusable, parameterised event loop that calls a default idler when no messages are received
     *
     * @param limit   passed to {@link Subscription#poll(int)}
     * @param running indication for loop
     * @return loop function
     */
    public static Consumer<Subscription> subscriberLoop(final int limit, final AtomicBoolean running)
    {
        final IdleStrategy idleStrategy = new BackoffIdleStrategy(

            100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

        return subscriberLoop(limit, running, idleStrategy);
    }

    /**
     * Return a reusable, parameterized event loop that calls and idler when no messages are received
     *
     * @param limit        passed to {@link Subscription#poll(int)}
     * @param running      indication for loop
     * @param idleStrategy to use for loop
     * @return loop function
     */
    public static Consumer<Subscription> subscriberLoop(
        final int limit, final AtomicBoolean running, final IdleStrategy idleStrategy)
    {
        return
            (subscription) ->
            {
                try
                {
                    while (running.get())
                    {
                        final int fragmentsRead = subscription.poll(limit);
                        idleStrategy.idle(fragmentsRead);
                    }
                }
                catch (final Exception ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
            };
    }

    /**
     * Return a reusable, parameterized {@link DataHandler} that prints to stdout
     *
     * @param streamId to show when printing
     * @return subscription data handler function that prints the message contents
     */
    public static DataHandler printStringMessage(final int streamId)
    {
        return (buffer, offset, length, header) ->
        {
            final byte[] data = new byte[length];
            buffer.getBytes(offset, data);

            System.out.println(
                String.format(
                    "message to stream %d from session %x (%d@%d) <<%s>>",
                    streamId, header.sessionId(), length, offset, new String(data)));
        };
    }

    /**
     * Return a reusable, parameteried {@link DataHandler} that calls into a
     * {@link RateReporter}.
     *
     * @param reporter for the rate
     * @return {@link DataHandler} that records the rate information
     */
    public static DataHandler rateReporterHandler(final RateReporter reporter)
    {
        return (buffer, offset, length, header) -> reporter.onMessage(1, length);
    }

    /**
     * Generic error handler that just prints message to stdout.
     *
     * @param channel   for the error
     * @param streamId  for the error
     * @param sessionId for the error, if source
     * @param message   indicating what the error was
     * @param cause     of the error
     */
    public static void printError(
        final String channel,
        final int streamId,
        final int sessionId,
        final String message,
        final HeaderFlyweight cause)
    {
        System.out.println(message);
    }

    /**
     * Print the rates to stdout
     *
     * @param messagesPerSec being reported
     * @param bytesPerSec    being reported
     * @param totalMessages  being reported
     * @param totalBytes     being reported
     */
    public static void printRate(
        final double messagesPerSec,
        final double bytesPerSec,
        final long totalMessages,
        final long totalBytes)
    {
        System.out.println(
            String.format(
                "%.02g msgs/sec, %.02g bytes/sec, totals %d messages %d MB",
                messagesPerSec, bytesPerSec, totalMessages, totalBytes / (1024 * 1024)));
    }

    /**
     * Print the information for a new connection to stdout.
     *
     * @param channel           for the connection
     * @param streamId          for the stream
     * @param sessionId         for the connection publication
     * @param sourceInformation that is transport specific
     */
    public static void printNewConnection(
        final String channel, final int streamId, final int sessionId, final String sourceInformation)
    {
        System.out.println(
            String.format(
                "new connection on %s streamId %d sessionId %x from %s",
                channel, streamId, sessionId, sourceInformation));
    }

    /**
     * Print the information for an inactive connection to stdout.
     *
     * @param channel   for the connection
     * @param streamId  for the stream
     * @param sessionId for the connection publication
     */
    public static void printInactiveConnection(final String channel, final int streamId, final int sessionId)
    {
        System.out.println(
            String.format(
                "inactive connection on %s streamId %d sessionId %x",
                channel, streamId, sessionId));
    }
}
