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

import static uk.co.real_logic.aeron.samples.SamplesUtil.rateReporterHandler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.RateReporter;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;

/**
 * Example that displays current rate while receiving data
 */
public class RateSubscriber
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Subscribing to " + CHANNEL + " on stream Id " + STREAM_ID);

        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launch() : null;
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        // Create a context with newConnectionHandler and inactiveConnectionHandler
        final Aeron.Context ctx = new Aeron.Context()
            .newConnectionHandler(SamplesUtil::printNewConnection)
            .inactiveConnectionHandler(SamplesUtil::printInactiveConnection);

        // Create a rate reporter which will call reporter function every one second
        final RateReporter reporter = new RateReporter(TimeUnit.SECONDS.toNanos(1), SamplesUtil::printRate);

        // Create a data handler to be called when a message is received
        final DataHandler rateReporterHandler = new FragmentAssemblyAdapter(rateReporterHandler(reporter));

        final AtomicBoolean running = new AtomicBoolean(true);
        // Register an SIGINT handler
        SigInt.register(
            () ->
            {
                reporter.halt();
                running.set(false);
            });
        // Add a subscriber to receive data from CHANNEL and STREAM_ID
        try (final Aeron aeron = Aeron.connect(ctx, executor);
             final Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID, rateReporterHandler))
        {
            // Receive Data at subscriber in a separate thread

            final Future future = executor.submit(
                () -> SamplesUtil.subscriberLoop(FRAGMENT_COUNT_LIMIT, running).accept(subscription));

            // run the rate reporter loop
            reporter.run();

            System.out.println("Shutting down...");
            future.get();
        }

        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS))
        {
            System.out.println("Warning: not all tasks completed promptly");
        }

        CloseHelper.quietClose(driver);
    }
}
