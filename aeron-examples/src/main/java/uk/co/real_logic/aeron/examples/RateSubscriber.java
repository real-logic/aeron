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
import uk.co.real_logic.aeron.common.CloseHelper;
import uk.co.real_logic.aeron.common.RateReporter;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Example that displays current rate while receiving data
 */
public class RateSubscriber
{
    public static final int CHANNEL_ID = ExampleConfiguration.CHANNEL_ID;
    public static final String DESTINATION = ExampleConfiguration.DESTINATION;
    public static final int FRAME_COUNT_LIMIT = ExampleConfiguration.FRAME_COUNT_LIMIT;
    public static final boolean EMBEDDED_MEDIA_DRIVER = ExampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    public static void main(final String[] args) throws Exception
    {
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final Aeron.ClientContext aeronContext = new Aeron.ClientContext();

        final MediaDriver driver = (EMBEDDED_MEDIA_DRIVER ? ExampleUtil.createEmbeddedMediaDriver() : null);
        final Aeron aeron = ExampleUtil.createAeron(aeronContext, executor);

        System.out.println("Subscribing to " + DESTINATION + " on channel Id " + CHANNEL_ID);

        final RateReporter reporter = new RateReporter(TimeUnit.SECONDS.toNanos(1), ExampleUtil::printRate);

        final Subscription subscription =
            aeron.addSubscription(DESTINATION, CHANNEL_ID, ExampleUtil.rateReporterHandler(reporter));

        executor.execute(() -> ExampleUtil.subscriberLoop(FRAME_COUNT_LIMIT).accept(subscription));

        // run the rate reporter loop
        reporter.run();

        CloseHelper.quietClose(aeron);
        CloseHelper.quietClose(driver);

        executor.shutdown();
    }
}
