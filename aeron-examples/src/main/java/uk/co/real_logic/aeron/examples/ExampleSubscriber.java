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
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.util.concurrent.atomic.AtomicBoolean;

import static uk.co.real_logic.aeron.examples.ExamplesUtil.printStringMessage;

/**
 * Example Aeron subscriber application
 */
public class ExampleSubscriber
{
    private static final int STREAM_ID = ExampleConfiguration.STREAM_ID;
    private static final String CHANNEL = ExampleConfiguration.CHANNEL;
    private static final int FRAGMENT_COUNT_LIMIT = ExampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final boolean EMBEDDED_MEDIA_DRIVER = ExampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    public static void main(final String[] args) throws Exception
    {
        ExamplesUtil.useSharedMemoryOnLinux();

        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launch() : null;

        final Aeron.Context ctx = new Aeron.Context()
            .newConnectionHandler(ExamplesUtil::printNewConnection)
            .inactiveConnectionHandler(ExamplesUtil::printInactiveConnection);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        System.out.println("Subscribing to " + CHANNEL + " on stream Id " + STREAM_ID);

        final DataHandler dataHandler = printStringMessage(STREAM_ID);
        try (final Aeron aeron = Aeron.connect(ctx);
             final Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID, dataHandler))
        {
            // run the subscriber thread from here
            ExamplesUtil.subscriberLoop(FRAGMENT_COUNT_LIMIT, running).accept(subscription);

            System.out.println("Shutting down...");
        }

        CloseHelper.quietClose(driver);
    }
}
