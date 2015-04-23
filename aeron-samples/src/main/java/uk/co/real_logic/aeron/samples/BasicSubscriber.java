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

import static uk.co.real_logic.aeron.samples.SamplesUtil.printStringMessage;

import java.util.concurrent.atomic.AtomicBoolean;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;

/**
 * This is a Basic Aeron subscriber application
 * The application subscribes to a default stream and channel. The default channel and stream
 * can be overwritten by changing the default value in {@link SampleConfiguration}. Also, the default
 * channel and stream can be changed by setting java system properties at the command line.
 * i.e. (-Daeron.sample.channel=udp://localhost:5555 -Daeron.sample.streamId=20)
 * This application only handles non-fragmented data.A Data handler method is called for every
 *  received  datagram.
 *  This application doesn't handle large fragmented messages. For fragmented message reception,
 * look at the application at {link@ MultipleSubscribersWithFragmentAssembly}
 */
public class BasicSubscriber
{
    private static final int STREAM_ID = SampleConfiguration.STREAM_ID;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final int FRAGMENT_COUNT_LIMIT = SampleConfiguration.FRAGMENT_COUNT_LIMIT;
    private static final boolean EMBEDDED_MEDIA_DRIVER = SampleConfiguration.EMBEDDED_MEDIA_DRIVER;

    public static void main(final String[] args) throws Exception
    {
        System.out.println("Subscribing to " + CHANNEL + " on stream Id " + STREAM_ID);

        final MediaDriver driver = EMBEDDED_MEDIA_DRIVER ? MediaDriver.launchEmbedded() : null;

        // Create a context for a client and specify callback methods when
        // a new connection starts (printNewConnection)
        // a connection goes inactive (printInactiveConnection)
        final Aeron.Context ctx = new Aeron.Context()
            .newConnectionHandler(SamplesUtil::printNewConnection)
            .inactiveConnectionHandler(SamplesUtil::printInactiveConnection);
        if (EMBEDDED_MEDIA_DRIVER)
        {
            ctx.dirName(driver.contextDirName());
        }

        // DataHandler method is called for every new datagram is received
        final DataHandler dataHandler = printStringMessage(STREAM_ID);
        final AtomicBoolean running = new AtomicBoolean(true);

        // Register a SIGINT handler
        SigInt.register(() -> running.set(false));

        // Create an Aeron instance with client provided context credentials
        try (final Aeron aeron = Aeron.connect(ctx);
                // Add a subscription to Aeron for a given channel and stream. Also, supply a dataHandler to
                // be called when data arrives
                final Subscription subscription = aeron.addSubscription(CHANNEL, STREAM_ID, dataHandler))
        {
            // run the subscriber thread from here
            SamplesUtil.subscriberLoop(FRAGMENT_COUNT_LIMIT, running).accept(subscription);

            System.out.println("Shutting down...");
        }

        CloseHelper.quietClose(driver);
    }
}
