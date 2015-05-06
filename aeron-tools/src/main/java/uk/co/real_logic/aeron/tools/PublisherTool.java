/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.apache.commons.cli.ParseException;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.InactiveConnectionHandler;
import uk.co.real_logic.aeron.NewConnectionHandler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.tools.SeedableThreadLocalRandom.SeedCallback;
import uk.co.real_logic.agrona.concurrent.SigInt;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

public class PublisherTool implements SeedCallback, RateReporter.Stats, RateReporter.Callback
{
    static
    {
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null)
        {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n");
        }
    }

    public static final String APP_USAGE = "PublisherTool";
    private static final Logger LOG = Logger.getLogger(PublisherTool.class.getName());

    private final PubSubOptions options;
    private final Thread[] pubThreads;
    private final PublisherThread[] publishers;
    private final int numThreads;
    private boolean shuttingDown;

    /**
     * Warn about options settings that might cause trouble.
     */
    private void sanityCheckOptions() throws Exception
    {
        if (options.threads() > 1)
        {
            if (options.input() != null)
            {
                LOG.warning("File data may be sent in a non-deterministic order when multiple publisher threads are used.");
            }
        }
        if (options.verify())
        {
            /* If verifiable messages are used, enforce a minimum of 16 bytes. */
            if (options.messageSizePattern().minimum() < 16)
            {
                throw new Exception("Minimum message size must be at least 16 bytes when using verifiable messages.");
            }
        }
        if (options.messageSizePattern().minimum() < 1)
        {
            throw new Exception(
                "Minimum message size must be at least 1 byte, as Aeron does not currently support 0-length messages.");
        }

    }

    public PublisherTool(final PubSubOptions options)
    {
        this.options = options;

        try
        {
            sanityCheckOptions();
        }
        catch (final Exception e)
        {
            e.printStackTrace();
            System.exit(-1);
        }

        /* Set pRNG seed callback. */
        SeedableThreadLocalRandom.setSeedCallback(this);

        /* Shut down gracefully when we receive SIGINT. */
        SigInt.register(() -> shuttingDown = true);

        /* Start embedded driver if requested. */
        MediaDriver driver = null;
        if (options.useEmbeddedDriver())
        {
            driver = MediaDriver.launch();
        }

        /* Create and start publishing threads. */
        numThreads = Math.min(options.threads(), options.numberOfStreams());
        if (numThreads < options.threads())
        {
            LOG.warning(options.threads() + " threads were requested, but only " + options.numberOfStreams() +
                " channel(s) were specified; using " + numThreads + " thread(s) instead.");
        }
        pubThreads = new Thread[numThreads];
        publishers = new PublisherThread[numThreads];
        final long messagesPerThread = options.messages() / numThreads;
        long leftoverMessages = options.messages() - (messagesPerThread * numThreads);
        for (int i = 0; i < numThreads; i++)
        {
            publishers[i] = new PublisherThread(i, messagesPerThread + ((leftoverMessages-- > 0) ? 1 : 0));
            pubThreads[i] = new Thread(publishers[i]);
            pubThreads[i].start();
        }

        final RateReporter rateReporter = new RateReporter(this, this);

        /* Wait for threads to exit. */
        try
        {
            for (int i = 0; i < pubThreads.length; i++)
            {
                pubThreads[i].join();
            }
            rateReporter.close();
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }

        /* Close the driver if we had opened it. */
        if (options.useEmbeddedDriver())
        {
            driver.close();
        }

        try
        {
            /* Close any open output files. */
            options.close();
        }
        catch (final IOException e)
        {
            e.printStackTrace();
        }

        final long verifiableMessages = verifiableMessages();
        final long nonVerifiableMessages = nonVerifiableMessages();
        final long bytesSent = bytes();
        LOG.info(String.format("Exiting. Sent %d messages (%d bytes) total. %d verifiable and %d non-verifiable.",
            verifiableMessages + nonVerifiableMessages,
            bytesSent, verifiableMessages, nonVerifiableMessages));
    }

    public static void main(final String[] args)
    {
        final PubSubOptions opts = new PubSubOptions();
        try
        {
            if (opts.parseArgs(args) != 0)
            {
                opts.printHelp(PublisherTool.APP_USAGE);
                System.exit(0);
            }
        }

        catch (final ParseException ex)
        {
            ex.printStackTrace();
            opts.printHelp(PublisherTool.APP_USAGE);
            System.exit(-1);
        }
        @SuppressWarnings("unused")
        final PublisherTool app = new PublisherTool(opts);
    }

    /**
     * Optionally sets the random seed used for the TLRandom class, and reports the seed used.
     *
     * @return the seed to use
     */
    public long setSeed(long seed)
    {
        if (options.randomSeed() != 0)
        {
            seed = options.randomSeed();
        }
        LOG.info(String.format("Thread %s using random seed %d.", Thread.currentThread().getName(), seed));
        return seed;
    }

    /**
     * A snapshot (non-atomic) total of the number of verifiable messages
     * sent across all publishing threads.
     *
     * @return current total number of verifiable messages sent
     */
    public long verifiableMessages()
    {
        long totalMessages = 0;
        for (int i = 0; i < publishers.length; i++)
        {
            totalMessages += publishers[i].verifiableMessagesSent();
        }

        return totalMessages;
    }

    /**
     * A snapshot (non-atomic) total of the number of bytes
     * sent across all publishing threads.
     *
     * @return current total number of bytes sent
     */
    public long bytes()
    {
        long totalBytes = 0;
        for (int i = 0; i < publishers.length; i++)
        {
            totalBytes += publishers[i].bytesSent();
        }

        return totalBytes;
    }

    /**
     * A snapshot (non-atomic) total of the number of non-verifiable messages
     * sent across all publishing threads.
     *
     * @return current total number of non-verifiable messages sent
     */
    public long nonVerifiableMessages()
    {
        long totalMessages = 0;
        for (int i = 0; i < publishers.length; i++)
        {
            totalMessages += publishers[i].nonVerifiableMessagesSent();
        }

        return totalMessages;
    }

    class PublisherThread implements Runnable, InactiveConnectionHandler, NewConnectionHandler, RateController.Callback
    {
        final int threadId;
        private long nonVerifiableMessagesSent;
        private long verifiableMessagesSent;
        private long bytesSent;
        private final long messagesToSend;
        private final Publication publications[];
        private final MessageStream messageStreams[];
        private int currentPublicationIndex;
        private final MessageSizePattern msp;
        private final RateController rateController;
        private final UnsafeBuffer sendBuffer;
        private final boolean verifiableMessages = options.verify();
        private final Aeron.Context ctx;
        private final Aeron aeron;

        @SuppressWarnings("resource")
        public PublisherThread(final int threadId, final long messages)
        {
            this.threadId = threadId;
            this.messagesToSend = messages;
            msp = options.messageSizePattern();
            RateController rc = null;
            try
            {
                rc = new RateController(this, options.rateIntervals(), options.iterations());
            }
            catch (final Exception e)
            {
                e.printStackTrace();
                System.exit(-1);
            }
            rateController = rc;
            sendBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(msp.maximum()));

            /* Create a context and subscribe to what we're supposed to
             * according to our thread ID. */
            ctx = new Aeron.Context()
                .inactiveConnectionHandler(this)
                .newConnectionHandler(this)
                .errorHandler(
                    (throwable) ->
                    {
                        throwable.printStackTrace();
                        if (throwable instanceof DriverTimeoutException)
                        {
                            LOG.severe("Driver does not appear to be running or has been unresponsive for ten seconds.");
                            System.exit(-1);
                        }
                    })
                .mediaDriverTimeout(10000); /* ten seconds */

            aeron = Aeron.connect(ctx);
            final ArrayList<Publication> publicationsList = new ArrayList<Publication>();

            int streamIdx = 0;
            for (int i = 0; i < options.channels().size(); i++)
            {
                final ChannelDescriptor channel = options.channels().get(i);
                for (int j = 0; j < channel.streamIdentifiers().length; j++)
                {
                    if ((streamIdx % numThreads) == this.threadId)
                    {
                        Publication pub;
                        if (options.useSessionId())
                        {
                            pub = aeron.addPublication(
                                channel.channel(),
                                channel.streamIdentifiers()[j],
                                options.sessionId());
                        }
                        else
                        {
                            // Aeron will generate a random sessionId
                            pub = aeron.addPublication(
                                channel.channel(),
                                channel.streamIdentifiers()[j]);
                        }
                        publicationsList.add(pub);

                        LOG.info(String.format("%s publishing %d messages to: %s#%d[%d]",
                            ("publisher-" + threadId), messagesToSend, pub.channel(),
                            pub.streamId(), pub.sessionId()));
                    }
                    streamIdx++;
                }
            }

            /* Send our allotted messages round-robin across our configured channels/stream IDs. */
            publications = new Publication[publicationsList.size()];
            publicationsList.toArray(publications);
            messageStreams = new MessageStream[publications.length];
            for (int i = 0; i < publications.length; i++)
            {
                try
                {
                    messageStreams[i] = new MessageStream(msp.maximum(), verifiableMessages, options.input());
                }
                catch (final Exception e)
                {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        /**
         * Get the number of bytes of all message types sent so far by this
         * individual publisher thread.
         *
         * @return the number of bytes sent by this thread
         */
        public long bytesSent()
        {
            return bytesSent;
        }

        /**
         * Gets the number of non-verifiable messages sent by this individual publisher thread.
         *
         * @return number of non-verifiable messages sent by this thread
         */
        public long nonVerifiableMessagesSent()
        {
            return nonVerifiableMessagesSent;
        }

        /**
         * Gets the number of verifiable messages sent by this individual publisher thread.
         *
         * @return number of verifiable messages sent by this thread
         */
        public long verifiableMessagesSent()
        {
            return verifiableMessagesSent;
        }

        /**
         * Publisher thread.  Creates its own Aeron context, and publishes
         * on a round-robin'd subset of the channels and stream IDs configured.
         */
        public void run()
        {
            Thread.currentThread().setName("publisher-" + threadId);

            while (!shuttingDown && rateController.next())
            {
                /* Rate controller handles sending. Stop if we
                 * hit our allotted number of messages. */
                if (rateController.messages() == messagesToSend)
                {
                    break;
                }
            }

            /* Shut down... */
            for (int i = 0; i < publications.length; i++)
            {
                publications[i].close();
            }

            aeron.close();
            ctx.close();
        }

        public void onInactiveConnection(final String channel, final int streamId,
            final int sessionId, final long position)
        {
            LOG.info(String.format("INACTIVE CONNECTION: channel \"%s\", stream %d, session %d, position 0x%x",
                channel, streamId, sessionId, position));
        }

        public void onNewConnection(final String channel, final int streamId,
            final int sessionId, final long position, final String sourceInformation)
        {
            LOG.info(String.format("NEW CONNECTION: channel \"%s\", stream %d, session %d, position 0x%x source \"%s\"",
                channel, streamId, sessionId, position, sourceInformation));
        }

        /**
         * Called by the rate controller when we should send the next message.
         * Returns the number of bytes successfully sent.
         */
        public int onNext()
        {
            int length = -1;
            boolean sendSucceeded = false;
            final Publication pub = publications[currentPublicationIndex];
            final MessageStream ms = messageStreams[currentPublicationIndex];
            currentPublicationIndex++;
            if (currentPublicationIndex == publications.length)
            {
                currentPublicationIndex = 0;
            }
            if (!ms.isActive())
            {
                /* I guess we're out of bytes - probably should only happen if we're sending a file. */
                return -1;
            }
            try
            {
                length = ms.getNext(sendBuffer, msp.getNext());
                if (length >= 0)
                {
                    while (!(sendSucceeded = (pub.offer(sendBuffer, 0, length) >= 0L)) && !shuttingDown)
                    {

                    }
                }
            }
            catch (final Exception e)
            {
                e.printStackTrace();
                System.exit(-1);
            }
            if (sendSucceeded)
            {
                if (verifiableMessages)
                {
                    verifiableMessagesSent++;
                }
                else
                {
                    nonVerifiableMessagesSent++;
                }
                bytesSent += length;
            }

            return (sendSucceeded ? length : -1);
        }

    }

    public void report(final StringBuilder reportString)
    {
        LOG.info(reportString.toString());
    }
}
