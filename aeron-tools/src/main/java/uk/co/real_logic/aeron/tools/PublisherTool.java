package uk.co.real_logic.aeron.tools;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.InactiveConnectionHandler;
import uk.co.real_logic.aeron.NewConnectionHandler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.tools.TLRandom.SeedCallback;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

public class PublisherTool implements SeedCallback, RateReporter.Stats
{
    public static final String APP_USAGE = "PublisherTool";
    private static final Logger LOG = LoggerFactory.getLogger(PublisherTool.class);

    private final PubSubOptions options;
    private final Thread[] pubThreads;
    private final PublisherThread[] publishers;
    private final int numThreads;
    private boolean shuttingDown;

    /** Warn about options settings that might cause trouble. */
    private void sanityCheckOptions() throws Exception
    {
        if (options.getThreads() > 1)
        {
            if (options.getInput() != null)
            {
                LOG.warn("File data may be sent in a non-deterministic order when multiple publisher threads are used.");
            }
        }
        if (options.getVerify())
        {
            /* If verifiable messages are used, enforce a minimum of 16 bytes. */
            if (options.getMessageSizePattern().getMinimum() < 16)
            {
                throw new Exception("Minimum message size must be at least 16 bytes when using verifiable messages.");
            }
        }
        if (options.getMessageSizePattern().getMinimum() < 1)
        {
            throw new Exception(
                    "Minimum message size must be at least 1 byte, as Aeron does not currently support 0-length messages.");
        }

    }

    public PublisherTool(PubSubOptions options)
    {
        this.options = options;

        try
        {
            sanityCheckOptions();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(-1);
        }

        /* Set pRNG seed callback. */
        TLRandom.setSeedCallback(this);

        /* Shut down gracefully when we receive SIGINT. */
        SigInt.register(() -> shuttingDown = true);

        /* Start embedded driver if requested. */
        MediaDriver driver = null;
        if (options.getUseEmbeddedDriver())
        {
            driver = MediaDriver.launch();
        }

        /* Create and start publishing threads. */
        numThreads = Math.min(options.getThreads(), options.getChannels().size());
        if (numThreads < options.getThreads())
        {
            LOG.warn(options.getThreads() + " threads were requested, but only " + options.getChannels().size() +
                    " channel(s) were specified; using " + numThreads + " thread(s) instead.");
        }
        pubThreads = new Thread[numThreads];
        publishers = new PublisherThread[numThreads];
        final long messagesPerThread = options.getMessages() / numThreads;
        long leftoverMessages = options.getMessages() - (messagesPerThread * numThreads);
        for (int i = 0; i < numThreads; i++)
        {
            publishers[i] = new PublisherThread(i, messagesPerThread + ((leftoverMessages-- > 0) ? 1 : 0));
            pubThreads[i] = new Thread(publishers[i]);
            pubThreads[i].start();
        }

        RateReporter rateReporter = new RateReporter(this);

        /* Wait for threads to exit. */
        try
        {
            for (int i = 0; i < pubThreads.length; i++)
            {
                pubThreads[i].join();
            }
            rateReporter.close();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }

        /* Close the driver if we had opened it. */
        if (options.getUseEmbeddedDriver())
        {
            driver.close();
        }

        try
        {
            /* Close any open output files. */
            options.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        final long verifiableMessages = verifiableMessages();
        final long nonVerifiableMessages = nonVerifiableMessages();
        final long bytesSent = bytes();
        System.out.format("Exiting. Sent %d messages (%d bytes) total. %d verifiable and %d non-verifiable.%n",
                verifiableMessages + nonVerifiableMessages,
                bytesSent, verifiableMessages, nonVerifiableMessages);
    }

    public static void main(String[] args)
    {
        PubSubOptions opts = new PubSubOptions();
        try
        {
            if (opts.parseArgs(args) != 0)
            {
                opts.printHelp(PublisherTool.APP_USAGE);
                System.exit(0);
            }
        }

        catch (ParseException ex)
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
     * @return the seed to use
     */
    @Override
    public long setSeed(long seed)
    {
        if (options.getRandomSeed() != 0)
        {
            seed = options.getRandomSeed();
        }
        System.out.format("Thread %s using random seed %d.%n", Thread.currentThread().getName(), seed);
        return seed;
    }

    /**
     * A snapshot (non-atomic) total of the number of verifiable messages
     * received across all receiving threads.
     * @return current total number of verifiable messages received
     */
    @Override
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
     * received across all receiving threads.
     * @return current total number of bytes received
     */
    @Override
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
     * received across all receiving threads.
     * @return current total number of non-verifiable messages received
     */
    @Override
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
        private final boolean verifiableMessages = options.getVerify();
        private final Aeron.Context ctx;
        private final Aeron aeron;


        @SuppressWarnings("resource")
        public PublisherThread(int threadId, long messages)
        {
            this.threadId = threadId;
            this.messagesToSend = messages;
            msp = options.getMessageSizePattern();
            RateController rc = null;
            try
            {
                rc = new RateController(this, options.getRateIntervals(), options.getIterations());
            }
            catch (Exception e)
            {
                e.printStackTrace();
                System.exit(-1);
            }
            rateController = rc;
            sendBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(msp.getMaximum()));

            /* Create a context and subscribe to what we're supposed to
             * according to our thread ID. */
            ctx = new Aeron.Context()
            .inactiveConnectionHandler(this)
            .newConnectionHandler(this)
            .errorHandler((throwable) ->
            {
                throwable.printStackTrace();
                if (throwable instanceof DriverTimeoutException)
                {
                    System.out.println("Driver does not appear to be running or has been unresponsive for ten seconds.");
                    System.exit(-1);
                }
            })
            .mediaDriverTimeout(10000); /* ten seconds */

            aeron = Aeron.connect(ctx);
            final ArrayList<Publication> publicationsList = new ArrayList<Publication>();

            int streamIdx = 0;
            for (int i = 0; i < options.getChannels().size(); i++)
            {
                ChannelDescriptor channel = options.getChannels().get(i);
                for (int j = 0; j < channel.getStreamIdentifiers().length; j++)
                {
                    if ((streamIdx % numThreads) == this.threadId)
                    {
                        final Publication pub = aeron.addPublication(
                                channel.getChannel(), channel.getStreamIdentifiers()[j], options.getSessionId());
                        publicationsList.add(pub);

                        System.out.format("%s publishing %d messages to: %s#%d[%d]%n", Thread.currentThread().getName(),
                                messagesToSend, pub.channel(), pub.streamId(), pub.sessionId());
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
                    messageStreams[i] = new MessageStream(msp.getMaximum(), verifiableMessages, options.getInput());
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
        }

        /** Get the number of bytes of all message types sent so far by this
         * individual publisher thread.
         * @return the number of bytes sent by this thread
         */
        public long bytesSent()
        {
            return bytesSent;
        }

        /**
         * Gets the number of non-verifiable messages sent by this individual publisher thread.
         * @return number of non-verifiable messages sent by this thread
         */
        public long nonVerifiableMessagesSent()
        {
            return nonVerifiableMessagesSent;
        }

        /**
         * Gets the number of verifiable messages sent by this individual publisher thread.
         * @return number of verifiable messages sent by this thread
         */
        public long verifiableMessagesSent()
        {
            return verifiableMessagesSent;
        }

        /** Publisher thread.  Creates its own Aeron context, and publishes
         * on a round-robin'd subset of the channels and stream IDs configured.
         */
        @Override
        public void run()
        {
            Thread.currentThread().setName("publisher-" + threadId);


            while (!shuttingDown && rateController.next())
            {
                /* Rate controller handles sending. Stop if we
                 * hit our allotted number of messages. */
                if (rateController.getMessages() == messagesToSend)
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

        @Override
        public void onInactiveConnection(String channel, int streamId, int sessionId)
        {
            System.out.format("INACTIVE CONNECTION: channel \"%s\", stream %d, session %d%n", channel, streamId, sessionId);
        }

        @Override
        public void onNewConnection(String channel, int streamId, int sessionId,
                String sourceInformation)
        {
            System.out.format("NEW CONNECTION: channel \"%s\", stream %d, session %d, source \"%s\"%n",
                    channel, streamId, sessionId, sourceInformation);
        }

        /**
         * Called by the rate controller when we should send the next message.
         * Returns the number of bytes successfully sent.
         */
        @Override
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
                if (length > 0)
                {
                    while (!(sendSucceeded = pub.offer(sendBuffer, 0, length)) && !shuttingDown)
                    {

                    }
                }
            }
            catch (Exception e)
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
}
