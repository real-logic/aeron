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
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.apache.commons.cli.ParseException;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.InactiveConnectionHandler;
import uk.co.real_logic.aeron.NewConnectionHandler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.exceptions.DriverTimeoutException;
import uk.co.real_logic.aeron.exceptions.RegistrationException;
import uk.co.real_logic.aeron.tools.SeedableThreadLocalRandom.SeedCallback;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.SigInt;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.nio.ByteBuffer.allocateDirect;


public class SubscriberTool
    implements RateReporter.Stats, SeedCallback, RateReporter.Callback
{
    static
    {
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null)
        {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n");
        }
    }

    private static final Logger LOG = Logger.getLogger(SubscriberTool.class.getName());
    private boolean shuttingDown;
    private final PubSubOptions options = new PubSubOptions();
    private SubscriberThread subscribers[];

    private static final String CONTROL_CHANNEL = "udp://localhost:";
    private static final int CONTROL_PORT_START = 62777;
    private static final int CONTROL_STREAMID = 9999;

    private static final int CONTROL_ACTION_NEW_CONNECTION = 0;
    private static final int CONTROL_ACTION_INACTIVE_CONNECTION = 1;

    /* IPv6 addresses have a max string length of 45, plus port, plus "udp://", etc.
     * This should be big enough to include all of that and any reasonable Aeron-
     * specific expansions in the future. */
    private static final int CHANNEL_NAME_MAX_LEN = 256;

    public static void main(final String[] args)
    {
        final SubscriberTool subTool = new SubscriberTool();
        try
        {
            if (1 == subTool.options.parseArgs(args))
            {
                subTool.options.printHelp("SubscriberTool");
                System.exit(-1);
            }
        }
        catch (final ParseException e)
        {
            LOG.severe(e.getMessage());
            subTool.options.printHelp("SubscriberTool");
            System.exit(-1);
        }

        sanityCheckOptions(subTool.options);

        /* Set pRNG seed callback. */
        SeedableThreadLocalRandom.setSeedCallback(subTool);

        /* Shut down gracefully when we receive SIGINT. */
        SigInt.register(() -> subTool.shuttingDown = true);

        /* Start embedded driver if requested. */
        MediaDriver driver = null;
        if (subTool.options.useEmbeddedDriver())
        {
            driver = MediaDriver.launch();
        }

        /* Create and start receiving threads. */
        final Thread subThreads[] = new Thread[subTool.options.threads()];
        subTool.subscribers = new SubscriberThread[subTool.options.threads()];
        for (int i = 0; i < subTool.options.threads(); i++)
        {
            subTool.subscribers[i] = subTool.new SubscriberThread(i);
            subThreads[i] = new Thread(subTool.subscribers[i]);
            subThreads[i].start();
        }

        final RateReporter rateReporter = new RateReporter(subTool, subTool);

        /* Wait for threads to exit. */
        try
        {
            for (final Thread subThread : subThreads)
            {
                subThread.join();
            }
            rateReporter.close();
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }

        /* Close the driver if we had opened it. */
        if (null != driver)
        {
            driver.close();
        }

        try
        {
            /* Close any open output files. */
            subTool.options.close();
        }
        catch (final IOException e)
        {
            e.printStackTrace();
        }

        final long verifiableMessages = subTool.verifiableMessages();
        final long nonVerifiableMessages = subTool.nonVerifiableMessages();
        final long bytesReceived = subTool.bytes();
        LOG.info(String.format(
            "Exiting. Received %d messages (%d bytes) total. %d verifiable and %d non-verifiable.",
            verifiableMessages + nonVerifiableMessages,
            bytesReceived,
            verifiableMessages,
            nonVerifiableMessages));
    }

    /** Warn about options settings that might cause trouble. */
    private static void sanityCheckOptions(final PubSubOptions options)
    {
        if (options.threads() > 1)
        {
            if (options.output() != null)
            {
                LOG.warning("File output may be non-deterministic when multiple subscriber threads are used.");
            }
        }
    }

    /**
     * A snapshot (non-atomic) total of the number of verifiable messages
     * received across all receiving threads.
     * @return current total number of verifiable messages received
     */
    public long verifiableMessages()
    {
        long totalMessages = 0;
        for (final SubscriberThread subscriber : subscribers)
        {
            totalMessages += subscriber.verifiableMessagesReceived();
        }

        return totalMessages;
    }

    /**
     * A snapshot (non-atomic) total of the number of bytes
     * received across all receiving threads.
     * @return current total number of bytes received
     */
    public long bytes()
    {
        long totalBytes = 0;
        for (final SubscriberThread subscriber : subscribers)
        {
            totalBytes += subscriber.bytesReceived();
        }

        return totalBytes;
    }

    /**
     * A snapshot (non-atomic) total of the number of non-verifiable messages
     * received across all receiving threads.
     * @return current total number of non-verifiable messages received
     */
    public long nonVerifiableMessages()
    {
        long totalMessages = 0;
        for (final SubscriberThread subscriber : subscribers)
        {
            totalMessages += subscriber.nonVerifiableMessagesReceived();
        }

        return totalMessages;
    }

    /**
     * Optionally sets the random seed used for the TLRandom class, and reports the seed used.
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

    class SubscriberThread implements Runnable, InactiveConnectionHandler, NewConnectionHandler, RateController.Callback
    {
        final int threadId;
        private long nonVerifiableMessagesReceived;
        private long verifiableMessagesReceived;
        private long bytesReceived;
        private byte[] bytesToWrite = new byte[1];
        private final Aeron.Context ctx;
        private final Aeron aeron;
        private final UnsafeBuffer controlBuffer = new UnsafeBuffer(allocateDirect(4 + 4 + CHANNEL_NAME_MAX_LEN + 4 + 4));
        private final RateController rateController;

        /* All subscriptions we're interested in. */
        final Subscription subscriptions[];
        /* Just the subscriptions we have reason to believe might have data available - these we actually poll on. */
        final Subscription activeSubscriptions[];
        int activeSubscriptionsLength = 0;

        private final Publication controlPublication;
        private Subscription controlSubscription;
        /* Doesn't use TLRandom, since this really does need to be random and shouldn't
         * be affected by manually setting the seed. */
        private final int controlSessionId = ThreadLocalRandom.current().nextInt();
        private String controlChannel;

        /* channel -> stream ID -> session ID */
        private final HashMap<String, Int2ObjectHashMap<Int2ObjectHashMap<MessageStream>>> messageStreams = new HashMap<>();
        private int lastBytesReceived;

        @SuppressWarnings("resource")
        public SubscriberThread(final int threadId)
        {
            this.threadId = threadId;
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
            /* Create a context and connect to the media driver. */
            ctx = new Aeron.Context()
                .inactiveConnectionHandler(this)
                .newConnectionHandler(this)
                .errorHandler((throwable) ->
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

            /* Create the control publication and subscription. */
            final MessageStreamHandler controlHandler = new MessageStreamHandler("control_channel", CONTROL_STREAMID, null);
            for (int i = 0; i < 1000; i++)
            {
                /* Try 1000 ports and if we don't get one, just exit */
                controlChannel = CONTROL_CHANNEL + Integer.toString(CONTROL_PORT_START + i);
                controlHandler.channel(controlChannel);
                try
                {
                    controlSubscription = aeron.addSubscription(controlChannel, CONTROL_STREAMID, controlHandler::onControl);
                    break;
                }
                catch (RegistrationException ignore)
                {
                }
            }
            if (controlSubscription == null)
            {
                LOG.severe("Couldn't create control channel.");
                System.exit(1);
            }
            controlPublication = aeron.addPublication(controlChannel, CONTROL_STREAMID, controlSessionId);

            /* Create the subscriptionsList and populate it with just the channels this thread is supposed
             * to subscribe to. */
            final ArrayList<Subscription> subscriptionsList = new ArrayList<>();
            int streamIdx = 0;
            for (int i = 0; i < options.channels().size(); i++)
            {
                final ChannelDescriptor channel = options.channels().get(i);
                for (int j = 0; j < channel.streamIdentifiers().length; j++)
                {
                    if ((streamIdx % options.threads()) == this.threadId)
                    {
                        LOG.info(String.format("subscriber-thread %d subscribing to: %s#%d",
                            threadId, channel.channel(), channel.streamIdentifiers()[j]));

                        /* Add appropriate entries to the messageStreams map. */
                        Int2ObjectHashMap<Int2ObjectHashMap<MessageStream>> streamIdMap =
                            messageStreams.get(channel.channel());
                        if (streamIdMap == null)
                        {
                            streamIdMap = new Int2ObjectHashMap<>();
                            messageStreams.put(channel.channel(), streamIdMap);
                        }

                        Int2ObjectHashMap<MessageStream> sessionIdMap =
                            streamIdMap.get(channel.streamIdentifiers()[j]);
                        if (sessionIdMap == null)
                        {
                            sessionIdMap = new Int2ObjectHashMap<>();
                            streamIdMap.put(channel.streamIdentifiers()[j], sessionIdMap);
                        }

                        final DataHandler dataHandler = new FragmentAssemblyAdapter(new MessageStreamHandler(
                            channel.channel(), channel.streamIdentifiers()[j], sessionIdMap)::onMessage);

                        subscriptionsList.add(aeron.addSubscription(
                            channel.channel(), channel.streamIdentifiers()[j], dataHandler));
                    }
                    streamIdx++;
                }
            }

            subscriptions = new Subscription[subscriptionsList.size()];
            subscriptionsList.toArray(subscriptions);
            activeSubscriptions = new Subscription[subscriptions.length];
            activeSubscriptionsLength = 0; /* No subscriptions are in the active list to start. */
        }

        /** Get the number of bytes of all message types received so far by this
         * individual subscriber thread.
         * @return the number of bytes received by this thread
         */
        public long bytesReceived()
        {
            return bytesReceived;
        }

        /**
         * Gets the number of non-verifiable messages received by this individual subscriber thread.
         * @return number of non-verifiable messages received by this thread
         */
        public long nonVerifiableMessagesReceived()
        {
            return nonVerifiableMessagesReceived;
        }

        /**
         * Gets the number of verifiable messages received by this individual subscriber thread.
         * @return number of verifiable messages received by this thread
         */
        public long verifiableMessagesReceived()
        {
            return verifiableMessagesReceived;
        }

        /** Looks for a connection in the active subscriptions list; if it's not found, it adds it. */
        private void makeActive(final String channel, final int streamId)
        {
            for (int i = 0; i < activeSubscriptionsLength; i++)
            {
                if ((activeSubscriptions[i].streamId() == streamId)
                    && (activeSubscriptions[i].channel().equals(channel)))
                {
                    /* Already in there, nothing to do. */
                    return;
                }
            }
            /* Didn't find it, add it at the end.  Need to find it in the overall
             * list of subscriptions first. */
            Subscription sub = null;
            for (final Subscription subscription : subscriptions)
            {
                if ((subscription.streamId() == streamId)
                    && (subscription.channel().equals(channel)))
                {
                    sub = subscription;
                }
            }

            if (sub == null)
            {
                throw new RuntimeException("Tried to activate a subscription we weren't supposed to be subscribed to.");
            }
            activeSubscriptions[activeSubscriptionsLength] = sub;
            activeSubscriptionsLength++;
        }

        /** Looks for a connection in the active subscriptions list; if it's found, take it out. */
        private void makeInactive(final String channel, final int streamId)
        {
            for (int i = 0; i < activeSubscriptionsLength; i++)
            {
                if ((activeSubscriptions[i].streamId() == streamId)
                    && (activeSubscriptions[i].channel().equals(channel)))
                {
                    /* Found it; "remove" it by overwriting it with the last subscription in the array
                     * and decrementing activeSubscriptionsLength. */
                    activeSubscriptions[i] = activeSubscriptions[activeSubscriptionsLength - 1];
                    activeSubscriptionsLength--;
                    return;
                }
            }
            /* If we get this far, we tried to make inactive a subscription that wasn't active. Something's wrong. */
            throw new RuntimeException("Tried to de-activate a subscription that was not active.");
        }

        /**
         * Implements the DataHandler callback for subscribers and checks
         * any verifiable messages received.
         *
         */
        public class MessageStreamHandler
        {
            private String channel;
            private final int streamId;
            private MessageStream cachedMessageStream;
            private int lastSessionId = -1;
            private final Int2ObjectHashMap<MessageStream> sessionIdMap;
            private final OutputStream os = options.output();

            public MessageStreamHandler(
                final String channel,
                final int streamId,
                final Int2ObjectHashMap<MessageStream> sessionIdMap)
            {
                this.channel = channel;
                this.streamId = streamId;
                this.sessionIdMap = sessionIdMap;
            }

            public void channel(String channel)
            {
                this.channel = channel;
            }

            public void onControl(final DirectBuffer buffer, final int offset, final int length, final Header header)
            {
                /* Make sure this was really intended for this app - we might have a bunch
                 * running on the same machine. */
                if (header.sessionId() != controlSessionId)
                {
                    return;
                }

                final int action = buffer.getInt(offset);
                final int channelLen = buffer.getInt(offset + 4);
                final byte[] channelNameBytes = new byte[channelLen];
                buffer.getBytes(offset + 8, channelNameBytes, 0, channelLen);
                final String channel = new String(channelNameBytes);
                final int streamId = buffer.getInt(offset + 8 + channelLen);
                final int sessionId = buffer.getInt(offset + 12 + channelLen);

                if (action == CONTROL_ACTION_NEW_CONNECTION)
                {
                    LOG.info(String.format("NEW CONNECTION: channel \"%s\", stream %d, session %d",
                        channel, streamId, sessionId));

                    /* Create a new MessageStream for this connection if it doesn't already exist. */
                    final Int2ObjectHashMap<Int2ObjectHashMap<MessageStream>> streamIdMap = messageStreams.get(channel);
                    if (streamIdMap == null)
                    {
                        LOG.warning("New connection detected for channel we were not subscribed to.");
                    }
                    else
                    {
                        final Int2ObjectHashMap<MessageStream> sessionIdMap = streamIdMap.get(streamId);
                        if (sessionIdMap == null)
                        {
                            LOG.warning("New connection detected for channel we were not subscribed to.");
                        }
                        else
                        {
                            MessageStream ms = sessionIdMap.get(sessionId);
                            if (ms == null)
                            {
                                ms = new MessageStream();
                                sessionIdMap.put(sessionId, ms);
                            }
                        }
                    }

                    /* And add this channel/stream ID to the active connections list if it wasn't already in it. */
                    makeActive(channel, streamId);
                }
                else if (action == CONTROL_ACTION_INACTIVE_CONNECTION)
                {
                    LOG.info(String.format("INACTIVE CONNECTION: channel \"%s\", stream %d, session %d",
                        channel, streamId, sessionId));

                    final Int2ObjectHashMap<Int2ObjectHashMap<MessageStream>> streamIdMap = messageStreams.get(channel);
                    if (streamIdMap == null)
                    {
                        LOG.warning("Inactive connection detected for unknown connection.");
                    }
                    else
                    {
                        final Int2ObjectHashMap<MessageStream> sessionIdMap = streamIdMap.get(streamId);
                        if (sessionIdMap == null)
                        {
                            LOG.warning("Inactive connection detected for unknown connection.");
                        }
                        else
                        {
                            final MessageStream ms = sessionIdMap.get(sessionId);
                            if (ms == null)
                            {
                                LOG.warning("Inactive connection detected for unknown connection.");
                            }
                            else
                            {
                                /* Great, found the message stream.  Now get rid of it. */
                                sessionIdMap.remove(sessionId);
                                /* If that was the last sessionId we were subscribed to, go ahead and make
                                 * this connection inactive. */
                                if (sessionIdMap.isEmpty())
                                {
                                    makeInactive(channel, streamId);
                                }
                            }
                        }
                    }
                }
                else
                {
                    LOG.warning(String.format("Unknown control message type (%d) received.", action));
                }
            }

            public void onMessage(final DirectBuffer buffer, final int offset, final int length, final Header header)
            {
                bytesReceived += length;
                MessageStream ms = null;
                if (MessageStream.isVerifiable(buffer, offset))
                {
                    final int sessionId = header.sessionId();
                    verifiableMessagesReceived++;
                    /* See if our cached MessageStream is the right one. */
                    if (sessionId == lastSessionId)
                    {
                        ms = cachedMessageStream;
                    }
                    if (ms == null)
                    {
                        /* Didn't have a MessageStream cached or it wasn't the right one. So do
                         * a lookup. */
                        ms = sessionIdMap.get(sessionId);
                        if (ms == null)
                        {
                            /* Haven't set things up yet, so do so now. */
                            ms = new MessageStream();
                            sessionIdMap.put(sessionId, ms);
                        }
                    }
                    /* Cache for next time. */
                    cachedMessageStream = ms;
                    lastSessionId = sessionId;

                    try
                    {
                        ms.putNext(buffer, offset, length);
                    }
                    catch (final Exception e)
                    {
                        LOG.warning("Channel " + channel + ":" + streamId + "[" + sessionId + "]: " + e.getMessage());
                    }
                }
                else
                {
                    nonVerifiableMessagesReceived++;
                }
                /* Write the message contents (minus verifiable message header) to our output
                 * stream if set. */
                if (os != null)
                {
                    final int payloadOffset = (ms == null ? 0 : ms.payloadOffset(buffer, offset));
                    final int lengthToWrite = length - payloadOffset;
                    if (lengthToWrite > bytesToWrite.length)
                    {
                        bytesToWrite = new byte[lengthToWrite];
                    }
                    buffer.getBytes(offset + payloadOffset, bytesToWrite, 0, lengthToWrite);
                    try
                    {
                        os.write(bytesToWrite, 0, lengthToWrite);
                    }
                    catch (final IOException e)
                    {
                        e.printStackTrace();
                    }
                }

                /* See if we've received as many messages as we wanted and should now exit. */
                if ((nonVerifiableMessagesReceived + verifiableMessagesReceived) == options.messages())
                {
                    shuttingDown = true;
                }

                /* Pause a bit, if requested, to simulate a slower receiver. */
                lastBytesReceived = length;
                if (!rateController.next())
                {
                    shuttingDown = true;
                }
            }
        }

        /** Subscriber thread.  Creates its own Aeron context, and subscribes
         * on a round-robin'd subset of the channels and stream IDs configured.
         */
        public void run()
        {
            Thread.currentThread().setName("subscriber-thread " + threadId);

            final IdleStrategy idleStrategy = new BackoffIdleStrategy(
                100, 10, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(100));

            /* Poll the subscriptions until shutdown. */
            while (!shuttingDown)
            {
                int fragmentsReceived = 0;
                /* Always poll the control channel and fully drain it if
                 * there's anything there. */
                while (0 != controlSubscription.poll(1))
                {
                    Thread.yield();
                }

                for (int i = 0; i < activeSubscriptionsLength; i++)
                {
                    fragmentsReceived += activeSubscriptions[i].poll(1);
                }
                idleStrategy.idle(fragmentsReceived);
            }

            /* Shut down... */
            for (final Subscription subscription : subscriptions)
            {
                subscription.close();
            }
            controlSubscription.close();
            controlPublication.close();
            aeron.close();
            ctx.close();
        }

        private void enqueueControlMessage(final int type, final String channel, final int streamId, final int sessionId)
        {
            /* Don't deliver events for the control channel itself. */
            if ((streamId != CONTROL_STREAMID)
                || (!channel.equals(controlChannel)))
            {
                /* Enqueue the control message. */
                final byte[] channelBytes = channel.getBytes();
                controlBuffer.putInt(0, type);
                controlBuffer.putInt(4, channelBytes.length);
                controlBuffer.putBytes(8, channelBytes);
                controlBuffer.putInt(8 + channelBytes.length, streamId);
                controlBuffer.putInt(12 + channelBytes.length, sessionId);
                while (controlPublication.offer(controlBuffer, 0, 16 + channelBytes.length) < 0L)
                {
                    Thread.yield();
                }
            }
        }

        public void onInactiveConnection(
            final String channel,
            final int streamId,
            final int sessionId,
            final long position)
        {
            /* Handle processing the inactive connection notice on the subscriber thread. */
            enqueueControlMessage(CONTROL_ACTION_INACTIVE_CONNECTION, channel, streamId, sessionId);
        }

        public void onNewConnection(
            final String channel,
            final int streamId,
            final int sessionId,
            final long position,
            final String sourceInformation)
        {
            /* Handle processing the new connection notice on the subscriber thread. */
            enqueueControlMessage(CONTROL_ACTION_NEW_CONNECTION, channel, streamId, sessionId);
        }

        public int onNext()
        {
            /* Doesn't really need to do anything; just used for pausing the receiver thread a bit
             * to simulate a slow receiver.  Return the number of bytes we just received. */
            return lastBytesReceived;
        }
    }

    public void report(final StringBuilder reportString)
    {
        LOG.info(reportString.toString());
    }
}
