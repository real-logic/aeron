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

import org.apache.commons.cli.ParseException;
import uk.co.real_logic.aeron.*;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.SigInt;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class ThwackerTool implements InactiveConnectionHandler, NewConnectionHandler
{
    static
    {
        if (System.getProperty("java.util.logging.SimpleFormatter.format") == null)
        {
            System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n");
        }

        /* Set some Aeron configuration so we dont blow through memory immediately */

        if (System.getProperty("aeron.dir.delete.on.exit") == null)
        {
            System.setProperty("aeron.dir.delete.on.exit", "true");
        }
        if (System.getProperty("aeron.publication.linger.timeout") == null)
        {
            System.setProperty("aeron.publication.linger.timeout", "1000");
        }
        if (System.getProperty("aeron.term.buffer.length") == null)
        {
            System.setProperty("aeron.term.buffer.length", "65536");
        }
    }

    private static final Logger LOG = Logger.getLogger(ThwackerTool.class.getName());
    private static final int FRAGMENT_LIMIT = 10;
    /* Number to retry sending a message if it fails in offer() */
    private static final int RETRY_LIMIT = 100000;
    /* Stream ID used for the control stream */
    private static final int CONTROL_SID = 9876;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 100;
    /* Latch used to know when all threads have finished */
    private CountDownLatch allDone = null;
    private MediaDriver driver = null;
    private Aeron aeron = null;
    private ThwackingElement[] pubs = null;
    private ThwackingElement[] subs = null;
    private ThwackingElement ctrlPub = null;
    private ThwackingElement ctrlSub = null;

    /* Flag signaling when to end the applications execution */
    private boolean running = false;
    /* Flag signaling worker threads to be actively "thwacking" or not */
    private boolean active = false;

    private ArrayList<Thread> thwackerThreads = null;

    /*
     *      CONFIGURABLE OPTIONS
     */
    private String channel;
    private int port;

    /* Number of thwacking elements used */
    private int numberOfPublications;
    private int numberOfSubscriptions;
    /* If true, than create a channel for each publication, else share the same one channel */
    private boolean useChannelPerPub;
    /* Creates an internal media driver */
    private boolean useEmbeddedDriver;
    /* Allows usage of only one stream ID for the entire application,
    otherwise each pub uses 0 to NUM_PUBS - 1 as a stream ID range */
    private boolean useSameStreamID;
    /* Allows each message and stream to be verifiable upon message reception */
    private boolean useVerifiableMessageStream;
    /* Number of threads creating objects (publications or subscriptions)
       Default = 1  This will create one for subs and one for pubs */
    private int createThreadCount = 1;
    /* Number of threads deleting objects (publications or subscriptions)
       Default = 1  This will create one for subs and one for pubs */
    private int deleteThreadCount = 1;
    private int senderThreadCount = 1;
    private int receiverThreadCount = 1;

    private int iterations;
    private int duration;

    private int maxSize;
    private int minSize;

    public static void main(final String[] args)
    {
        new ThwackerTool(args);
    }

    public ThwackerTool(final String[] args)
    {
        final ThwackerOptions opts = new ThwackerOptions();
        try
        {
            if (opts.parseArgs(args) != 0)
            {
                opts.printHelp("ThwackerTool");
                System.exit(-1);
            }
        }
        catch (final ParseException e)
        {
            opts.printHelp("ThwackerTool");
            System.exit(-1);
        }

        createAndInitObjects(opts);
        createAndStartThreads();
        run(duration, iterations);
        reportMessageCounts();
        cleanUp();
    }

    /**
     * createAndInitObjects():
     *  Initializes all the necessary objects used
     */
    public void createAndInitObjects(final ThwackerOptions opts)
    {
        populateOptions(opts);
        driver = useEmbeddedDriver ? MediaDriver.launch() : null;
        pubs = new ThwackingElement[numberOfPublications];
        subs = new ThwackingElement[numberOfSubscriptions];
        populateArrays();
        SigInt.register(() -> this.running = false);

        final Aeron.Context ctx = new Aeron.Context();
        /* Set timeout to be very large so the connections will not timeout when
           debugging or examining a hang.
         */
        ctx.mediaDriverTimeout(9999999999L);
        ctx.inactiveConnectionHandler(this);
        ctx.newConnectionHandler(this);

        aeron = Aeron.connect(ctx);
        active = true;
        running = true;

        final int totalWorkerThreadCount =
            createThreadCount * 2 + deleteThreadCount * 2 + senderThreadCount + receiverThreadCount;
        allDone = new CountDownLatch(totalWorkerThreadCount);

        ctrlSub = new ThwackingElement(channel + ":" + port, CONTROL_SID, useVerifiableMessageStream, true);
        ctrlPub = new ThwackingElement(channel + ":" + port, CONTROL_SID, useVerifiableMessageStream, false);
        ctrlSub.tryAddSub();
        ctrlPub.tryAddPub();

        thwackerThreads = new ArrayList<>();
    }

    public void populateOptions(final ThwackerOptions opts)
    {
        channel = opts.channel();
        port = opts.port();
        numberOfPublications = opts.elements();
        numberOfSubscriptions = opts.elements();
        useChannelPerPub = opts.channelPerPub();
        useEmbeddedDriver = opts.embeddedDriver();
        useSameStreamID = opts.sameSID();
        useVerifiableMessageStream = opts.verifiable();
        createThreadCount = opts.adders();
        deleteThreadCount = opts.removers();
        senderThreadCount = opts.senders();
        /* Receiver Threads hard coded to 1.  May add ability to create more receiving
            threads at another time */
        receiverThreadCount = 1;
        iterations = opts.iterations();
        duration = opts.duration();
        maxSize = opts.maxMsgSize();
        minSize = opts.minMsgSize();
    }


    /**
     * populateArrays()
     *  Used to populated the publication and subscription arrays with thwacking elements.
     *  This will take into account the CHANNEL_PER_PUB and SAME_SID options described above
     *  and populate the arrays with the correct values for channel and stream_id.
     */
    public void populateArrays()
    {
        int port;
        for (int i = 0; i < pubs.length; i++)
        {
            port = useChannelPerPub ? this.port + i : this.port;
            pubs[i] = new ThwackingElement(channel + ":" + port, useSameStreamID ? 0 : i, useVerifiableMessageStream, false);
        }
        for (int i = 0; i < subs.length; i++)
        {
            port = useChannelPerPub ? this.port + i : this.port;
            subs[i] = new ThwackingElement(channel + ":" + port, useSameStreamID ? 0 : i, useVerifiableMessageStream, true);
        }
    }

    public void reportMessageCounts()
    {
        int sent;
        int rcvd;
        for (int i = 0; i < pubs.length; i++)
        {
            sent = pubs[i].msgCount.get();
            LOG.info("Publication " + i + " sent messages:"  + sent);
            rcvd = subs[i].msgCount.get();
            LOG.info("Subscription " + i + " received messages:" + rcvd);
        }
        LOG.info("Control Publication sent messages:"  + ctrlPub.msgCount.get());
        LOG.info("Control Subscription received messages:"  + ctrlSub.msgCount.get());
    }

    /**
     * Submits all the jobs to the Executor
     */
    public void createAndStartThreads()
    {
        LOG.fine("Creating and starting threads");
        for (int i = 0; i < createThreadCount; i++)
        {
            thwackerThreads.add(new Thread(this::createSubs));
            thwackerThreads.add(new Thread(this::createPubs));
        }
        for (int i = 0; i < deleteThreadCount; i++)
        {
            thwackerThreads.add(new Thread(this::deleteSubs));
            thwackerThreads.add(new Thread(this::deletePubs));
        }
        for (int i = 0; i < receiverThreadCount; i++)
        {
            thwackerThreads.add(new Thread(this::receiveOnSubs));
        }
        for (int i = 0; i < senderThreadCount; i++)
        {
            thwackerThreads.add(i, new Thread(this::sendOnRandomPub));
        }
        for (int i = 0; i < thwackerThreads.size(); i++)
        {
            thwackerThreads.get(i).start();
        }
    }

    /**
     * Run function that allows threads to run for a set duration
     * If iterations is set, add/remove threads will be running for one duration then
     * will sleep for a few seconds and repeat until the set number of iterations.
     */
    public void run(final int duration, int iterations)
    {
        boolean alwaysOn = true;
        if (iterations > 1)
        {
            alwaysOn = false;
            iterations *= 2;
        }

        for (int i = 0; i < iterations; i++)
        {
            try
            {
                if (!alwaysOn)
                {
                    if (active)
                    {
                        active = false;
                        LOG.fine("Thwacking Stopped");
                        Thread.sleep(3000);
                    }
                    else
                    {
                        active = true;
                        LOG.fine("Thwacking Started");
                        Thread.sleep(duration);
                    }
                }
                else
                {
                    Thread.sleep(duration);
                }
            }
            catch (final InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        running = false;
    }

    public void cleanUp()
    {
        try
        {
            //Wait for each thwacking thread to finish
            allDone.await();
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }
        LOG.info("All finished, Closing now!");

        for (int i = 0; i < thwackerThreads.size(); i++)
        {
            try
            {
                thwackerThreads.get(i).join();
            }
            catch (final InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        cleanUpArray(pubs);
        cleanUpArray(subs);
        ctrlPub.close();
        ctrlSub.close();
        aeron.close();
        CloseHelper.quietClose(driver);
    }

    public void cleanUpArray(final ThwackingElement[] arr)
    {
        for (final ThwackingElement elem : arr)
        {
            elem.close();
        }
    }

    /**
     * Worker Thread Method:
     * Randomly selects a slot out of the publication array and creates a publication
     * if there isn't an active Publication there already
     */
    public void createPubs()
    {
        final Random rand = SeedableThreadLocalRandom.current();

        while (running)
        {
            if (active)
            {
                //Get random pub slot
                final int i = rand.nextInt(numberOfPublications);
                final ThwackingElement pub = pubs[i];
                pub.tryAddPub();
            }
            Thread.yield();
        }
        allDone.countDown();
        LOG.fine("CreatePubs all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a publication and deletes it if no other threads are using that specific publication
     */

    public void deletePubs()
    {
        final Random rand = SeedableThreadLocalRandom.current();
        int i;
        ThwackingElement pub;
        while (running)
        {
            if (active)
            {
                //Get random pub slot
                i = rand.nextInt(numberOfPublications);
                pub = pubs[i];
                pub.tryRemovePub();
            }
            Thread.yield();
        }
        allDone.countDown();
        LOG.fine("DeletePubs all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a publication, checks if it is active, then attempts to send on that publication
     */

    public void sendOnRandomPub()
    {
        final Random rand = SeedableThreadLocalRandom.current();
        int i;
        ThwackingElement pub;
        final long threadId = Thread.currentThread().getId();

        LOG.fine("Sending thread " + threadId + " started!");
        while (running)
        {
            //Get random publication slot
            i = rand.nextInt(numberOfPublications);
            pub = pubs[i];
            pub.trySendMessage(threadId);
            /* Every iteration sends on the control Publication */
            ctrlPub.trySendMessage(threadId);

            Thread.yield();
        }
        allDone.countDown();
        LOG.fine("Sending thread " + threadId + " all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a slot out of the subscription array and creates a subscription
     * if there isn't on there already
     */
    public void createSubs()
    {
        final Random rand = SeedableThreadLocalRandom.current();
        int i;
        ThwackingElement sub;
        while (running)
        {
            if (active)
            {
                i = rand.nextInt(numberOfSubscriptions);
                sub = subs[i];
                sub.tryAddSub();
            }
            Thread.yield();
        }
        allDone.countDown();
        LOG.fine("CreateSubs all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a subscription and deletes it if no other threads are using that specific subscription
     */

    public void deleteSubs()
    {
        final Random rand = SeedableThreadLocalRandom.current();
        int i;
        ThwackingElement sub;
        while (running)
        {
            if (active)
            {
                i = rand.nextInt(numberOfSubscriptions);
                sub = subs[i];
                sub.tryRemoveSub();
            }
            try
            {
                Thread.sleep(1);
            }
            catch (final InterruptedException e)
            {
                e.printStackTrace();
            }
            Thread.yield();
        }
        allDone.countDown();
        LOG.fine("DeleteSubs all done!");
    }

    /**
     * Worker thread method that will iterate over the list of subscriptions and call poll()
     */

    public void receiveOnSubs()
    {
        while (running)
        {
            /* Run through each sub slot sequentially */
            for (final ThwackingElement s : subs)
            {
                s.tryGetMessages();
            }
            Thread.yield();
            /* Every iteration calls poll() on the control subscription */
            ctrlSub.tryGetMessages();
        }
        allDone.countDown();
        LOG.fine("RecSubs all done!");
    }

    public void onInactiveConnection(
        final String channel,
        final int streamId,
        final int sessionId,
        final long position)
    {
        LOG.fine("ON INACTIVE ::: " + channel + streamId + sessionId + position);
    }

    public void onNewConnection(
        final String channel,
        final int streamId,
        final int sessionId,
        final long position,
        final String sourceIdentity)
    {
        LOG.fine("ON NEW CONNECTION ::: " + channel + streamId + sessionId + position + sourceIdentity);
    }

    /**
     * Each subscription uses a message handler to increment the total amount of messages received
     * Message data is verified if the option has been set
     */
    public class Handler
    {
        /* Reference to the subscriber that calls this handler for verification purposes */
        private final ThwackingElement sub;

        public Handler(final ThwackingElement e)
        {
            sub = e;
        }

        public void messageHandler(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            MessageStream ms = null;
            //Retrieve sending threadId
            final int verifyLength = length - Long.BYTES;
            final long threadId = buffer.getLong(offset + verifyLength);

            if (useVerifiableMessageStream)
            {
                if (MessageStream.isVerifiable(buffer, offset))
                {
                    final int sessionId = header.sessionId();
                    /* See if our cached MessageStream is the right one. */
                    if (sessionId == sub.lastSessionId && threadId == sub.lastThreadId)
                    {
                        ms = sub.cachedMessageStream;
                    }
                    if (ms == null)
                    {
                        /* Didn't have a MessageStream cached or it wasn't the right one. So do
                         * a lookup. */
                        Long2ObjectHashMap<MessageStream> threadIdMap = sub.streamMap.get(sessionId);
                        if (threadIdMap == null)
                        {
                            threadIdMap = new Long2ObjectHashMap<>();
                            sub.streamMap.put(sessionId, threadIdMap);
                        }
                        ms = threadIdMap.get(threadId);

                        if (ms == null)
                        {
                            /* Haven't set things up yet, so do so now. */
                            ms = new MessageStream();
                            threadIdMap.put(threadId, ms);
                        }
                    }
                    /* Cache for next time. */
                    sub.cachedMessageStream = ms;
                    sub.lastSessionId = sessionId;
                    sub.lastThreadId = threadId;

                    try
                    {
                        ms.putNext(buffer, offset, verifyLength);
                    }
                    catch (final Exception e)
                    {
                        /* Failed verification
                           Exclude the case where the next sequence number is 0 because this is the case
                           of a new publisher for an existing subscription and will happen many times per
                           run.
                         */
                        if (!e.getMessage().contains("but was expecting 0"))
                        {
                            LOG.warning(e.getMessage() + " StreamID" + header.streamId() + ":" + header.sessionId() + ":" +
                                threadId);
                        }
                    }
                }
            }
            sub.msgCount.incrementAndGet();
        }
    }

    /** Thwacking Element:
     *  Internal object used to contain either a publication or subscription
     *  and state associated along with it
     */
    private class ThwackingElement
    {
        //TODO Make this an interface to generalize Thwacker
        private ThreadLocal<UnsafeBuffer> buffer = null;
        private Publication pub;
        private Subscription sub;
        private MessageStream ms;
        private final String channel;
        private final int streamId;
        private final AtomicInteger msgCount;
        /* Flag indicating if this element should be using a verifiable stream */
        private final boolean verify;
        /* Active flag showing whether there is an already created pub or sub in this thwacking element */
        private AtomicBoolean isActive;
        /* Lock for create/delete and sending/receiving on a publication or subscription
         * The creation/deletion methods use the write lock to ensure exclusive access.
         * The send method is thread safe so they use the read lock to allow many threads to offer()
         * on the same pub */
        private final ReentrantReadWriteLock lock;
        private FragmentHandler msgHandler = null;
        private ThreadLocal<Boolean> previousSendFailed = null;
        private ThreadLocal<Integer> bytesSent = null;
        /* Message stream per threadId per sessionId map for message verification with multiple
           send threads.  SessionId > threadId > messageStream */
        private Int2ObjectHashMap<Long2ObjectHashMap<MessageStream>> streamMap = null;
        private MessageStream cachedMessageStream;
        private ThreadLocal<MessageStream> senderStream = null;
        private int lastSessionId;
        private long lastThreadId;


        ThwackingElement(final String chan, final int stId, final boolean verifiable, final boolean createSubscriber)
        {
            channel = chan;
            streamId = stId;
            verify = verifiable;
            buffer = ThreadLocal.withInitial(() -> new UnsafeBuffer(ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE)));
            isActive = new AtomicBoolean(false);
            lock = new ReentrantReadWriteLock();
            msgCount = new AtomicInteger(0);

            if (createSubscriber)
            {
                msgHandler = new FragmentAssemblyAdapter(new Handler(this)::messageHandler);
                streamMap = new Int2ObjectHashMap<>();
            }
            else
            {
                senderStream = new ThreadLocal<>();
                bytesSent = new ThreadLocal<>();
                previousSendFailed = ThreadLocal.withInitial(() -> false);
            }
        }

        boolean tryAddPub()
        {
            boolean added = false;
            if (lock.writeLock().tryLock())
            {
                if (!isActive.get())
                {
                    pub = aeron.addPublication(this.channel, this.streamId);
                    isActive.set(true);
                    added = true;
                    LOG.fine("Added pub " + streamId);
                }
                lock.writeLock().unlock();
            }
            return added;
        }

        boolean tryAddSub()
        {
            boolean added = false;
            if (lock.writeLock().tryLock())
            {
                if (!isActive.get())
                {
                    sub = aeron.addSubscription(this.channel, this.streamId);
                    isActive.set(true);
                    added = true;
                    LOG.fine("Added sub " + streamId);
                }
                lock.writeLock().unlock();
            }
            return added;
        }

        boolean tryRemovePub()
        {
            boolean removed = false;
            if (lock.writeLock().tryLock())
            {
                //If the publication is active, delete it
                if (isActive.get())
                {
                    pub.close();
                    isActive.set(false);
                    removed = true;
                    pub = null;
                    LOG.fine("Removed pub " + streamId);
                }
                lock.writeLock().unlock();
            }
            return removed;
        }

        boolean tryRemoveSub()
        {
            boolean removed = false;
            if (lock.writeLock().tryLock())
            {
                //If the publication is active, delete it
                if (isActive.get())
                {
                    sub.close();
                    isActive.set(false);
                    removed = true;
                    sub = null;
                    LOG.fine("Removed sub " + streamId);
                }
                lock.writeLock().unlock();
            }
            return removed;
        }

        /**
         * Attempts to call poll on a subscriber.  Will attempt to get the lock for a poll and upon failure
         * gives up and returns to the random subscription remover thread.
         * @return Amount of fragments returned from poll()
         */
        int tryGetMessages()
        {
            int rc = -1;
            if (sub != null)
            {
                /* If the sub is active, call poll() to receive messages */
                if (lock.writeLock().tryLock())
                {
                    if (isActive.get())
                    {
                        rc = sub.poll(this.msgHandler, FRAGMENT_LIMIT);
                        LOG.fine("called poll on sub " + streamId);
                    }
                    lock.writeLock().unlock();
                }
            }

            return rc;
        }

        /**
         *  Attempts to send on a publication for up to RETRY_LIMIT times before giving up and
         *  returning to the random send thread.  If the same publication is picked again after it has failed, it
         *  should retry the last message from the previous attempt.
         * @param threadId Thread ID of the calling thread to be sent with message
         * @return Returns Aeron error codes in failure or the position in success
         */

        long trySendMessage(final long threadId)
        {
            MessageStream ms = senderStream.get();
            /* See if our cached MessageStream is the right one. */
            if (ms == null)
            {
                /* Haven't set things up yet, so do so now. */
                try
                {
                    ms = new MessageStream(minSize, maxSize, verify);
                }
                catch (final Exception e)
                {
                    throw new RuntimeException(e);
                }
                senderStream.set(ms);
            }

            int retryCount = 0;
            long rc = 0;

            /* Try to acquire the send lock that allows only senders, not adders/removers */
            if (lock.readLock().tryLock())
            {
                //If this pub is active, send the buffer
                if (isActive.get())
                {
                    /* Check to see if last time we hit our RETRY_LIMIT */
                    if (!previousSendFailed.get())
                    {
                        try
                        {
                            /* Fill in the verifiable buffer then append our threadId */
                            final int sent = ms.getNext(buffer.get());
                            buffer.get().putLong(sent, threadId);
                            bytesSent.set(sent + Long.BYTES);
                        }
                        catch (final Exception e)
                        {
                            e.printStackTrace();
                        }
                    }

                    /* Actually try the offer(). If it fails, retry for RETRY_LIMIT times, then give up and
                       return.
                     */
                    do
                    {
                        try
                        {
                            rc = pub.offer(buffer.get(), 0, bytesSent.get());
                            retryCount++;
                        }
                        catch (final Exception e)
                        {
                            e.printStackTrace();
                            LOG.fine("BytesSent: " + bytesSent.get());
                        }
                    }
                    while (rc < 0 && retryCount < RETRY_LIMIT && running);

                    /* Check if send was successful or we hit the RETRY_LIMIT */
                    if (rc >= 0)
                    {
                        if (previousSendFailed.get())
                        {
                            /* A failed offer() of a message has finally completed */
                        }
                        previousSendFailed.set(false);
                        /* Send success! Increment sent messages counter */
                        msgCount.incrementAndGet();
                    }
                    else
                    {
                        /* Send failure after RETRY_LIMIT tries! Set flag to not populate the
                           buffer with new data if this publication is attempting to send again
                         */
                        previousSendFailed.set(true);
                    }
                }
                lock.readLock().unlock();
            }
            /* Return the value from offer() */
            return rc;
        }

        void close()
        {
            if (pub != null)
            {
                pub.close();
            }
            if (sub != null)
            {
                sub.close();
            }
            isActive = null;
            ms = null;
        }
    }
}
