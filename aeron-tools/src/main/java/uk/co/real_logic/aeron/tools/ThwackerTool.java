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

/**
 * Created by mike on 3/31/2015.
 */

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.InactiveConnectionHandler;
import uk.co.real_logic.aeron.NewConnectionHandler;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.SigInt;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;




public class ThwackerTool implements InactiveConnectionHandler, NewConnectionHandler
{
    static
    {
        /* Turn off some of the default clutter of the default logger if the
         * user hasn't explicitly turned it back on. */
        if (System.getProperty("org.slf4j.simpleLogger.showThreadName") == null)
        {
            System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        }
        if (System.getProperty("org.slf4j.simpleLogger.showLogName") == null)
        {
            System.setProperty("org.slf4j.simpleLogger.showLogName", "false");
        }
        if (System.getProperty("org.slf4j.simpleLogger.defaultLogLevel") == null)
        {
            /* Change this property to debug to include debug output */
            System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        }
    }

    private final ThwackerOptions options = null;
    private static final Logger LOG = LoggerFactory.getLogger(ThwackerTool.class);
    private static final int FRAGMENT_LIMIT = 10;
    /* Number to retry sending a message if it fails in offer() */
    private static final int RETRY_LIMIT = 100000;
    /* Stream ID used for the control stream */
    private static final int CONTROL_SID = 9876;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 10;
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
    /* ExecutorService that handles the running of worker threads */
    private ExecutorService runStuffs = null;

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
        final ThwackerTool app = new ThwackerTool(args);
    }

    public ThwackerTool(final String[] args)
    {

        final ThwackerOptions opts = new ThwackerOptions();
        try
        {
            if (opts.parseArgs(args) != 0)
            {
                //TODO: catch this
                LOG.error("Parse args failed");
            }
        }
        catch (final ParseException e)
        {
            e.printStackTrace();
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
        ctx.mediaDriverTimeout(9999999999L);
        ctx.inactiveConnectionHandler(this);
        ctx.newConnectionHandler(this);

        aeron = Aeron.connect(ctx);
        active = true;
        running = true;

        final int totalWorkerThreadCount = createThreadCount * 2 + deleteThreadCount * 2 + senderThreadCount +
                receiverThreadCount;
        allDone = new CountDownLatch(totalWorkerThreadCount);
        runStuffs = Executors.newFixedThreadPool(totalWorkerThreadCount);

        ctrlSub = new ThwackingElement(channel + port, CONTROL_SID, useVerifiableMessageStream, true);
        ctrlPub = new ThwackingElement(channel + port, CONTROL_SID, useVerifiableMessageStream, false);
        ctrlSub.tryAddSub();
        ctrlPub.tryAddPub();

    }

    public void populateOptions(final ThwackerOptions opts)
    {
        channel = opts.getChannel();
        port = opts.getPort();
        numberOfPublications = opts.getElements();
        numberOfSubscriptions = opts.getElements();
        useChannelPerPub = opts.getChannelPerPub();
        useEmbeddedDriver = opts.getEmbeddedDriver();
        useSameStreamID = opts.getSameSID();
        useVerifiableMessageStream = opts.getVerifiable();
        createThreadCount = opts.getAdders();
        deleteThreadCount = opts.getRemovers();
        senderThreadCount = opts.getSenders();
        /* Receiver Threads hard coded to 1.  May add ability to create more receiving
            threads at another time */
        receiverThreadCount = 1;
        iterations = opts.getIterations();
        duration = opts.getDuration();
        maxSize = opts.getMaxMsgSize();
        minSize = opts.getMinMsgSize();
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
            pubs[i] = new ThwackingElement(channel + port, useSameStreamID ? 0 : i, useVerifiableMessageStream, false);
        }
        for (int i = 0; i < subs.length; i++)
        {
            port = useChannelPerPub ? this.port + i : this.port;
            subs[i] = new ThwackingElement(channel + port, useSameStreamID ? 0 : i, useVerifiableMessageStream, true);
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
        LOG.debug("Creating and starting threads");
        for (int i = 0; i < createThreadCount; i++)
        {
            runStuffs.submit(() -> createSubs());
            runStuffs.submit(() -> createPubs());
        }
        for (int i = 0; i < deleteThreadCount; i++)
        {
            runStuffs.submit(() -> deleteSubs());
            runStuffs.submit(() -> deletePubs());
        }
        for (int i = 0; i < receiverThreadCount; i++)
        {
            runStuffs.submit(() -> receiveOnSubs());
        }
        for (int i = 0; i < senderThreadCount; i++)
        {
            runStuffs.submit(() -> sendOnRandomPub());
        }

    }


    /**
     * Run function that allows threads to run for a set duration
     * If iterations is set, add/remove threads will be running for one duration then
     * will sleep for a few seconds and repeat until the set number of iterations.
     * @param duration
     * @param iterations
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
                        LOG.debug("Thwacking OFF");
                        Thread.sleep(3000);
                    }
                    else
                    {
                        active = true;
                        LOG.debug("Thwacking ON");
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

        cleanUpArray(pubs);
        cleanUpArray(subs);
        ctrlPub.close();
        ctrlSub.close();
        aeron.close();
        runStuffs.shutdown();
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
     * if there isn't on there already
     */
    public void createPubs()
    {
        final Random rand = new Random();
        int i = -1;
        ThwackingElement pub;
        while (running)
        {
            if (active)
            {
                //Get random pub slot
                i = rand.nextInt(numberOfPublications);
                pub = pubs[i];
                pub.tryAddPub();
            }
            Thread.yield();
        }
        allDone.countDown();
        LOG.debug("CreatePubs all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a publication and deletes it if no other threads are using that specific publication
     */

    public void deletePubs()
    {
        final Random rand = new Random();
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
        LOG.debug("DeletePubs all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a publication, checks if it is active, then attempts to send on that publication
     */

    public void sendOnRandomPub()
    {
        final Random rand = new Random();
        int i;
        ThwackingElement pub;
        final long threadId = Thread.currentThread().getId();

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
        LOG.debug("Sending thread all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a slot out of the subscription array and creates a subscription
     * if there isn't on there already
     */
    public void createSubs()
    {
        final Random rand = new Random();
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
        LOG.debug("CreateSubs all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a subscription and deletes it if no other threads are using that specific subscription
     */

    public void deleteSubs()
    {
        final Random rand = new Random();
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
            Thread.yield();
        }
        allDone.countDown();
        LOG.debug("DeleteSubs all done!");
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
        LOG.debug("RecSubs all done!");
    }

    @Override
    public void onInactiveConnection(final String channel, final int streamId,
                                     final int sessionId, final long position)
    {
        //LOG.error("ON INACTIVE ::: " + channel + streamId + sessionId + position);
    }

    @Override
    public void onNewConnection(final String channel, final int streamId, final int sessionId,
                                final long position, final String sourceInformation)
    {
        //LOG.error("ON NEW CONNECTION ::: " + channel + streamId + sessionId + position + sourceInformation);
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

        public void messageHandler(final DirectBuffer buffer, final int offset, final int length,
                                   final Header header)
        {
            MessageStream ms = null;
            //Retrieve sending threadId
            final int verifyLength = length - Long.SIZE;
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
                            threadIdMap = new Long2ObjectHashMap<MessageStream>();
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
                        /* Failed verification */
                        LOG.warn(e.getMessage() + " StreamID" + header.streamId() + ":" + header.sessionId() + ":" + threadId);
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
    public class ThwackingElement
    {
        //TODO Make this an interface to generalize Thwacker
        private final UnsafeBuffer buffer;
        private Publication pub;
        private Subscription sub;
        private MessageStream ms;
        private final String channel;
        private final int streamId;
        private final int sessionId = 0;
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
        private DataHandler msgHandler = null;
        private boolean previousSendFailed = false;
        private int bytesSent = 0;
        /* Message stream to threadId map for the sending side */
        private Long2ObjectHashMap<MessageStream> threadIdMap = null;
        /* Message stream per threadId per sessionId map for message verification with multiple
           send threads.  SessionId > threadId > messageStream */
        private Int2ObjectHashMap<Long2ObjectHashMap<MessageStream>> streamMap = null;
        private MessageStream cachedMessageStream;
        private int lastSessionId;
        private long lastThreadId;


        ThwackingElement(final String chan, final int stId, final boolean verifiable, final boolean createSubscriber)
        {
            channel = chan;
            streamId = stId;
            verify = verifiable;
            buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE));
            isActive = new AtomicBoolean(false);
            lock = new ReentrantReadWriteLock();
            msgCount = new AtomicInteger(0);
            if (createSubscriber)
            {
                msgHandler = new FragmentAssemblyAdapter(new Handler(this)::messageHandler);
                streamMap = new Int2ObjectHashMap<Long2ObjectHashMap<MessageStream>>();
            }
            else
            {
                threadIdMap = new Long2ObjectHashMap<MessageStream>();
            }
        }

        boolean tryAddPub()
        {
            boolean added = false;
            if (lock.writeLock().tryLock())
            {
                if (!isActive.get())
                {
                    //createMessageStream();
                    pub = aeron.addPublication(this.channel, this.streamId);
                    isActive.set(true);
                    added = true;
                    LOG.debug("Added pub " + streamId);
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
                    //createMessageStream();
                    sub = aeron.addSubscription(this.channel, this.streamId, this.msgHandler);
                    isActive.set(true);
                    added = true;
                    LOG.debug("Added sub " + streamId);
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
                    threadIdMap.clear();
                    isActive.set(false);
                    removed = true;
                    pub = null;
                    LOG.debug("Removed pub " + streamId);

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
                    //sessionIdMap.clear();
                    isActive.set(false);
                    removed = true;
                    sub = null;
                    LOG.debug("Removed sub " + streamId);
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
                        rc = sub.poll(FRAGMENT_LIMIT);
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
            MessageStream ms = null;
            /* See if our cached MessageStream is the right one. */
            if (threadId == lastThreadId)
            {
                ms = cachedMessageStream;
            }
            if (ms == null)
            {
                /* Didn't have a MessageStream cached or it wasn't the right one. So do
                 * a lookup. */
                ms = threadIdMap.get(threadId);
                if (ms == null)
                {
                    /* Haven't set things up yet, so do so now. */
                    try
                    {
                        ms = new MessageStream(minSize, maxSize, verify);
                    }
                    catch (final Exception e)
                    {
                        e.printStackTrace();
                    }
                    threadIdMap.put(threadId, ms);
                }
            }

            /* Cache for next time. */
            cachedMessageStream = ms;
            lastThreadId = threadId;

            int retryCount = 0;
            long rc = 0;

            /* Try to acquire the send lock that allows only senders, not adders/removers */
            if (lock.readLock().tryLock())
            {
                //If this pub is active, send the buffer
                if (isActive.get())
                {
                    /* Check to see if last time we hit our RETRY_LIMIT */
                    if (!previousSendFailed)
                    {
                        try
                        {
                            /* Fill in the verifiable buffer then append our threadId */
                            bytesSent = ms.getNext(buffer);
                            buffer.putLong(bytesSent, threadId);
                            bytesSent += Long.SIZE;
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
                        rc = pub.offer(buffer, 0, bytesSent);
                        retryCount++;
                    }
                    while (rc < 0 && retryCount < RETRY_LIMIT && running);

                    /* Check if send was successful or we hit the RETRY_LIMIT */
                    if (rc >= 0)
                    {
                        if (previousSendFailed)
                        {
                            /* A failed offer() of a message has finally completed */
                        }
                        previousSendFailed = false;
                        /* Send success! Increment sent messages counter */
                        msgCount.incrementAndGet();
                    }
                    else
                    {
                        /* Send failure after RETRY_LIMIT tries! Set flag to not populate the
                           buffer with new data if this publication is attempting to send again
                         */
                        previousSendFailed = true;
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
