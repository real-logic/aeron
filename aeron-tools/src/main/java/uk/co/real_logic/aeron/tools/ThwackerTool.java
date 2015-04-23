package uk.co.real_logic.aeron.tools;

/**
 * Created by mike on 3/31/2015.
 */

import org.apache.commons.cli.*;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.CloseHelper;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.lang.System.exit;


public class ThwackerTool
{

    private final ThwackerOptions options = null;
    /* Buffer used to hold messages for passing into publication.offer() */
    private static final UnsafeBuffer CONTROL_BUFFER = new UnsafeBuffer(ByteBuffer.allocateDirect(1024 * 10));
    private static final int FRAGMENT_LIMIT = 10;
    /* Stream ID used for the control stream */
    private static final int CONTROL_SID = 9876;
    private static final int MESSAGE_MIN_SIZE = 35;
    private static final int MESSAGE_MAX_SIZE = 35;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 10;
    /* Latch used to know when all threads have finished */
    private static CountDownLatch allDone = null;
    private static MediaDriver driver = null;
    private static Aeron aeron = null;
    private static ThwackingElement[] pubs = null;
    private static ThwackingElement[] subs = null;
    private static Publication controlPub = null;
    private static Subscription controlSub = null;
    private static MessageStream controlPublisherMessageStream = null;
    private static MessageStream controlSubscriberMessageStream = null;

    /* Flag signaling when to end the applications execution */
    private static AtomicBoolean running = null;
    /* Flag signaling worker threads to be actively "thwacking" or not */
    private static AtomicBoolean active = null;
    /* Global message count for thwacked subs to verify we get messages */
    private static AtomicInteger msgCount = null;
    /* Message count of messages received on the control stream */
    private static AtomicInteger controlMsgCount = null;
    /* Map used for mapping streams to subscriptions, used for verfiable messages */
    private static Int2ObjectHashMap<MessageStream> sessionIdMap = null;
    private static MessageStream cachedStream = null;
    private static int lastSessionId = 0;

    /* ExecutorService that handles the running of worker threads */
    static ExecutorService runStuffs = null;

    /*
            OPTIONS
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

    /* Number of threads creating objects (publications or subscriptions
   Default = 1  This will create one for subs and one for PUBS */
    private int createThreadCount = 1;
    /* Number of threads deleting objects (publications or subscriptions
       Default = 1  This will create one for subs and one for PUBS */
    private int deleteThreadCount = 1;
    private int senderThreadCount = 1;
    private int receiverThreadCount = 1;

    private int iterations;
    private int duration;

    public static void main(final String[] args)
    {
        final ThwackerTool app = new ThwackerTool(args);
    }

    public ThwackerTool(String[] args)
    {

        final ThwackerOptions opts = new ThwackerOptions();
        try
        {
            if (opts.parseArgs(args) != 0)
            {
                //TODO: catch this
                System.out.println("hey we failed!");
            }
        }
        catch (ParseException e)
        {
            e.printStackTrace();
        }

        createAndInitObjects(opts);
        createAndStartThreads();
        run(duration, iterations);
        cleanUp();
    }




    /**
     * createAndInitObjects():
     *  Initializes all the necessary objects used
     */
    public void createAndInitObjects(ThwackerOptions opts)
    {
        populateOptions(opts);
        driver = useEmbeddedDriver ? MediaDriver.launch() : null;
        pubs = new ThwackingElement[numberOfPublications];
        subs = new ThwackingElement[numberOfSubscriptions];
        populateArrays();
        SigInt.register(() -> exit(1));
        //dataHandler = new FragmentAssemblyAdapter(new Handlers()::handler);
        DataHandler controlDataHandler = new FragmentAssemblyAdapter(new Handlers()::controlHandler);
        final Aeron.Context ctx = new Aeron.Context();
        ctx.mediaDriverTimeout(99999999999L);
        aeron = Aeron.connect(ctx);
        msgCount = new AtomicInteger(0);
        controlMsgCount = new AtomicInteger(0);
        active = new AtomicBoolean(true);
        running = new AtomicBoolean(true);
        sessionIdMap = new Int2ObjectHashMap<MessageStream>();

        final int totalWorkerThreadCount = createThreadCount * 2 + deleteThreadCount * 2 + senderThreadCount +
                receiverThreadCount;
        allDone = new CountDownLatch(totalWorkerThreadCount);
        runStuffs = Executors.newFixedThreadPool(totalWorkerThreadCount);

        controlSub = aeron.addSubscription(channel + port, CONTROL_SID, controlDataHandler);
        controlPub = aeron.addPublication(channel + port, CONTROL_SID);
        try
        {
            controlPublisherMessageStream = new MessageStream(MESSAGE_MIN_SIZE, MESSAGE_MAX_SIZE, useVerifiableMessageStream);
            controlSubscriberMessageStream = new MessageStream(MESSAGE_MIN_SIZE, MESSAGE_MAX_SIZE, useVerifiableMessageStream);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    public void populateOptions(ThwackerOptions opts)
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
        receiverThreadCount = opts.getReceivers();
        iterations = opts.getIterations();
        duration = opts.getDuration();
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
            pubs[i] = new ThwackingElement(channel + port, useSameStreamID ? 0 : i, useVerifiableMessageStream);
        }
        for (int i = 0; i < subs.length; i++)
        {
            port = useChannelPerPub ? this.port + i : this.port;
            subs[i] = new ThwackingElement(channel + port, useSameStreamID ? 0 : i, useVerifiableMessageStream);
        }
    }

    /**
     * Submits all the jobs to the Executor
     */
    public void createAndStartThreads()
    {
        System.out.println("Creating and starting threads");
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
    public void run(int duration, int iterations)
    {
        boolean alwaysOn = true;
        if (iterations > 1)
        {
            alwaysOn = false;
            iterations *= 2;
        }


        for (int i = 0; i < iterations; i++)
        {
            System.out.println(i + ": Thwacking!!! Rcvd messages: " + msgCount.get());
            System.out.println("  Control messages received: " + controlMsgCount.get());
            try
            {
                if (!alwaysOn)
                {

                    if (active.get())
                    {
                        active.set(false);
                        System.out.println("Thwacking OFF");
                        Thread.sleep(3000);
                    }
                    else
                    {
                        active.set(true);
                        System.out.println("Thwacking ON");
                        Thread.sleep(duration);
                    }
                }
                else
                {
                    Thread.sleep(duration);
                }
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
        System.out.println("Thwacking!!! Rcvd messages: " + msgCount.get());
        System.out.println("  Control messages received: " + controlMsgCount.get());
        running.set(false);

    }

    public void cleanUp()
    {
        try
        {
            //Wait for each thwacking thread to finish
            allDone.await();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        System.out.println("Closing now!");

        cleanUpArray(pubs);
        cleanUpArray(subs);
        controlPub.close();
        controlSub.close();
        aeron.close();
        runStuffs.shutdown();
        CloseHelper.quietClose(driver);

    }

    public void cleanUpArray(ThwackingElement[] arr)
    {
        for (ThwackingElement elem : arr)
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
        while (running.get())
        {
            if (active.get())
            {
                //Get random pub slot
                i = rand.nextInt(numberOfPublications);
                pub = pubs[i];
                if (pub.lock.writeLock().tryLock())
                {
                    //If not populated, create new publisher
                    if (!pub.isActive.get())
                    {
                        System.out.println("ADD PUB " + i);
                        pub.setPub(aeron.addPublication(pub.channel, pub.streamId));
                        pub.createMessageStream();
                        pub.isActive.set(true);
                    }
                    pub.lock.writeLock().unlock();
                }
            }
            Thread.yield();
        }
        allDone.countDown();
        System.out.println("CreatePubs all done!");
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
        while (running.get())
        {
            if (active.get())
            {
                //Get random pub slot
                i = rand.nextInt(numberOfPublications);
                pub = pubs[i];
                if (pub.lock.writeLock().tryLock())
                {
                    //If the publication is active, delete it
                    if (pub.isActive.get())
                    {
                        System.out.println("CLOSE PUB " + i);
                        pub.getPub().close();
                        pub.setPub(null);
                        pub.isActive.set(false);
                    }
                    pub.lock.writeLock().unlock();
                }
            }
            Thread.yield();
        }
        allDone.countDown();
        System.out.println("DeletePubs all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a publication, checks if it is active, then attempts to send on that publication
     */

    public void sendOnRandomPub()
    {
        final Random rand = new Random();
        int i = -1, bytesSent = 0, retryCount = 0;
        ThwackingElement pub;

        long rc = 0;
        while (running.get())
        {
                //Get random publication slot
                i = rand.nextInt(numberOfPublications);
                pub = pubs[i];
                //Populate a buffer to send
                //String message = "Hello World!" + i + ":" + msgSent.incrementAndGet();
                //BUFFER.putBytes(0, message.getBytes());

            if (pub.lock.readLock().tryLock())
            {
                //If this pub is active, send the buffer
                if (pub.isActive.get())
                {
                    try
                    {
                        bytesSent = pub.ms.getNext(pub.buffer);
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                    }
                    do
                    {
                        rc = pub.getPub().offer(pub.buffer, 0, bytesSent);
                        retryCount++;
                        if (retryCount == 50)
                        {
                            //System.out.println("WHA " + retryCount);
                        }
                    }
                    while (rc <= 0 && retryCount < 50 && running.get());
                    retryCount = 0;
                }
                pub.lock.readLock().unlock();
            }
            //Every iteration sends on the control Publication
            try
            {
                bytesSent = controlPublisherMessageStream.getNext(CONTROL_BUFFER);
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            do
            {
                rc = controlPub.offer(CONTROL_BUFFER, 0, bytesSent);
                retryCount++;
                if (retryCount == 50)
                {
                    //System.out.println("WHA " + retryCount);
                }
            }
            while (rc < 0 && retryCount < 50 && running.get());
            retryCount = 0;

            Thread.yield();
        }
        allDone.countDown();
        System.out.println("SendPubs all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a slot out of the subscription array and creates a subscription
     * if there isn't on there already
     */
    public void createSubs()
    {
        final Random rand = new Random();
        int i = -1;
        ThwackingElement sub;
        while (running.get())
        {
            if (active.get())
            {
                i = rand.nextInt(numberOfSubscriptions);
                sub = subs[i];
                if (sub.lock.writeLock().tryLock())
                {
                    if (!sub.isActive.get())
                    {
                        System.out.println("ADD SUB " + i);
                        final DataHandler dataHandler = new FragmentAssemblyAdapter(new Handlers()::handler);
                        sub.setSub(aeron.addSubscription(sub.channel, sub.streamId, dataHandler));
                        sub.isActive.set(true);
                    }
                    sub.lock.writeLock().unlock();
                }
            }
            Thread.yield();
        }
        allDone.countDown();
        System.out.println("CreateSubs all done!");
    }

    /**
     * Worker Thread Method:
     * Randomly selects a subscription and deletes it if no other threads are using that specific subscription
     */

    public void deleteSubs()
    {
        final Random rand = new Random();
        int i = -1;
        ThwackingElement sub;
        while (running.get())
        {
            if (active.get())
            {
                i = rand.nextInt(numberOfSubscriptions);
                sub = subs[i];
                if (sub.lock.writeLock().tryLock())
                {
                    if (sub.isActive.get())
                    {
                        System.out.println("CLOSE SUB " + i);
                        sub.getSub().close();
                        sub.setSub(null);
                        sub.isActive.set(false);
                    }
                    sub.lock.writeLock().unlock();
                }
            }
            Thread.yield();
        }
        allDone.countDown();
        System.out.println("DeleteSubs all done!");
    }

    /**
     * Worker thread method that will iterate over the list of subscriptions and call poll()
     */

    public void receiveOnSubs()
    {
        while (running.get())
        {
            //Run through each sub slot sequentially
            for (ThwackingElement s : subs)
            {
                //If the sub is active, call poll() to receive messages
                if (s.lock.writeLock().tryLock())
                {
                    if (s.isActive.get())
                    {
                        //System.out.println("RCV SUB " + s.channel() + " : " + s.streamId());
                        s.getSub().poll(FRAGMENT_LIMIT);
                    }
                    s.lock.writeLock().unlock();
                }
            }
            Thread.yield();
            //Every iteration calls poll() on the control subscription
            controlSub.poll(FRAGMENT_LIMIT);
        }
        allDone.countDown();
        System.out.println("RecSubs all done!");
    }



    public class Handlers
    {
        /**
         * Each subscription uses a message handler to increment the total amount of messages received
         * Message data is verified if set
         * @param buffer
         * @param offset
         * @param length
         * @param header
         */
        public void handler(DirectBuffer buffer, int offset, int length,
                            Header header)
        {
            if (useVerifiableMessageStream)
            {
                MessageStream ms = null;
                if (MessageStream.isVerifiable(buffer, offset))
                {
                    final int sessionId = header.sessionId();

                    /* See if our cached MessageStream is the right one. */
                    if (sessionId == lastSessionId)
                    {
                        ms = cachedStream;
                    }
                    if (ms == null)
                    {
                        /* Didn't have a MessageStream cached or it wasn't the right one. So do
                         * a lookup. */
                        ms = sessionIdMap.get(sessionId);
                        if (ms == null)
                        {
                            try
                            {
                                ms = new MessageStream(MESSAGE_MIN_SIZE, MESSAGE_MAX_SIZE, true);
                                System.out.println("PUTTING IN SESS ID " + sessionId);
                                sessionIdMap.put(sessionId, ms);
                            }
                            catch (Exception e)
                            {
                                e.printStackTrace();
                            }
                        }
                    }
                    /* Cache for next time. */
                    cachedStream = ms;
                    lastSessionId = sessionId;
                    try
                    {
                        ms.putNext(buffer, offset, length);
                    }
                    catch (final Exception e)
                    {
                        System.out.println("SESS ID FAIL " + sessionId);
                        System.out.println(e.getMessage());
                        //e.printStackTrace();
                    }
                }
            }
            msgCount.incrementAndGet();
        }

        /**
         * The control subscription has its own handler and message count
         */

        public void controlHandler(DirectBuffer buffer, int offset, int length,
                                   Header header)
        {
            if (useVerifiableMessageStream)
            {
                if (MessageStream.isVerifiable(buffer, offset))
                {
                    try
                    {
                        controlSubscriberMessageStream.putNext(buffer, offset, length);
                    }
                    catch (final Exception e)
                    {
                        System.out.println(e.getMessage());
                        //e.printStackTrace();
                    }
                }
            }
            controlMsgCount.incrementAndGet();
        }
    }


    /** Thwacking Element:
     *  Internal object used to contain either a publication or subscription
     *  and state associated along with it
     */
    public class ThwackingElement
    {
        private final UnsafeBuffer buffer;
        //TODO Make publication and subscription interfaces to generalize Thwacker
        private Publication pub;
        private Subscription sub;
        public MessageStream ms;
        public String channel;
        public int streamId;
        boolean verify;
        /* Active flag showing whether there is an already created pub or sub in this thwacking element */
        public AtomicBoolean isActive;
        /* Lock for create/delete and sending/receiving on a publication or subscription
         * The creation/deletion methods use the write lock to ensure exclusive access.
         * The sending/receiving methods are thread safe so they use the read lock to allow many threads to offer()
         * or poll() on the same sub or pub */
        public ReentrantReadWriteLock lock;


        ThwackingElement(String chan, int stId, boolean verifiable)
        {
            channel = chan;
            streamId = stId;
            verify = verifiable;
            buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(DEFAULT_BUFFER_SIZE));
            isActive = new AtomicBoolean(false);
            lock = new ReentrantReadWriteLock();
        }

        /**
         * The only time this is called is within pubCreate()
         */
        void createMessageStream()
        {
            /* If ms is defined upon create, we're using a previously used Pub slot, so reset the stream */

            if (ms != null)
            {
                ms.reset();
            }
            else
            {
                try
                {
                    ms = new MessageStream(MESSAGE_MIN_SIZE, MESSAGE_MAX_SIZE, verify);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }

        void setPub(Publication p)
        {
            pub = p;
        }

        Publication getPub()
        {
            return pub;
        }

        void setSub(Subscription s)
        {
            sub = s;
        }

        Subscription getSub()
        {
            return sub;
        }

        void close()
        {
            if (pub != null)
            {
                pub.close();
                pub = null;
            }
            if (sub != null)
            {
                sub.close();
                sub = null;
            }
            isActive = null;
            lock = null;
            channel = null;
            ms = null;
        }
    }
}
