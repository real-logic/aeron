package uk.co.real_logic.aeron.tools;

import java.util.ArrayList;

import uk.co.real_logic.agrona.concurrent.SystemNanoClock;


public class RateReporter implements RateController.Callback, Runnable
{
    private static final SystemNanoClock CLOCK = new SystemNanoClock();
    private final Thread reporterThread;
    private long lastReportTimeNanos;
    private long lastNonVerifiableMessages;
    private long lastVerifiableMessages;
    private long lastBytes;
    private boolean shuttingDown;
    private final Stats app;

    public interface Stats
    {
        /**
         * A snapshot (non-atomic) total of the number of verifiable messages
         * sent/received across all threads.
         * @return current total number of verifiable messages sent or received
         */
        long verifiableMessages();

        /**
         * A snapshot (non-atomic) total of the number of bytes
         * sent/received across all threads.
         * @return current total number of bytes sent or received
         */
        long bytes();

        /**
         * A snapshot (non-atomic) total of the number of non-verifiable messages
         * sent/received across all threads.
         * @return current total number of non-verifiable messages sent or received
         */
        long nonVerifiableMessages();
    }

    public RateReporter(Stats app)
    {
        this.app = app;
        reporterThread = new Thread(this);
        reporterThread.start();
    }

    /** Returns a human-readable bits/messages/whatever-per-second string
     * @param bits The total number of bits (per second) to convert to a human-readable string
     * @return the human-readable bits-per-second string
     */
    public static String getHumanReadableRate(long bits)
    {
        if (bits < 1000)
        {
            return bits + " ";
        }
        final int exp = (int) (Math.log(bits) / Math.log(1000));
        return String.format("%.1f %s", bits / Math.pow(1000, exp), "KMGTPE".charAt(exp-1));
    }

    /** Returns a human-readable bits/messages/whatever-per-second string
     * @param bits The total number of bits (per second) to convert to a human-readable string
     * @return the human-readable bits-per-second string
     */
    public static String getHumanReadableRate(double bits)
    {
        if (bits < 1000)
        {
            return String.format("%.3f ",  bits);
        }
        final int exp = (int) (Math.log(bits) / Math.log(1000));
        return String.format("%.3f %s", bits / Math.pow(1000, exp), "KMGTPE".charAt(exp-1));
    }

    /** Shuts down the rate reporter thread; blocks until it is finished. */
    public void close()
    {
        shuttingDown = true;
        try
        {
            reporterThread.join();
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void run()
    {
        final ArrayList<RateControllerInterval> intervals = new ArrayList<RateControllerInterval>();

        intervals.add(new MessagesAtMessagesPerSecondInterval(Long.MAX_VALUE, 1));

        RateController rateController;

        try
        {
            rateController = new RateController(this, intervals);
            lastReportTimeNanos = CLOCK.time() - 1000000000; /* Subtract a second so the first print is correct. */
            while (!shuttingDown && rateController.next())
            {
                /* rateController will call onNext to report the interval's rates. */
            }
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }
        shuttingDown = true; /* If we weren't shutting down already, we certainly should be now. */
    }

    /**
     * Function called by the RateController once a second; used to report
     * the current aggregate receiving rates.
     */
    @Override
    public int onNext()
    {
        final long currentTimeNanos = CLOCK.time();
        final long verifiableMessages = app.verifiableMessages();
        final long nonVerifiableMessages = app.nonVerifiableMessages();
        final long totalMessages = verifiableMessages + nonVerifiableMessages;
        final long lastTotalMessages = lastNonVerifiableMessages + lastVerifiableMessages;
        final long bytesReceived = app.bytes();
        final double secondsElapsed = (currentTimeNanos - lastReportTimeNanos)/1000000000.0;
        System.out.format("%.6f: %smsgs/sec %sbps%n",
                secondsElapsed,
                getHumanReadableRate((totalMessages - lastTotalMessages) / secondsElapsed),
                getHumanReadableRate((long)((((bytesReceived - lastBytes) * 8)) / secondsElapsed)));
        lastReportTimeNanos = currentTimeNanos;
        lastVerifiableMessages = verifiableMessages;
        lastNonVerifiableMessages = nonVerifiableMessages;
        lastBytes = bytesReceived;
        /* Should we exit? */
        if (shuttingDown)
        {
            return -1;
        }
        return 0; /* no "bytes" sent. */
    }
}
