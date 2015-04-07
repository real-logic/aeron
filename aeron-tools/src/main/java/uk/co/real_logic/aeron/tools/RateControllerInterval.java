package uk.co.real_logic.aeron.tools;

public abstract class RateControllerInterval
{
    /* Is this interval currently active/running? */
    protected boolean active;

    /* Number of bits sent so far during this interval. */
    protected long bitsSent;
    /* Number of messages sent so far during this interval. */
    protected long messagesSent;

    /* Begin and end timestamps for last time this interval was active. */
    protected long beginTimeNanos;
    protected long endTimeNanos;

    public long messagesSent()
    {
        return messagesSent;
    }

    public long bytesSent()
    {
        return (bitsSent / 8);
    }

    public long startTimeNanos()
    {
        return beginTimeNanos;
    }

    public long stopTimeNanos()
    {
        return endTimeNanos;
    }

    abstract RateController.IntervalInternal makeInternal(RateController rateController) throws Exception;
}
