package uk.co.real_logic.aeron.conductor;

/**
 * .
 */
public class Signal
{
    private boolean signalRaised;

    public Signal()
    {
        signalRaised = false;
    }

    public synchronized void signal()
    {
        if (signalRaised)
        {
            throw new IllegalStateException("Attempting to signal when signal has already been raised");
        }

        signalRaised = true;
        this.notify();
    }

    public synchronized void await(final long awaitTimeout)
    {
        if (signalRaised)
        {
            return;
        }

        try
        {
            wait(awaitTimeout);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            signalRaised = false;
        }
    }
}
