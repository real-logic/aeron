package uk.co.real_logic.aeron.conductor;

/**
 * Signal a waiting thread that an event has occurred which it has been waiting on.
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
        catch (final InterruptedException ex)
        {
            throw new RuntimeException(ex);
        }
        finally
        {
            signalRaised = false;
        }
    }
}
