package uk.co.real_logic.aeron.util;

public abstract class ClosableThread implements Runnable, AutoCloseable
{

    private volatile boolean running;

    public ClosableThread()
    {
        running = true;
    }

    @Override
    public void run()
    {
        while (running)
        {
            process();
        }
    }

    @Override
    public void close() throws Exception
    {
        running = false;
    }

    public abstract void process();

}
