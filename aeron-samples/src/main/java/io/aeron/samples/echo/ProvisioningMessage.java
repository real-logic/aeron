package io.aeron.samples.echo;

public class ProvisioningMessage
{
    private final Object mutex = new Object();
    private volatile Object result = null;

    public void await() throws InterruptedException
    {
        while (null == result) {
            synchronized (mutex) {
                mutex.wait();
            }
        }

        if (result instanceof Exception) {
            throw new RuntimeException((Exception)result);
        }
    }

    public void complete(Object value)
    {
        synchronized (mutex) {
            result = value;
            mutex.notify();
        }
    }
}
