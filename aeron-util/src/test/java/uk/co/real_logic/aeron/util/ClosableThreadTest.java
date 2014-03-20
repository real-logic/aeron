package uk.co.real_logic.aeron.util;

public class ClosableThreadTest
{
    public static void processLoop(final ClosableThread thread, final int iterations)
    {
        for (int i = 0; i < iterations; i++)
        {
            thread.process();
        }
    }

}
