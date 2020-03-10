package io.aeron.test;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

public final class CountingFragmentHandler implements FragmentHandler
{
    private final String name;
    private int lastCheckedTargetValue = 0;
    private int received = 0;

    public CountingFragmentHandler(final String name)
    {
        this.name = name;
    }

    public boolean notDone(final int targetValue)
    {
        lastCheckedTargetValue = targetValue;
        return targetValue != received;
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        received++;
    }

    public String toString()
    {
        return "CountingFragmentHandler{" +
            "name='" + name + '\'' +
            ", received=" + received +
            ", lastCheckedTargetValue" + lastCheckedTargetValue +
            '}';
    }
}
