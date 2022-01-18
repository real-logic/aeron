package io.aeron.samples.echo;

import io.aeron.ConcurrentPublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.bindings.agent.api.EchoMonitorMBean;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

public class EchoPair implements ControlledFragmentHandler, AutoCloseable
{
    private final long correlationId;
    private final Subscription subscription;
    private final Publication publication;

    private long notConnectedCount = 0;
    private long backPressureCount = 0;
    private long adminActionCount = 0;
    private long closedCount = 0;
    private long maxSessionExceededCount = 0;

    private long fragmentCount = 0;
    private long byteCount = 0;

    public EchoPair(final long correlationId, final Subscription subscription, final Publication publication)
    {
        this.correlationId = correlationId;
        this.subscription = subscription;
        this.publication = publication;
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long offer = publication.offer(buffer, offset, length);
        if (Publication.NOT_CONNECTED == offer)
        {
            notConnectedCount++;
            return Action.ABORT;
        }
        else if (Publication.BACK_PRESSURED == offer)
        {
            backPressureCount++;
            return Action.ABORT;
        }
        else if (Publication.ADMIN_ACTION == offer)
        {
            adminActionCount++;
            return Action.ABORT;
        }
        else if (Publication.CLOSED == offer)
        {
            closedCount++;
            return Action.CONTINUE;
        }
        else if (Publication.MAX_POSITION_EXCEEDED == offer)
        {
            maxSessionExceededCount++;
            return Action.CONTINUE;
        }
        else
        {
            fragmentCount++;
            byteCount += length;
            return Action.COMMIT;
        }
    }

    public int poll()
    {
        return subscription.controlledPoll(this, 20);
    }

    public long correlationId()
    {
        return correlationId;
    }

    public EchoMonitorMBean monitor()
    {
        return new EchoMonitor();
    }

    public void close()
    {
        CloseHelper.quietCloseAll(subscription, publication);
    }

    private class EchoMonitor implements EchoMonitorMBean
    {
        public long getCorrelationId()
        {
            return correlationId;
        }

        public long getBackPressureCount()
        {
            return backPressureCount;
        }

        public long getFragmentCount()
        {
            return fragmentCount;
        }

        public long getByteCount()
        {
            return byteCount;
        }
    }
}
