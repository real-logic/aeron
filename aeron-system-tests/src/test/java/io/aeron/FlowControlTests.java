package io.aeron;

import io.aeron.test.Counters;
import io.aeron.test.Tests;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.driver.status.SystemCounterDescriptor.STATUS_MESSAGES_RECEIVED;

public class FlowControlTests
{
    public static void waitForConnectionAndStatusMessages(
        final CountersReader countersReader,
        final Subscription subscription,
        final Subscription... subscriptions)
    {
        while (!subscription.isConnected())
        {
            Tests.sleep(1);
        }

        for (final Subscription sub : subscriptions)
        {
            while (!sub.isConnected())
            {
                Tests.sleep(1);
            }
        }

        final long delta = 1 + subscriptions.length;
        Counters.waitForCounterIncrease(countersReader, STATUS_MESSAGES_RECEIVED.id(), delta);
    }
}
