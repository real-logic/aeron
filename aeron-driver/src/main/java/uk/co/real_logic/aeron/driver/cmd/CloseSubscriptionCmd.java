package uk.co.real_logic.aeron.driver.cmd;

import uk.co.real_logic.aeron.driver.DriverSubscription;

/**
 * .
 */
public class CloseSubscriptionCmd
{
    private final DriverSubscription subscription;

    public CloseSubscriptionCmd(final DriverSubscription subscription)
    {
        this.subscription = subscription;
    }

    public DriverSubscription subscription()
    {
        return subscription;
    }

}
