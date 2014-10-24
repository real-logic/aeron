package uk.co.real_logic.aeron.driver.cmd;

import uk.co.real_logic.aeron.driver.DriverSubscription;
import uk.co.real_logic.aeron.driver.Receiver;

public class CloseSubscriptionCmd implements ReceiverCmd
{
    private final DriverSubscription subscription;

    public CloseSubscriptionCmd(final DriverSubscription subscription)
    {
        this.subscription = subscription;
    }

    public void execute(final Receiver receiver)
    {
        receiver.onCloseSubscription(subscription);
    }
}
