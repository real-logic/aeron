package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.status.PositionIndicator;

/**
 * .
 */
public final class SubscriptionPosition
{

    private final DriverSubscription subscription;
    private final int positionCounterId;
    private final PositionIndicator positionIndicator;

    public SubscriptionPosition(
        final DriverSubscription subscription, final int positionCounterId, final PositionIndicator positionIndicator)
    {
        this.subscription = subscription;
        this.positionCounterId = positionCounterId;
        this.positionIndicator = positionIndicator;
    }

    public PositionIndicator positionIndicator()
    {
        return positionIndicator;
    }

    public int positionCounterId()
    {
        return positionCounterId;
    }

    public DriverSubscription subscription()
    {
        return subscription;
    }
}
