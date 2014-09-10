package uk.co.real_logic.aeron.driver;

import uk.co.real_logic.aeron.common.status.BufferPositionIndicator;
import uk.co.real_logic.aeron.common.status.PositionIndicator;

/**
 * .
 */
public final class SubscriptionPosition
{
    private final long registrationId;
    private final int positionCounterId;
    private final PositionIndicator positionIndicator;

    public SubscriptionPosition(
        final long registrationId, final int positionCounterId, final PositionIndicator positionIndicator)
    {
        this.registrationId = registrationId;
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

    public long registrationId()
    {
        return registrationId;
    }
}
