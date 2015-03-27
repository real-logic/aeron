package uk.co.real_logic.aeron.tools;

import uk.co.real_logic.aeron.tools.RateController.IntervalInternal;

public class SecondsAtMessagesPerSecondInterval extends RateControllerInterval
{
    /* The rate we _want_ to achieve, if possible.  Might not be able
     * to hit it exactly due to receiver pacing, etc.  But it's what
     * we're aiming for. */
    private final double goalMessagesPerSecond;
    /* Number of seconds (can be fractional) to run for, in total. */
    private final double seconds;

    public SecondsAtMessagesPerSecondInterval(double seconds, double messagesPerSecond)
    {
        this.goalMessagesPerSecond = messagesPerSecond;
        this.seconds = seconds;
    }

    public double messagesPerSecond()
    {
        return goalMessagesPerSecond;
    }

    public double seconds()
    {
        return seconds;
    }

    @Override
    IntervalInternal makeInternal(RateController rateController) throws Exception
    {
        return rateController.new SecondsAtMessagesPerSecondInternal(rateController, seconds, goalMessagesPerSecond);
    }
}
