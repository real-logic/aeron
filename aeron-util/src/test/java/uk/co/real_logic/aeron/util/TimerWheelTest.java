package uk.co.real_logic.aeron.util;

import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class TimerWheelTest
{
    private long controlTimestamp;

    public long getControlTimestamp()
    {
        return controlTimestamp;
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldExceptionOnNonPowerOf2TicksPerWheel()
    {
        final TimerWheel wheel = new TimerWheel(100, TimeUnit.MILLISECONDS, 10);
    }

    @Test
    public void shouldBeAbleToCalculateDelayWithRealTime()
    {
        final TimerWheel wheel = new TimerWheel(100, TimeUnit.MILLISECONDS, 512);

        assertThat(wheel.calculateDelayInMsec(), is(100L));
    }

    @Test
    public void shouldBeAbleToCalculateDelay()
    {
        controlTimestamp = 100;
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 512);

        assertThat(wheel.calculateDelayInMsec(), is(1L));
    }

    @Ignore
    @Test
    public void shouldBeAbleToScheduleTimer()
    {
        controlTimestamp = 100;
        final AtomicBoolean fired = new AtomicBoolean(false);
        final TimerWheel wheel = new TimerWheel(this::getControlTimestamp, 1, TimeUnit.MILLISECONDS, 1024);

        final TimerWheel.Timer timeout = wheel.newTimeout(() -> { fired.set(true); }, 10, TimeUnit.MILLISECONDS);

        IntStream.range(0, 9).forEach((i) ->
        {
            controlTimestamp++;
            wheel.expireTimers();
            assertThat(controlTimestamp, lessThanOrEqualTo(110L));
            assertThat(fired.get(), is(false));
        });
        controlTimestamp++;
        wheel.expireTimers();
        assertThat(controlTimestamp, is(110L));
        assertThat(fired.get(), is(true));
        // TODO: fixme
    }

    @Ignore
    @Test
    public void shouldHandleTimeUnitCorrectly()
    {
        // TODO: finish
    }

    @Ignore
    @Test
    public void shouldHandleMultipleRounds()
    {
        // TODO: finish
    }

    @Ignore
    @Test
    public void shouldBeAbleToCancelTimer()
    {
        // TODO: finish
    }
}
