/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver.ext;

import io.aeron.driver.CongestionControl;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.PerImageIndicator;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.CongestionControl.packOutcome;

/**
 * CUBIC congestion control manipulation of the receiver window length.
 * <p>
 * <a target="_blank" href="https://tools.ietf.org/id/draft-rhee-tcpm-cubic-02.txt">
 * https://tools.ietf.org/id/draft-rhee-tcpm-cubic-02.txt</a> and
 * <a target="_blank" href="https://dl.acm.org/doi/10.1145/1400097.1400105">
 * https://dl.acm.org/doi/10.1145/1400097.1400105</a>
 * <p>
 * {@code W_cubic = C(T - K)^3 + w_max}
 * <p>
 * {@code K = cbrt(w_max * B / C)}
 * {@code w_max} = window size before reduction
 * {@code T} = time since last decrease
 * <p>
 * {@code C} = scaling constant (default 0.4)
 * {@code B} = multiplicative decrease (default 0.2)
 * <p>
 * at MTU=4K, max window=128KB (w_max = 32 MTUs), then K ~= 2.5 seconds.
 */
public class CubicCongestionControl implements CongestionControl
{
    /**
     * URI param value to identify this {@link CongestionControl} strategy.
     */
    public static final String CC_PARAM_VALUE = "cubic";

    private static final long RTT_MEASUREMENT_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long SECOND_IN_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long RTT_MAX_TIMEOUT_NS = SECOND_IN_NS;
    private static final int MAX_OUTSTANDING_RTT_MEASUREMENTS = 1;

    private static final double C = 0.4;
    private static final double B = 0.2;

    private final int minWindow;
    private final int mtu;
    private final int maxCwnd;
    private final ErrorHandler errorHandler;

    private long lastLossTimestampNs;
    private long lastUpdateTimestampNs;
    private long lastRttTimestampNs = 0;
    private final long windowUpdateTimeoutNs;
    private long rttNs;
    private double k;
    private int cwnd;
    private int w_max;

    private int outstandingRttMeasurements = 0;

    private final AtomicCounter rttIndicator;
    private final AtomicCounter windowIndicator;

    /**
     * Construct a new {@link CongestionControl} instance for a received stream image using the Cubic algorithm.
     *
     * @param registrationId  for the publication image.
     * @param udpChannel      for the publication image.
     * @param streamId        for the publication image.
     * @param sessionId       for the publication image.
     * @param termLength      for the publication image.
     * @param senderMtuLength for the publication image.
     * @param controlAddress  for the publication image.
     * @param sourceAddress   for the publication image.
     * @param nanoClock       for the precise timing.
     * @param context         for configuration options applied in the driver.
     * @param countersManager for the driver.
     */
    public CubicCongestionControl(
        final long registrationId,
        final UdpChannel udpChannel,
        final int streamId,
        final int sessionId,
        final int termLength,
        final int senderMtuLength,
        final InetSocketAddress controlAddress,
        final InetSocketAddress sourceAddress,
        final NanoClock nanoClock,
        final MediaDriver.Context context,
        final CountersManager countersManager)
    {
        AtomicCounter rttIndicator = null;
        AtomicCounter windowIndicator = null;
        try
        {
            mtu = senderMtuLength;
            minWindow = senderMtuLength;
            final int maxWindow = Math.min(termLength >> 1, context.initialWindowLength());

            maxCwnd = maxWindow / mtu;
            cwnd = 1;
            w_max = maxCwnd; // initially set w_max to max window and act in the TCP and concave region initially
            k = StrictMath.cbrt((double)w_max * B / C);

            // determine interval for adjustment based on heuristic of MTU, max window, and/or RTT estimate
            rttNs = CubicCongestionControlConfiguration.INITIAL_RTT_NS;
            windowUpdateTimeoutNs = rttNs;

            rttIndicator = PerImageIndicator.allocate(
                context.tempBuffer(),
                "rcv-cc-cubic-rtt",
                countersManager,
                registrationId,
                sessionId,
                streamId,
                udpChannel.originalUriString());

            windowIndicator = PerImageIndicator.allocate(
                context.tempBuffer(),
                "rcv-cc-cubic-wnd",
                countersManager,
                registrationId,
                sessionId,
                streamId,
                udpChannel.originalUriString());

            rttIndicator.setOrdered(0);
            windowIndicator.setOrdered(minWindow);
            this.rttIndicator = rttIndicator;
            this.windowIndicator = windowIndicator;

            lastLossTimestampNs = nanoClock.nanoTime();
            lastUpdateTimestampNs = lastLossTimestampNs;

            errorHandler = context.errorHandler();
        }
        catch (final Throwable ex)
        {
            CloseHelper.close(rttIndicator);
            CloseHelper.close(windowIndicator);
            throw ex;
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean shouldMeasureRtt(final long nowNs)
    {
        return CubicCongestionControlConfiguration.MEASURE_RTT &&
            outstandingRttMeasurements < MAX_OUTSTANDING_RTT_MEASUREMENTS &&
            (((lastRttTimestampNs + RTT_MAX_TIMEOUT_NS) - nowNs < 0) ||
                ((lastRttTimestampNs + RTT_MEASUREMENT_TIMEOUT_NS) - nowNs < 0));
    }

    /**
     * {@inheritDoc}
     */
    public void onRttMeasurementSent(final long nowNs)
    {
        lastRttTimestampNs = nowNs;
        outstandingRttMeasurements++;
    }

    /**
     * {@inheritDoc}
     */
    public void onRttMeasurement(final long nowNs, final long rttNs, final InetSocketAddress srcAddress)
    {
        outstandingRttMeasurements--;
        lastRttTimestampNs = nowNs;
        this.rttNs = rttNs;
        rttIndicator.setOrdered(rttNs);
    }

    /**
     * {@inheritDoc}
     */
    public long onTrackRebuild(
        final long nowNs,
        final long newConsumptionPosition,
        final long lastSmPosition,
        final long hwmPosition,
        final long startingRebuildPosition,
        final long endingRebuildPosition,
        final boolean lossOccurred)
    {
        boolean forceStatusMessage = false;

        if (lossOccurred)
        {
            w_max = cwnd;
            k = StrictMath.cbrt((double)w_max * B / C);
            cwnd = Math.max(1, (int)(cwnd * (1.0 - B)));
            lastLossTimestampNs = nowNs;
            forceStatusMessage = true;
        }
        else if (cwnd < maxCwnd && ((lastUpdateTimestampNs + windowUpdateTimeoutNs) - nowNs < 0))
        {
            // W_cubic = C(T - K)^3 + w_max
            final double durationSinceDecr = (double)(nowNs - lastLossTimestampNs) / (double)SECOND_IN_NS;
            final double diffToK = durationSinceDecr - k;
            final double incr = C * diffToK * diffToK * diffToK;

            cwnd = Math.min(maxCwnd, w_max + (int)incr);

            // if using TCP mode, then check to see if we are in the TCP region
            if (CubicCongestionControlConfiguration.TCP_MODE && cwnd < w_max)
            {
                // W_tcp(t) = w_max * (1 - B) + 3 * B / (2 - B) * t / RTT

                final double rttInSeconds = (double)rttNs / (double)SECOND_IN_NS;
                final double wTcp =
                    (double)w_max * (1.0 - B) + ((3.0 * B / (2.0 - B)) * (durationSinceDecr / rttInSeconds));

                cwnd = Math.max(cwnd, (int)wTcp);
            }

            lastUpdateTimestampNs = nowNs;
        }

        final int window = cwnd * mtu;
        windowIndicator.setOrdered(window);

        return packOutcome(window, forceStatusMessage);
    }

    /**
     * {@inheritDoc}
     */
    public int initialWindowLength()
    {
        return minWindow;
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(errorHandler, rttIndicator);
        CloseHelper.close(errorHandler, windowIndicator);
    }
}
