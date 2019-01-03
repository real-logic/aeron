/*
 * Copyright 2014-2019 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.CongestionControlUtil.packOutcome;

/**
 * CUBIC congestion control manipulation of the receiver window length.
 * <p>
 * <a target="_blank" href="https://research.csc.ncsu.edu/netsrv/?q=content/bic-and-cubic">
 *     https://research.csc.ncsu.edu/netsrv/?q=content/bic-and-cubic</a>
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
    private static final boolean RTT_MEASUREMENT = CubicCongestionControlConfiguration.MEASURE_RTT;
    private static final boolean TCP_MODE = CubicCongestionControlConfiguration.TCP_MODE;

    private static final long RTT_MEASUREMENT_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long SECOND_IN_NS = TimeUnit.SECONDS.toNanos(1);
    private static final long RTT_MAX_TIMEOUT_NS = SECOND_IN_NS;
    private static final int MAX_OUTSTANDING_RTT_MEASUREMENTS = 1;

    private static final double C = 0.4;
    private static final double B = 0.2;

    private final int minWindow;
    private final int mtu;
    private final int maxCwnd;

    private long lastLossTimestampNs;
    private long lastUpdateTimestampNs;
    private long lastRttTimestampNs = 0;
    private final long windowUpdateTimeoutNs;
    private long rttInNs;
    private double k;
    private int cwnd;
    private int w_max;

    private int outstandingRttMeasurements = 0;

    private final AtomicCounter rttIndicator;
    private final AtomicCounter windowIndicator;

    CubicCongestionControl(
        final long registrationId,
        final UdpChannel udpChannel,
        final int streamId,
        final int sessionId,
        final int termLength,
        final int senderMtuLength,
        final NanoClock clock,
        final MediaDriver.Context context,
        final CountersManager countersManager)
    {
        mtu = senderMtuLength;
        minWindow = senderMtuLength;
        final int maxWindow = Math.min(termLength / 2, context.initialWindowLength());

        maxCwnd = maxWindow / mtu;
        cwnd = 1;
        w_max = maxCwnd; // initially set w_max to max window and act in the TCP and concave region initially
        k = Math.cbrt((double)w_max * B / C);

        // determine interval for adjustment based on heuristic of MTU, max window, and/or RTT estimate
        rttInNs = CubicCongestionControlConfiguration.INITIAL_RTT_NS;
        windowUpdateTimeoutNs = rttInNs;

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

        lastLossTimestampNs = clock.nanoTime();
        lastUpdateTimestampNs = lastLossTimestampNs;
    }

    public boolean shouldMeasureRtt(final long nowNs)
    {
        return RTT_MEASUREMENT &&
            outstandingRttMeasurements < MAX_OUTSTANDING_RTT_MEASUREMENTS &&
            (((lastRttTimestampNs + RTT_MAX_TIMEOUT_NS) - nowNs < 0) ||
                ((lastRttTimestampNs + RTT_MEASUREMENT_TIMEOUT_NS) - nowNs < 0));
    }

    public void onRttMeasurementSent(final long nowNs)
    {
        lastRttTimestampNs = nowNs;
        outstandingRttMeasurements++;
    }

    public void onRttMeasurement(final long nowNs, final long rttNs, final InetSocketAddress srcAddress)
    {
        outstandingRttMeasurements--;
        lastRttTimestampNs = nowNs;
        this.rttInNs = rttNs;
        rttIndicator.setOrdered(rttNs);
    }

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
            k = Math.cbrt((double)w_max * B / C);
            cwnd = Math.min(1, (int)(cwnd * (1.0 - B)));
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
            if (TCP_MODE && cwnd < w_max)
            {
                // W_tcp(t) = w_max*(1-B) + 3*B/(2-B)* t/RTT

                final double rttInSeconds = (double)rttInNs / (double)SECOND_IN_NS;
                final double wTcp =
                    (double)w_max * (1.0 - B) + ((3.0 * B / (2.0 * B)) * (durationSinceDecr / rttInSeconds));

                cwnd = Math.max(cwnd, (int)wTcp);
            }

            lastUpdateTimestampNs = nowNs;
        }

        final int window = cwnd * mtu;
        windowIndicator.setOrdered(window);

        return packOutcome(window, forceStatusMessage);
    }

    public int initialWindowLength()
    {
        return minWindow;
    }

    public void close()
    {
        CloseHelper.close(rttIndicator);
        CloseHelper.close(windowIndicator);
    }
}
