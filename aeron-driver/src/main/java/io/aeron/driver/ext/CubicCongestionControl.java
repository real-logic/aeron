/*
 * Copyright 2017 Real Logic Ltd.
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
import org.agrona.concurrent.NanoClock;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.CongestionControlUtil.packOutcome;

/**
 * CUBIC congestion control manipulation of the receiver window length.
 *
 * https://research.csc.ncsu.edu/netsrv/?q=content/bic-and-cubic
 *
 * W_cubic = C(T - K)^3 + w_max
 *
 * K = cbrt(w_max * B / C)
 * w_max = window size before reduction
 * T = time since last decrease
 *
 * C = scaling constant (default 0.4)
 * B = multiplicative decrease (default 0.2)
 *
 * at MTU=4K, max window=128KB (w_max = 32 MTUs), then K ~= 2.5 seconds.
 */
public class CubicCongestionControl implements CongestionControl
{
    private static final boolean RTT_MEASUREMENT = false;
    private static final long RTT_MEASUREMENT_TIMEOUT = TimeUnit.MILLISECONDS.toNanos(10);
    private static final long RTT_MAX_TIMEOUT = TimeUnit.SECONDS.toNanos(1);
    private static final int MAX_OUTSTANDING_RTT_MEASUREMENTS = 1;

    private static final boolean TCP_MODE = false;

    private static final double C = 0.4;
    private static final double B = 0.2;

    private final int minWindow;
    private final int mtu;
    private final int maxCwnd;

    private long lastLossTimestamp;
    private long lastUpdateTimestamp;
    private long lastRttTimestamp = 0;
    private long windowUpdateTimeout;
    private long rttInNanos;
    private double k;
    private int cwnd;
    private int w_max;

    private int outstandingRttMeasurements = 0;

    CubicCongestionControl(
        final UdpChannel udpChannel,
        final int streamId,
        final int sessionId,
        final int termLength,
        final int senderMtuLength,
        final NanoClock clock,
        final MediaDriver.Context context)
    {
        mtu = senderMtuLength;
        minWindow = senderMtuLength;
        final int maxWindow = context.initialWindowLength();

        maxCwnd = maxWindow / mtu;
        cwnd = 1;
        w_max = maxCwnd; // initially set w_max to max window and act in the TCP and concave region initially
        k = Math.cbrt((double)w_max * B / C);

        // determine interval for adjustment based on heuristic of MTU, max window, and/or RTT estimate
        rttInNanos = TimeUnit.MICROSECONDS.toNanos(100); // initial RTT
        windowUpdateTimeout = rttInNanos;

        // TODO: add counters manager so that some stats can be outputted (such as RTT and cwnd)

        lastLossTimestamp = clock.nanoTime();
        lastUpdateTimestamp = lastLossTimestamp;
    }

    public boolean shouldMeasureRtt(final long now)
    {
        boolean result = false;

        if (RTT_MEASUREMENT && outstandingRttMeasurements < MAX_OUTSTANDING_RTT_MEASUREMENTS)
        {
            if (now > (lastRttTimestamp + RTT_MAX_TIMEOUT))
            {
                lastRttTimestamp = now;
                outstandingRttMeasurements++;
                result = true;
            }
            else if (now > (lastRttTimestamp + RTT_MEASUREMENT_TIMEOUT))
            {
                lastRttTimestamp = now;
                outstandingRttMeasurements++;
                result = true;
            }
        }

        return result;
    }

    public void onRttMeasurement(final long now, final long rttInNanos, final InetSocketAddress srcAddress)
    {
        outstandingRttMeasurements--;
        lastRttTimestamp = now;
        this.rttInNanos = rttInNanos;
    }

    public long onTrackRebuild(
        final long now,
        final long newConsumptiopnPosition,
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
            lastLossTimestamp = now;
            forceStatusMessage = true;
        }
        else if (cwnd < maxCwnd && now > (lastUpdateTimestamp + windowUpdateTimeout))
        {
            // W_cubic = C(T - K)^3 + w_max
            final double durationSinceDecr = (double)(now - lastLossTimestamp) / (double)TimeUnit.SECONDS.toNanos(1);
            final double diffToK = durationSinceDecr - k;
            final double incr = C * diffToK * diffToK * diffToK;

            cwnd = Math.min(maxCwnd, w_max + (int)incr);

            // if using TCP mode, then check to see if we are in the TCP region
            if (TCP_MODE && cwnd < w_max)
            {
                // W_tcp(t) = w_max*(1-B) + 3*B/(2-B)* t/RTT

                final double rttInSeconds = (double)rttInNanos / (double)TimeUnit.SECONDS.toNanos(1);
                final double wTcp =
                    (double)w_max * (1.0 - B) + ((3.0 * B / (2.0 * B)) * (durationSinceDecr / rttInSeconds));

                cwnd = Math.max(cwnd, (int)wTcp);
            }

            lastUpdateTimestamp = now;
        }

        final int window = cwnd * mtu;

        return packOutcome(window, forceStatusMessage);
    }

    public int initialWindowLength()
    {
        return minWindow;
    }
}
