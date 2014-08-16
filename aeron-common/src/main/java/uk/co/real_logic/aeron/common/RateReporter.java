/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.common;

import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

/**
 * Tracker and reporter of rates.
 *
 * Uses volatile semantics for counters.
 */
public class RateReporter implements Runnable
{
    private final long reportIntervalNs;
    private final long parkNs;
    private final BiConsumer<Double, Double> reportingFunction;

    private volatile boolean done = false;
    private volatile long totalBytes;
    private volatile long totalMessages;
    private long lastTotalBytes;
    private long lastTotalMessages;
    private long lastTimestamp;

    /**
     * Create a rate reporter with the given report interval in nanoseconds and the reporting function.
     *
     * @param reportInterval in nanoseconds
     * @param reportingFunction to call for reporting rates
     */
    public RateReporter(final long reportInterval, final BiConsumer<Double, Double> reportingFunction)
    {
        this.reportIntervalNs = reportInterval;
        this.parkNs = reportInterval;
        this.reportingFunction = reportingFunction;
        lastTimestamp = System.nanoTime();
    }

    /**
     * Run loop for the rate reporter
     */
    public void run()
    {
        do
        {
            LockSupport.parkNanos(parkNs);

            // These are not transacted, so they could be off from one another.
            final long currentTotalMessages = totalMessages;
            final long currentTotalBytes = totalBytes;
            final long currentTimestamp = System.nanoTime();

            final long timeSpanNs = currentTimestamp - lastTimestamp;
            final double messagesPerSec = ((currentTotalMessages - lastTotalMessages) * reportIntervalNs) / (double)timeSpanNs;
            final double bytesPerSec = ((currentTotalBytes - lastTotalBytes) * reportIntervalNs) / (double)timeSpanNs;

            reportingFunction.accept(messagesPerSec, bytesPerSec);

            lastTotalBytes = currentTotalBytes;
            lastTotalMessages = currentTotalMessages;
            lastTimestamp = currentTimestamp;
        }
        while (!done);
    }

    /**
     * Signal the run loop to exit. Does not block.
     */
    public void halt()
    {
        done = true;
    }

    /**
     * Tell rate reporter of number of messages and bytes received, sent, etc.
     *
     * @param messages received, sent, etc.
     * @param bytes received, sent, etc.
     */
    public void onMessage(final long messages, final long bytes)
    {
        totalBytes += bytes;
        totalMessages += messages;
    }
}
