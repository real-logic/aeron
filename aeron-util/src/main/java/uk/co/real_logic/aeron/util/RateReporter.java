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
package uk.co.real_logic.aeron.util;

import java.util.concurrent.locks.LockSupport;

/**
 * Tracker and reporter of rates.
 */
public class RateReporter implements Runnable
{
    @FunctionalInterface
    public interface ReportingFunction
    {
        void onReport(final double messagesPerSec, final double bytesPerSec);
    }

    private final long reportIntervalNs;
    private final long parkNs;
    private final ReportingFunction reportingFunction;

    private volatile boolean done = false;
    private volatile long totalBytes;
    private volatile long totalMessages;
    private long lastTotalBytes;
    private long lastTotalMessages;
    private long lastTimestamp;

    public RateReporter(final long reportInterval, final ReportingFunction reportingFunction)
    {
        this.reportIntervalNs = reportInterval;
        this.parkNs = reportInterval;
        this.reportingFunction = reportingFunction;
        lastTimestamp = System.nanoTime();
    }

    public void run()
    {
        do
        {
            // "sleep" for report interval
            LockSupport.parkNanos(parkNs);

            // These are not transacted, so they could be off from one another.
            final long currentTotalMessages = totalMessages;
            final long currentTotalBytes = totalBytes;
            final long currentTimestamp = System.nanoTime();

            final long timeSpanNs = currentTimestamp - lastTimestamp;
            final double messagesPerSec = (double)((currentTotalMessages - lastTotalMessages) * reportIntervalNs) /
                    (double)timeSpanNs;
            final double bytesPerSec = (double)((currentTotalBytes - lastTotalBytes) * reportIntervalNs) /
                    (double)timeSpanNs;

            reportingFunction.onReport(messagesPerSec, bytesPerSec);

            lastTotalBytes = currentTotalBytes;
            lastTotalMessages = currentTotalMessages;
            lastTimestamp = currentTimestamp;
        }
        while (!done);
    }

    public void done()
    {
        done = true;
    }

    public void onMessage(final long messages, final long bytes)
    {
        totalBytes += bytes;
        totalMessages += messages;
    }
}
