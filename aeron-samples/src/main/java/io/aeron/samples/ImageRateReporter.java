/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.samples;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Report the rate received to an {@link io.aeron.Image} and print to {@link System#out}.
 */
public final class ImageRateReporter implements Runnable
{
    private final int messageLength;
    private final AtomicBoolean running;
    private final ImageRateSubscriber subscriber;

    /**
     * Construct a reporter for a single image.
     *
     * @param messageLength of each message.
     * @param running       flag to control reporter, so it can be stopped running.
     * @param subscriber    for the image.
     */
    public ImageRateReporter(final int messageLength, final AtomicBoolean running, final ImageRateSubscriber subscriber)
    {
        this.messageLength = messageLength;
        this.running = running;
        this.subscriber = subscriber;
    }

    /**
     * {@inheritDoc}
     */
    public void run()
    {
        long lastTimestampMs = System.currentTimeMillis();
        long lastTotalBytes = subscriber.totalBytes();
        final int messageLength = this.messageLength;

        while (running.get())
        {
            LockSupport.parkNanos(1_000_000_000);

            final long newTimestampMs = System.currentTimeMillis();
            final long newTotalBytes = subscriber.totalBytes();

            final long durationMs = newTimestampMs - lastTimestampMs;
            final long bytesTransferred = newTotalBytes - lastTotalBytes;

            System.out.format(
                "Duration %dms - %,d messages - %,d payload bytes%n",
                durationMs, bytesTransferred / messageLength, bytesTransferred);

            lastTimestampMs = newTimestampMs;
            lastTotalBytes = newTotalBytes;
        }
    }
}
