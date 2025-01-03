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

import io.aeron.*;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

class ImageRateSubscriberLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

class ImageRateSubscriberValues extends ImageRateSubscriberLhsPadding
{
    static final AtomicLongFieldUpdater<ImageRateSubscriberValues> TOTAL_BYTES_UPDATER =
        AtomicLongFieldUpdater.newUpdater(ImageRateSubscriberValues.class, "totalBytes");

    volatile long totalBytes;
}

class ImageRateSubscriberRhsPadding extends ImageRateSubscriberValues
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

/**
 * {@link Runnable} which picks up a single {@link Image} from a {@link Subscription} and reports the rate of
 * consumption.
 */
public final class ImageRateSubscriber extends ImageRateSubscriberRhsPadding implements Runnable
{
    private final int fragmentLimit;
    private final AtomicBoolean running;
    private final Subscription subscription;

    /**
     * Construct the rate subscriber over a {@link Subscription}.
     *
     * @param fragmentLimit per poll operation.
     * @param running       atomic flag to indicate if it should keep running.
     * @param subscription  to pick up the {@link Image} with.
     */
    public ImageRateSubscriber(final int fragmentLimit, final AtomicBoolean running, final Subscription subscription)
    {
        this.fragmentLimit = fragmentLimit;
        this.running = running;
        this.subscription = subscription;
    }

    /**
     * Total bytes consumed.
     *
     * @return total bytes consumed.
     */
    public long totalBytes()
    {
        return totalBytes;
    }

    /**
     * {@inheritDoc}
     */
    public void run()
    {
        while (!subscription.isConnected())
        {
            Thread.yield();
        }

        final Image image = subscription.images().get(0);
        final ImageFragmentAssembler fragmentAssembler = new ImageFragmentAssembler(this::onFragment);
        final IdleStrategy idleStrategy = SampleConfiguration.newIdleStrategy();
        final int fragmentLimit = this.fragmentLimit;

        long failedPolls = 0;
        long successfulPolls = 0;

        while (running.get())
        {
            final int fragmentsRead = image.poll(fragmentAssembler, fragmentLimit);
            if (0 == fragmentsRead)
            {
                ++failedPolls;
            }
            else
            {
                ++successfulPolls;
            }

            idleStrategy.idle(fragmentsRead);
        }

        final double failureRatio = failedPolls / (double)(successfulPolls + failedPolls);
        System.out.format("Subscriber poll failure ratio: %f%n", failureRatio);
    }

    private void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        TOTAL_BYTES_UPDATER.lazySet(this, totalBytes + length);
    }
}
