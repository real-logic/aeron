/*
 * Copyright 2014-2017 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archive.client.ControlResponseAdapter;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.archive.codecs.RecordingDescriptorDecoder;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.Description;
import org.junit.Assert;
import org.junit.rules.TestWatcher;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestUtil
{
    public static final int TIMEOUT_MS = 5000;
    static final boolean DEBUG = false;
    private static final int SLEEP_TIME_NS = 5000;

    public static File makeTempDir()
    {
        final File tempDirForTest;
        try
        {
            tempDirForTest = File.createTempFile("archive", "tmp");
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        Assert.assertTrue(tempDirForTest.delete());
        Assert.assertTrue(tempDirForTest.mkdir());

        return tempDirForTest;
    }

    public static void printf(final String s, final Object... args)
    {
        if (DEBUG)
        {
            System.out.printf(s, args);
        }
    }

    public static void println(final String s)
    {
        if (DEBUG)
        {
            System.out.println(s);
        }
    }

    public static void waitForOk(final Subscription controlResponse, final long expectedCorrelationId)
    {
        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
                public void onResponse(
                    final long correlationId,
                    final long relevantId,
                    final ControlResponseCode code,
                    final String errorMessage)
                {
                    assertThat("Error message: " + errorMessage, code, is(ControlResponseCode.OK));
                    assertThat(errorMessage, isEmptyOrNullString());
                    assertThat(correlationId, is(expectedCorrelationId));
                }
            },
            controlResponse,
            1
        );

        waitFor(() -> controlResponseAdapter.poll() != 0);
    }

    static void waitForNotFound(final Subscription controlResponse, final long expectedCorrelationId)
    {
        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
                public void onResponse(
                    final long correlationId,
                    final long relevantId,
                    final ControlResponseCode code,
                    final String errorMessage)
                {
                    assertThat(correlationId, is(expectedCorrelationId));
                }
            },
            controlResponse,
            1
        );

        waitFor(() -> controlResponseAdapter.poll() != 0);
    }

    static void poll(final Subscription subscription, final FragmentHandler handler)
    {
        waitFor(() -> subscription.poll(handler, 1) > 0);
    }

    public static void offer(final Publication publication, final UnsafeBuffer buffer, final int length)
    {
        waitFor(() -> publication.offer(buffer, 0, length) > 0);
    }

    static void offer(final ExclusivePublication publication, final UnsafeBuffer buffer, final int length)
    {
        waitFor(() -> publication.offer(buffer, 0, length) > 0);
    }

    public static void waitFor(final BooleanSupplier conditionSupplier)
    {
        final long deadlineMs = System.currentTimeMillis() + TIMEOUT_MS;
        while (!conditionSupplier.getAsBoolean())
        {
            LockSupport.parkNanos(SLEEP_TIME_NS);
            if (deadlineMs < System.currentTimeMillis())
            {
                fail();
            }
        }
    }

    public static void awaitSubscriptionIsConnected(final Subscription subscription)
    {
        waitFor(() -> subscription.imageCount() > 0);
    }

    public static void awaitPublicationIsConnected(final Publication publication)
    {
        waitFor(publication::isConnected);
    }

    public static void awaitPublicationIsConnected(final ExclusivePublication publication)
    {
        waitFor(publication::isConnected);
    }

    static RecordingFragmentReader newRecordingFragmentReader(
        final UnsafeBuffer descriptorBuffer, final File archiveDir)
        throws IOException
    {
        final RecordingDescriptorDecoder descriptorDecoder = new RecordingDescriptorDecoder().wrap(
            descriptorBuffer,
            Catalog.DESCRIPTOR_HEADER_LENGTH,
            RecordingDescriptorDecoder.BLOCK_LENGTH,
            RecordingDescriptorDecoder.SCHEMA_VERSION);

        return new RecordingFragmentReader(
            descriptorDecoder,
            archiveDir,
            RecordingFragmentReader.NULL_POSITION,
            RecordingFragmentReader.NULL_LENGTH,
            null);
    }

    public static TestWatcher newWatcher(final Class clazz, final long seed)
    {
        return new TestWatcher()
        {
            protected void failed(final Throwable t, final Description description)
            {
                System.err.println(clazz.getName() + " failed with random seed: " + seed);
            }
        };
    }
}
