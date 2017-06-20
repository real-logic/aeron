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
package io.aeron.archiver;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.archiver.client.ArchiveClient;
import io.aeron.archiver.codecs.ControlResponseCode;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class TestUtil
{
    public static final int TIMEOUT = 5000;
    public static final boolean DEBUG = false;
    public static final int SLEEP_TIME_NS = 5000;

    public static File makeTempDir() throws IOException
    {
        final File tempDirForTest = File.createTempFile("archiver", "tmp");
        // we really need a temp dir, not a file... delete and remake!
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

    public static void waitForOk(final ArchiveClient client, final Subscription reply, final long correlationId1)
    {
        waitFor(() -> client.pollResponses(reply, new FailResponseListener()
        {
            public void onResponse(final long correlationId2, final ControlResponseCode code, final String errorMessage)
            {
                assertThat("Error message: " + errorMessage, code, is(ControlResponseCode.OK));
                assertThat(errorMessage, isEmptyOrNullString());
                assertThat(correlationId1, is(correlationId2));
            }
        }, 1) != 0);
    }

    public static void waitForFail(final ArchiveClient client, final Subscription reply, final long correlationId1)
    {
        waitFor(() -> client.pollResponses(reply, new FailResponseListener()
        {
            public void onResponse(final long correlationId2, final ControlResponseCode code, final String errorMessage)
            {
                assertThat(code, not(ControlResponseCode.OK));
                assertThat(correlationId1, is(correlationId2));
            }
        }, 1) != 0);
    }

    public static void poll(final Subscription subscription, final FragmentHandler handler)
    {
        waitFor(() -> subscription.poll(handler, 1) > 0);
    }

    public static void offer(final Publication publication, final UnsafeBuffer buffer, final int length)
    {
        waitFor(() -> publication.offer(buffer, 0, length) > 0);
    }

    public static void waitFor(final BooleanSupplier conditionSupplier)
    {
        final long limit = System.currentTimeMillis() + TIMEOUT;
        while (!conditionSupplier.getAsBoolean())
        {
            LockSupport.parkNanos(SLEEP_TIME_NS);
            if (limit < System.currentTimeMillis())
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
}
