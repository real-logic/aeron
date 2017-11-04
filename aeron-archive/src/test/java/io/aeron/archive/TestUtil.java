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
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

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

        if (!tempDirForTest.delete())
        {
            throw new IllegalStateException("Failed to delete: " + tempDirForTest);
        }

        if (!tempDirForTest.mkdir())
        {
            throw new IllegalStateException("Failed to create: " + tempDirForTest);
        }

        return tempDirForTest;
    }

    public static void printf(final String s, final Object... args)
    {
        if (DEBUG)
        {
            System.out.printf(s, args);
        }
    }

    public static void awaitConnectedReply(
        final Subscription controlResponse,
        final long expectedCorrelationId,
        final Consumer<Long> receiveSessionId)
    {
        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
                public void onResponse(
                    final long controlSessionId,
                    final long correlationId,
                    final long relevantId,
                    final ControlResponseCode code,
                    final String errorMessage)
                {
                    if (ControlResponseCode.OK != code)
                    {
                        throw new IllegalStateException(
                            "expected=" + ControlResponseCode.OK + " actual=" + code);
                    }

                    if (correlationId != expectedCorrelationId)
                    {
                        throw new IllegalStateException(
                            "expected=" + expectedCorrelationId + " actual=" + correlationId);
                    }

                    receiveSessionId.accept(controlSessionId);
                }
            },
            controlResponse,
            1
        );

        await(() -> controlResponseAdapter.poll() != 0);
    }

    public static void awaitOk(final Subscription controlResponse, final long expectedCorrelationId)
    {
        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
                public void onResponse(
                    final long controlSessionId,
                    final long correlationId,
                    final long relevantId,
                    final ControlResponseCode code,
                    final String errorMessage)
                {
                    if (ControlResponseCode.OK != code)
                    {
                        System.out.println(errorMessage);
                        throw new IllegalStateException(
                            "expected=" + ControlResponseCode.OK + " actual=" + code);
                    }

                    if (correlationId != expectedCorrelationId)
                    {
                        throw new IllegalStateException(
                            "expected=" + expectedCorrelationId + " actual=" + correlationId);
                    }
                }
            },
            controlResponse,
            1
        );

        await(() -> controlResponseAdapter.poll() != 0);
    }

    static void awaitResponse(final Subscription controlResponse, final long expectedCorrelationId)
    {
        final ControlResponseAdapter controlResponseAdapter = new ControlResponseAdapter(
            new FailControlResponseListener()
            {
                public void onResponse(
                    final long controlSessionId,
                    final long correlationId,
                    final long relevantId,
                    final ControlResponseCode code,
                    final String errorMessage)
                {
                    if (correlationId != expectedCorrelationId)
                    {
                        throw new IllegalStateException(
                            "expected=" + expectedCorrelationId + " actual=" + correlationId);
                    }
                }
            },
            controlResponse,
            1
        );

        await(() -> controlResponseAdapter.poll() != 0);
    }

    static void poll(final Subscription subscription, final FragmentHandler handler)
    {
        await(() -> subscription.poll(handler, 1) > 0);
    }

    public static void offer(final Publication publication, final UnsafeBuffer buffer, final int length)
    {
        await(
            () ->
            {
                final long result = publication.offer(buffer, 0, length);
                if (result > 0)
                {
                    return true;
                }
                else if (result == Publication.ADMIN_ACTION || result == Publication.BACK_PRESSURED)
                {
                    return false;
                }

                throw new IllegalStateException("Unexpected return code: " + result);
            });
    }

    static void offer(final ExclusivePublication publication, final UnsafeBuffer buffer, final int length)
    {
        await(
            () ->
            {
                final long result = publication.offer(buffer, 0, length);
                if (result > 0)
                {
                    return true;
                }
                else if (result == Publication.ADMIN_ACTION || result == Publication.BACK_PRESSURED)
                {
                    return false;
                }

                throw new IllegalStateException("Unexpected return code: " + result);
            });
    }

    public static void await(final BooleanSupplier conditionSupplier)
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
