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
package io.aeron.archive;

import io.aeron.Subscription;
import io.aeron.archive.client.ControlResponseAdapter;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.exceptions.TimeoutException;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.LongConsumer;

public class TestUtil
{
    public static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    static final boolean DEBUG = false;

    public static void printf(final String s, final Object... args)
    {
        if (DEBUG)
        {
            System.out.printf(s, args);
        }
    }

    public static File makeTestDirectory()
    {
        final File archiveDir = new File(SystemUtil.tmpDirName(), "archive-test");
        if (archiveDir.exists())
        {
            System.err.println("Warning archive directory exists, deleting: " + archiveDir.getAbsolutePath());
            IoUtil.delete(archiveDir, false);
        }

        if (!archiveDir.mkdirs())
        {
            throw new IllegalStateException("failed to make archive test directory: " + archiveDir.getAbsolutePath());
        }

        return archiveDir;
    }

    public static void awaitConnectedReply(
        final Subscription controlResponse,
        final long expectedCorrelationId,
        final LongConsumer receiveSessionId)
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

    public static void await(final BooleanSupplier conditionSupplier)
    {
        final long deadlineNs = System.nanoTime() + TIMEOUT_NS;
        while (!conditionSupplier.getAsBoolean())
        {
            if (Thread.currentThread().isInterrupted())
            {
                throw new IllegalStateException("unexpected interrupt in test");
            }

            if (System.nanoTime() > deadlineNs)
            {
                throw new TimeoutException();
            }

            Thread.yield();
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
