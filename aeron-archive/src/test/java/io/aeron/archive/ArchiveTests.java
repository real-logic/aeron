/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.archive;

import io.aeron.Subscription;
import io.aeron.archive.client.ControlResponseAdapter;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.test.Tests;
import org.agrona.IoUtil;
import org.agrona.SystemUtil;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

public class ArchiveTests
{
    private static final long TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);

    public static File makeTestDirectory()
    {
        final File archiveDir = new File(SystemUtil.tmpDirName(), "archive-test");
        if (archiveDir.exists())
        {
            System.err.println("warning - archive directory exists, deleting: " + archiveDir.getAbsolutePath());
            IoUtil.delete(archiveDir, false);
        }

        if (!archiveDir.mkdirs())
        {
            throw new IllegalStateException("failed to make archive test directory: " + archiveDir.getAbsolutePath());
        }

        return archiveDir;
    }

    public static void awaitConnectedReply(
        final Subscription controlResponse, final long expectedCorrelationId, final LongConsumer receiveSessionId)
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

        Tests.await(() -> controlResponseAdapter.poll() != 0, TIMEOUT_NS);
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

        Tests.await(() -> controlResponseAdapter.poll() != 0, TIMEOUT_NS);
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

        Tests.await(() -> controlResponseAdapter.poll() != 0, TIMEOUT_NS);
    }

    public static TestWatcher newWatcher(final long seed)
    {
        return new TestWatcher()
        {
            public void testFailed(final ExtensionContext context, final Throwable cause)
            {
                System.err.println(context.getDisplayName() + " failed with random seed: " + seed);
            }
        };
    }
}
