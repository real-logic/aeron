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
package io.aeron.test;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.util.RuntimeUtils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

public class InterruptingTestCallback implements BeforeEachCallback, AfterEachCallback
{
    private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(
        r ->
        {
            final Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("interrupting-test-callback");
            return thread;
        });
    private ScheduledFuture<?> timer = null;

    public void afterEach(final ExtensionContext context)
    {
        if (null != timer)
        {
            timer.cancel(false);
            timer = null;
        }
    }

    public void beforeEach(final ExtensionContext context)
    {
        timer = null;
        final InterruptAfter annotation = context.getRequiredTestMethod().getAnnotation(InterruptAfter.class);

        if (null != annotation && !RuntimeUtils.isDebugMode())
        {
            final Thread testThread = Thread.currentThread();
            timer = SCHEDULER.schedule(testThread::interrupt, annotation.value(), annotation.unit());
        }
    }
}
