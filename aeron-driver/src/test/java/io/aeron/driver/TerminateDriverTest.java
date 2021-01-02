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
package io.aeron.driver;

import io.aeron.CommonContext;
import io.aeron.test.Tests;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class TerminateDriverTest
{
    private final TerminationValidator mockTerminationValidator = mock(TerminationValidator.class);

    @Test
    @Timeout(10)
    public void shouldCallTerminationHookUponValidRequest()
    {
        final AtomicBoolean hasTerminated = new AtomicBoolean(false);
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .terminationHook(() -> hasTerminated.lazySet(true))
            .terminationValidator(mockTerminationValidator);

        when(mockTerminationValidator.allowTermination(any(), any(), anyInt(), anyInt())).thenReturn(true);

        try (MediaDriver ignore = MediaDriver.launch(ctx))
        {
            assertTrue(CommonContext.requestDriverTermination(ctx.aeronDirectory(), null, 0, 0));

            while (!hasTerminated.get())
            {
                Tests.yield();
            }
        }

        verify(mockTerminationValidator).allowTermination(any(), any(), anyInt(), anyInt());
    }

    @Test
    @Timeout(10)
    public void shouldNotCallTerminationHookUponInvalidRequest()
    {
        final AtomicBoolean hasTerminated = new AtomicBoolean(false);
        final AtomicBoolean hasCalledTerminationValidator = new AtomicBoolean(false);
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .terminationHook(() -> hasTerminated.lazySet(true))
            .terminationValidator((dir, buffer, offset, length) ->
            {
                hasCalledTerminationValidator.lazySet(true);
                return false;
            });

        try (MediaDriver ignore = MediaDriver.launch(ctx))
        {
            assertTrue(CommonContext.requestDriverTermination(ctx.aeronDirectory(), null, 0, 0));

            while (!hasCalledTerminationValidator.get())
            {
                Tests.yield();
            }
        }

        assertFalse(hasTerminated.get());
    }
}
