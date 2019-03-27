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
package io.aeron.driver;

import io.aeron.CommonContext;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

public class TerminateDriverTest
{
    private final TerminationValidator mockTerminationValidator = mock(TerminationValidator.class);

    @Test(timeout = 10_000)
    public void shouldCallTerminationHookUponValidRequest()
    {
        final AtomicBoolean hasTerminated = new AtomicBoolean(false);
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .terminationHook(() -> hasTerminated.lazySet(true))
            .terminationValidator(mockTerminationValidator);

        when(mockTerminationValidator.allowTermination(any(), any(), anyInt(), anyInt())).thenReturn(true);

        try (MediaDriver driver = MediaDriver.launch(ctx))
        {
            assertTrue(CommonContext.requestDriverTermination(ctx.aeronDirectory(), null, 0, 0));

            while (!hasTerminated.get())
            {
                Thread.yield();
            }
        }

        verify(mockTerminationValidator).allowTermination(any(), any(), anyInt(), anyInt());

        ctx.deleteAeronDirectory();
    }

    @Test(timeout = 10_000)
    public void shouldNotCallTerminationHookUponInvalidRequest()
    {
        final AtomicBoolean hasTerminated = new AtomicBoolean(false);
        final AtomicBoolean hasCalledTerminationValidator = new AtomicBoolean(false);
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .terminationHook(() -> hasTerminated.lazySet(true))
            .terminationValidator((dir, buffer, offset, length) ->
            {
                hasCalledTerminationValidator.lazySet(true);
                return false;
            });

        try (MediaDriver driver = MediaDriver.launch(ctx))
        {
            assertTrue(CommonContext.requestDriverTermination(ctx.aeronDirectory(), null, 0, 0));

            while (!hasCalledTerminationValidator.get())
            {
                Thread.yield();
            }
        }

        assertFalse(hasTerminated.get());

        ctx.deleteAeronDirectory();
    }
}
