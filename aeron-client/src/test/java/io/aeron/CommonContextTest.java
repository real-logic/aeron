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
package io.aeron;

import io.aeron.exceptions.ConcurrentConcludeException;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.LoggingErrorHandler;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class CommonContextTest
{
    @Test
    void shouldNotAllowConcludeMoreThanOnce()
    {
        final CommonContext ctx = new CommonContext();
        ctx.conclude();

        assertThrows(ConcurrentConcludeException.class, ctx::conclude);
    }

    @Test
    void setupErrorHandlerReturnsALoggingErrorHandlerInstanceIfNoUserErrorHandlerSupplied()
    {
        final DistinctErrorLog distinctErrorLog = mock(DistinctErrorLog.class);

        final ErrorHandler errorHandler = CommonContext.setupErrorHandler(null, distinctErrorLog);

        assertNotNull(errorHandler);
        final LoggingErrorHandler loggingErrorHandler = assertInstanceOf(LoggingErrorHandler.class, errorHandler);
        assertSame(distinctErrorLog, loggingErrorHandler.distinctErrorLog());
    }

    @Test
    void setupErrorHandlerReturnsAnErrorHandlerThatFirstInvokesUserSuppliedErrorHandlerBeforeTheLoggingErrorHandler()
    {
        final Throwable throwable = new Throwable("Hello, world!");
        final ErrorHandler userErrorHandler = mock(ErrorHandler.class);
        final AssertionError userHandlerError = new AssertionError("user handler error");
        doThrow(userHandlerError).when(userErrorHandler).onError(throwable);
        final DistinctErrorLog distinctErrorLog = mock(DistinctErrorLog.class);
        doReturn(true).when(distinctErrorLog).record(any(Throwable.class));
        final InOrder inOrder = inOrder(userErrorHandler, distinctErrorLog);

        final ErrorHandler errorHandler = CommonContext.setupErrorHandler(userErrorHandler, distinctErrorLog);

        assertNotNull(errorHandler);
        assertNotSame(userErrorHandler, errorHandler);

        errorHandler.onError(throwable);

        inOrder.verify(userErrorHandler).onError(throwable);
        inOrder.verify(distinctErrorLog).record(userHandlerError);
        inOrder.verify(distinctErrorLog).record(throwable);
        inOrder.verifyNoMoreInteractions();
    }
}
