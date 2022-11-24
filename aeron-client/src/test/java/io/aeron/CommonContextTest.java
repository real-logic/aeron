/*
 * Copyright 2014-2022 Real Logic Limited.
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
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.LoggingErrorHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;

import static io.aeron.CommonContext.FALLBACK_LOGGER_PROP_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalMatchers.and;
import static org.mockito.Mockito.*;

class CommonContextTest
{
    @TempDir
    private Path tempDir;

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
        assertSame(System.err, loggingErrorHandler.errorOverflow());
    }

    @Test
    void setupErrorHandlerUsesAFallBackLoggingHandlerForTheOverflow()
    {
        System.setProperty(FALLBACK_LOGGER_PROP_NAME, "no_op");
        try
        {
            final DistinctErrorLog distinctErrorLog = mock(DistinctErrorLog.class);

            final ErrorHandler errorHandler = CommonContext.setupErrorHandler(null, distinctErrorLog);

            assertNotNull(errorHandler);
            final LoggingErrorHandler loggingErrorHandler = assertInstanceOf(LoggingErrorHandler.class, errorHandler);
            assertSame(distinctErrorLog, loggingErrorHandler.distinctErrorLog());
            assertSame(CommonContext.fallbackLogger(), loggingErrorHandler.errorOverflow());
        }
        finally
        {
            System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        }
    }

    @Test
    void setupErrorHandlerReturnsAnErrorHandlerThatFirstInvokesLoggingErrorHandlerBeforeCallingSuppliedErrorHandler()
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

        final AssertionError error = assertThrowsExactly(AssertionError.class, () -> errorHandler.onError(throwable));
        assertSame(userHandlerError, error);

        inOrder.verify(distinctErrorLog).record(throwable);
        inOrder.verify(userErrorHandler).onError(throwable);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void saveExistingErrorsIsANoOpIfErrorBufferIsEmpty()
    {
        final File markFile = tempDir.resolve("mark.dat").toFile();
        final UnsafeBuffer errorBuffer = new UnsafeBuffer(new byte[0]);
        final PrintStream logger = mock(PrintStream.class);
        final String errorFilePrefix = "test-error-";

        CommonContext.saveExistingErrors(markFile, errorBuffer, logger, errorFilePrefix);

        verifyNoInteractions(logger);
    }

    @Test
    void saveExistingErrorsCreatesErrorFileInTheSameDirectoryAsTheCorrespondingMarkFile()
    {
        final File markFile = tempDir.resolve("mark.dat").toFile();
        final DistinctErrorLog errorLog =
            new DistinctErrorLog(new UnsafeBuffer(new byte[10 * 1024]), SystemEpochClock.INSTANCE);
        assertTrue(errorLog.record(new Exception("Just to test")));
        final PrintStream logger = mock(PrintStream.class);
        final String errorFilePrefix = "my-file-";

        CommonContext.saveExistingErrors(markFile, errorLog.buffer(), logger, errorFilePrefix);

        final File[] files = tempDir.toFile().listFiles(
            (dir, name) -> name.endsWith("-error.log") && name.startsWith(errorFilePrefix));
        assertNotNull(files);
        assertEquals(1, files.length);

        verify(logger).println(and(startsWith("WARNING: existing errors saved to: "), endsWith("-error.log")));
        verifyNoMoreInteractions(logger);
    }

    @Test
    void fallbackLoggerReturnsSystemErrorIfNothingSpecified()
    {
        System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        assertSame(System.err, CommonContext.fallbackLogger());
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "stderr", "gaga" })
    void fallbackLoggerReturnsSystemError(final String logger)
    {
        System.setProperty(FALLBACK_LOGGER_PROP_NAME, logger);
        try
        {
            assertSame(System.err, CommonContext.fallbackLogger());
        }
        finally
        {
            System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        }
    }

    @Test
    void fallbackLoggerReturnsSystemOutIfConfigured()
    {
        System.setProperty(FALLBACK_LOGGER_PROP_NAME, "stdout");
        try
        {
            assertSame(System.out, CommonContext.fallbackLogger());
        }
        finally
        {
            System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        }
    }

    @Test
    void fallbackLoggerReturnsNoOpLoggerIfConfigured()
    {
        System.setProperty(FALLBACK_LOGGER_PROP_NAME, "no_op");
        try
        {
            final PrintStream logger = CommonContext.fallbackLogger();
            assertNotNull(logger);
            assertNotSame(System.err, logger);
            assertNotSame(System.out, logger);
            assertSame(logger, CommonContext.fallbackLogger());
        }
        finally
        {
            System.clearProperty(FALLBACK_LOGGER_PROP_NAME);
        }
    }
}
