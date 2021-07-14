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

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.test.SlowTest;
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.ErrorHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class AsyncResourceTest
{
    private static final int STREAM_ID = 7777;
    private static final String AERON_IPC = "aeron:ipc";

    @RegisterExtension
    final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @Test
    @Timeout(10)
    void shouldAddAsyncPublications()
    {
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(Tests::onError);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        try (
            TestMediaDriver ignore = TestMediaDriver.launch(driverCtx, testWatcher);
            Aeron aeron = Aeron.connect(clientCtx))
        {
            final long registrationIdOne = aeron.asyncAddPublication(AERON_IPC, STREAM_ID);
            final long registrationIdTwo = aeron.asyncAddExclusivePublication(AERON_IPC, STREAM_ID);

            ConcurrentPublication publicationOne;
            while (null == (publicationOne = aeron.getPublication(registrationIdOne)))
            {
                Tests.yield();
            }

            ExclusivePublication publicationTwo;
            while (null == (publicationTwo = aeron.getExclusivePublication(registrationIdTwo)))
            {
                Tests.yield();
            }

            assertFalse(aeron.isCommandActive(registrationIdOne));
            assertFalse(aeron.isCommandActive(registrationIdTwo));
            assertFalse(aeron.hasActiveCommands());

            assertNotNull(publicationOne);
            assertNotNull(publicationTwo);
        }
        finally
        {
            driverCtx.deleteDirectory();
        }
    }

    @Test
    @Timeout(10)
    void shouldDetectInvalidUri()
    {
        final ErrorHandler mockClientErrorHandler = mock(ErrorHandler.class);
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(mockClientErrorHandler);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        try (
            TestMediaDriver ignore = TestMediaDriver.launch(driverCtx, testWatcher);
            Aeron aeron = Aeron.connect(clientCtx))
        {
            final long registrationId = aeron.asyncAddPublication("invalid" + AERON_IPC, STREAM_ID);

            verify(mockClientErrorHandler, timeout(5000)).onError(any(RegistrationException.class));

            assertFalse(aeron.isCommandActive(registrationId));
            assertFalse(aeron.hasActiveCommands());
        }
        finally
        {
            driverCtx.deleteDirectory();
        }
    }

    @Test
    @SlowTest
    @Timeout(60)
    void shouldDetectUnknownHost()
    {
        final ErrorHandler mockClientErrorHandler = mock(ErrorHandler.class);
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(mockClientErrorHandler);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        try (
            TestMediaDriver ignore = TestMediaDriver.launch(driverCtx, testWatcher);
            Aeron aeron = Aeron.connect(clientCtx))
        {
            final long registrationId = aeron.asyncAddPublication("aeron:udp?endpoint=wibble:1234", STREAM_ID);

            verify(mockClientErrorHandler, timeout(55_000)).onError(any(RegistrationException.class));

            assertFalse(aeron.isCommandActive(registrationId));
            assertFalse(aeron.hasActiveCommands());
        }
        finally
        {
            driverCtx.deleteDirectory();
        }
    }
}
