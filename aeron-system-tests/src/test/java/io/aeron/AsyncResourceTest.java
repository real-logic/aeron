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

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.test.SlowTest;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SuppressWarnings("try")
class AsyncResourceTest
{
    private static final int STREAM_ID = 7777;
    private static final String AERON_IPC = "aeron:ipc";

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        try
        {
            driver = TestMediaDriver.launch(driverCtx, testWatcher);
        }
        finally
        {
            testWatcher.dataCollector().add(driverCtx.aeronDirectory());
        }
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.close(driver);
    }

    @Test
    @Timeout(10)
    void shouldAddAsyncPublications()
    {
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(Tests::onError);

        try (Aeron aeron = Aeron.connect(clientCtx))
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
    }

    @Test
    @Timeout(10)
    void shouldAsyncRemovePublication()
    {
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(Tests::onError);

        try (Aeron aeron = Aeron.connect(clientCtx))
        {
            final long registrationId = aeron.asyncAddPublication(AERON_IPC, STREAM_ID);

            Publication publication;
            while (null == (publication = aeron.getPublication(registrationId)))
            {
                Tests.yield();
            }

            assertFalse(aeron.hasActiveCommands());
            assertEquals(registrationId, publication.registrationId());

            aeron.asyncRemovePublication(registrationId);
            assertTrue(publication.isClosed());
            assertNull(aeron.getPublication(registrationId));
        }
    }

    @Test
    @Timeout(10)
    void shouldDetectInvalidUri()
    {
        final ErrorHandler mockClientErrorHandler = mock(ErrorHandler.class);
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(mockClientErrorHandler);

        try (Aeron aeron = Aeron.connect(clientCtx))
        {
            testWatcher.ignoreErrorsMatching(
                (s) -> s.contains("Aeron URIs must start with") || s.contains("invalid channel"));

            final long registrationId = aeron.asyncAddPublication("invalid" + AERON_IPC, STREAM_ID);

            verify(mockClientErrorHandler, timeout(5000)).onError(any(RegistrationException.class));

            assertFalse(aeron.isCommandActive(registrationId));
            assertFalse(aeron.hasActiveCommands());
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

        try (Aeron aeron = Aeron.connect(clientCtx))
        {
            testWatcher.ignoreErrorsMatching(
                (s) -> s.contains("unresolved") || s.contains("unknown host"));

            final long registrationId = aeron.asyncAddPublication("aeron:udp?endpoint=wibble:1234", STREAM_ID);

            verify(mockClientErrorHandler, timeout(55_000)).onError(any(RegistrationException.class));

            assertFalse(aeron.isCommandActive(registrationId));
            assertFalse(aeron.hasActiveCommands());
        }
    }
}
