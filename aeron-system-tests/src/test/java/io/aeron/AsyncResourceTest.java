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
import io.aeron.exceptions.ChannelEndpointException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.ErrorHandler;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Timeout(10)
class AsyncResourceTest
{
    private static final int STREAM_ID = 7777;
    private static final String AERON_IPC = "aeron:ipc";

    @RegisterExtension
    final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    @Test
    void shouldAddAsyncPublications()
    {
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(Tests::onError);

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(Tests::onError)
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
    void shouldDetectInvalidUri()
    {
        final ErrorHandler mockClientErrorHandler = mock(ErrorHandler.class);
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(mockClientErrorHandler);

        final ErrorHandler mockDriverErrorHandler = mock(ErrorHandler.class);
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(mockDriverErrorHandler)
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
    void shouldDetectUnknownHost()
    {
        final ErrorHandler mockClientErrorHandler = mock(ErrorHandler.class);
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(mockClientErrorHandler);

        final ErrorHandler mockDriverErrorHandler = mock(ErrorHandler.class);
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(mockDriverErrorHandler)
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        try (
            TestMediaDriver ignore = TestMediaDriver.launch(driverCtx, testWatcher);
            Aeron aeron = Aeron.connect(clientCtx))
        {
            final long registrationId = aeron.asyncAddPublication("aeron:udp?endpoint=wibble:1234", STREAM_ID);

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
    void shouldDetectChannelEndpointError()
    {
        final ErrorHandler mockClientErrorHandler = mock(ErrorHandler.class);
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(mockClientErrorHandler);

        final ErrorHandler mockDriverErrorHandler = mock(ErrorHandler.class);
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(mockDriverErrorHandler)
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.DEDICATED);

        try (
            TestMediaDriver ignore = TestMediaDriver.launch(driverCtx, testWatcher);
            Aeron aeron = Aeron.connect(clientCtx))
        {
            final String channel = "aeron:udp?control=localhost:34567|endpoint=localhost:2345";
            final Publication publicationOne = aeron.addPublication(channel + "6", STREAM_ID);

            while (publicationOne.channelStatus() == ChannelEndpointStatus.INITIALIZING)
            {
                Tests.yield();
            }

            final long registrationId = aeron.asyncAddPublication(channel + "7", STREAM_ID);

            ConcurrentPublication publicationTwo;
            while (null == (publicationTwo = aeron.getPublication(registrationId)))
            {
                Tests.yield();
            }

            assertFalse(aeron.isCommandActive(registrationId));
            assertFalse(aeron.hasActiveCommands());

            verify(mockClientErrorHandler, timeout(5000)).onError(any(ChannelEndpointException.class));

            assertEquals(ChannelEndpointStatus.ERRORED, publicationTwo.channelStatus());
        }
        finally
        {
            driverCtx.deleteDirectory();
        }
    }
}
