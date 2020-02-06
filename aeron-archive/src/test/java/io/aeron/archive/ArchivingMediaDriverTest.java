/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.driver.MediaDriver;
import org.agrona.ErrorHandler;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static io.aeron.test.Tests.throwOnClose;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class ArchivingMediaDriverTest
{
    @Test
    void close() throws Exception
    {
        final ErrorHandler errorHandler = mock(ErrorHandler.class);
        final MediaDriver mediaDriver = mock(MediaDriver.class);
        when(mediaDriver.context()).thenReturn(new MediaDriver.Context().errorHandler(errorHandler));
        final IllegalArgumentException driverException = new IllegalArgumentException("driver");
        throwOnClose(mediaDriver, driverException);

        final Archive archive = mock(Archive.class);
        final AbstractMethodError archiveException = new AbstractMethodError("archive");
        throwOnClose(archive, archiveException);

        final ArchivingMediaDriver archivingMediaDriver = new ArchivingMediaDriver(mediaDriver, archive);

        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, archivingMediaDriver::close);

        assertSame(driverException, ex);
        final InOrder inOrder = inOrder(archive, mediaDriver, errorHandler);
        inOrder.verify(archive).close();
        inOrder.verify(errorHandler).onError(archiveException);
        inOrder.verify(mediaDriver).close();
        inOrder.verifyNoMoreInteractions();
    }
}