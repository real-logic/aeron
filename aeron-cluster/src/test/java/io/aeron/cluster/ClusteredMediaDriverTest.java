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
package io.aeron.cluster;

import io.aeron.archive.Archive;
import io.aeron.driver.MediaDriver;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.CountedErrorHandler;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.sql.BatchUpdateException;

import static io.aeron.test.Tests.throwOnClose;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class ClusteredMediaDriverTest
{
    @Test
    void close() throws Exception
    {
        final ErrorHandler errorHandler = mock(ErrorHandler.class);
        final CountedErrorHandler countedErrorHandler = mock(CountedErrorHandler.class);

        final MediaDriver driver = mock(MediaDriver.class);
        when(driver.context()).thenReturn(new MediaDriver.Context().errorHandler(errorHandler));
        final AssertionError driverException = new AssertionError("driver");
        throwOnClose(driver, driverException);

        final Archive archive = mock(Archive.class);
        when(archive.context()).thenReturn(new Archive.Context().countedErrorHandler(countedErrorHandler));
        final ArithmeticException archiveException = new ArithmeticException("archive");
        throwOnClose(archive, archiveException);

        final ConsensusModule consensusModule = mock(ConsensusModule.class);
        final BatchUpdateException consensusModuleException = new BatchUpdateException();
        throwOnClose(consensusModule, consensusModuleException);

        final ClusteredMediaDriver clusteredMediaDriver =
            new ClusteredMediaDriver(driver, archive, consensusModule);

        final AssertionError ex = assertThrows(AssertionError.class, clusteredMediaDriver::close);

        assertSame(driverException, ex);
        final InOrder inOrder = inOrder(errorHandler, countedErrorHandler);
        inOrder.verify(countedErrorHandler).onError(consensusModuleException);
        inOrder.verify(errorHandler).onError(archiveException);
        inOrder.verifyNoMoreInteractions();
    }
}