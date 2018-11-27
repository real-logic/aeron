/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.status.ReadableCounter;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class CounterTest
{
    private static final int COUNTER_TYPE_ID = 101;
    private static final String COUNTER_LABEL = "counter label";

    private final UnsafeBuffer labelBuffer = new UnsafeBuffer(new byte[COUNTER_LABEL.length()]);

    private Aeron clientA;
    private Aeron clientB;
    private MediaDriver driver;

    private final AvailableCounterHandler availableCounterHandlerClientA = mock(AvailableCounterHandler.class);
    private final UnavailableCounterHandler unavailableCounterHandlerClientA = mock(UnavailableCounterHandler.class);
    private AvailableCounterHandler availableCounterHandlerClientB = mock(AvailableCounterHandler.class);
    private UnavailableCounterHandler unavailableCounterHandlerClientB = mock(UnavailableCounterHandler.class);

    private volatile ReadableCounter readableCounter;

    private void launch()
    {
        labelBuffer.putStringWithoutLengthAscii(0, COUNTER_LABEL);

        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .errorHandler(Throwable::printStackTrace)
                .threadingMode(ThreadingMode.SHARED));

        clientA = Aeron.connect(
            new Aeron.Context()
                .availableCounterHandler(availableCounterHandlerClientA)
                .unavailableCounterHandler(unavailableCounterHandlerClientA));

        clientB = Aeron.connect(
            new Aeron.Context()
                .availableCounterHandler(availableCounterHandlerClientB)
                .unavailableCounterHandler(unavailableCounterHandlerClientB));
    }

    @After
    public void after()
    {
        CloseHelper.quietClose(clientB);
        CloseHelper.quietClose(clientA);

        CloseHelper.close(driver);
        driver.context().deleteAeronDirectory();
    }

    @Test(timeout = 2000)
    public void shouldBeAbleToAddCounter()
    {
        launch();

        final Counter counter = clientA.addCounter(
            COUNTER_TYPE_ID,
            null,
            0,
            0,
            labelBuffer,
            0,
            COUNTER_LABEL.length());

        assertFalse(counter.isClosed());

        verify(availableCounterHandlerClientA, timeout(1000))
            .onAvailableCounter(any(CountersReader.class), eq(counter.registrationId()), eq(counter.id()));
        verify(availableCounterHandlerClientB, timeout(1000))
            .onAvailableCounter(any(CountersReader.class), eq(counter.registrationId()), eq(counter.id()));
    }

    @Test(timeout = 2000)
    public void shouldBeAbleToAddReadableCounterWithinHandler()
    {
        availableCounterHandlerClientB = this::createReadableCounter;

        launch();

        final Counter counter = clientA.addCounter(
            COUNTER_TYPE_ID,
            null,
            0,
            0,
            labelBuffer,
            0,
            COUNTER_LABEL.length());

        while (null == readableCounter)
        {
            SystemTest.checkInterruptedStatus();
            SystemTest.sleep(1);
        }

        assertThat(readableCounter.state(), is(CountersReader.RECORD_ALLOCATED));
        assertThat(readableCounter.counterId(), is(counter.id()));
        assertThat(readableCounter.registrationId(), is(counter.registrationId()));
    }

    @Test(timeout = 2000)
    public void shouldCloseReadableCounterOnUnavailableCounter()
    {
        availableCounterHandlerClientB = this::createReadableCounter;
        unavailableCounterHandlerClientB = this::unavailableCounterHandler;

        launch();

        final Counter counter = clientA.addCounter(
            COUNTER_TYPE_ID,
            null,
            0,
            0,
            labelBuffer,
            0,
            COUNTER_LABEL.length());

        while (null == readableCounter)
        {
            SystemTest.checkInterruptedStatus();
            SystemTest.sleep(1);
        }

        assertTrue(!readableCounter.isClosed());
        assertThat(readableCounter.state(), is(CountersReader.RECORD_ALLOCATED));

        counter.close();

        while (!readableCounter.isClosed())
        {
            SystemTest.checkInterruptedStatus();
            SystemTest.sleep(1);
        }
    }

    private void createReadableCounter(
        final CountersReader countersReader, final long registrationId, final int counterId)
    {
        readableCounter = new ReadableCounter(countersReader, registrationId, counterId);
    }

    private void unavailableCounterHandler(
        @SuppressWarnings("unused") final CountersReader countersReader,
        final long registrationId,
        final int counterId)
    {
        assertThat(registrationId, is(readableCounter.registrationId()));
        assertThat(counterId, is(readableCounter.counterId()));

        readableCounter.close();
    }
}
