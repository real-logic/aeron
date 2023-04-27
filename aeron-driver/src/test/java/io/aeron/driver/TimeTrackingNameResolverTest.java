/*
 * Copyright 2014-2023 Real Logic Limited.
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

import org.agrona.concurrent.NanoClock;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.net.InetAddress;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class TimeTrackingNameResolverTest
{
    @Test
    void throwsNullPointerExceptionIfDelegateResolverIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> new TimeTrackingNameResolver(null, mock(NanoClock.class), mock(DutyCycleTracker.class)));
    }

    @Test
    void throwsNullPointerExceptionIfNanoClockIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> new TimeTrackingNameResolver(mock(NameResolver.class), null, mock(DutyCycleTracker.class)));
    }

    @Test
    void throwsNullPointerExceptionIfDutyCycleTrackerIsNull()
    {
        assertThrowsExactly(
            NullPointerException.class,
            () -> new TimeTrackingNameResolver(mock(NameResolver.class), mock(NanoClock.class), null));
    }

    @Test
    void closeIsANoOpIfDelegateResolverIsNotCloseable()
    {
        final NameResolver delegateResolver = mock(NameResolver.class);
        final NanoClock clock = mock(NanoClock.class);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        resolver.close();

        verifyNoInteractions(delegateResolver, clock, maxTime);
    }

    @Test
    void closeIShouldCloseDelegateResolver() throws Exception
    {
        final NameResolver delegateResolver = mock(
            NameResolver.class, withSettings().extraInterfaces(AutoCloseable.class));
        final NanoClock clock = mock(NanoClock.class);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        resolver.close();

        verify((AutoCloseable)delegateResolver).close();
        verifyNoMoreInteractions(delegateResolver);
        verifyNoInteractions(clock, maxTime);
    }

    @Test
    void doWorkShouldCallActualMethod()
    {
        final NameResolver delegateResolver = mock(NameResolver.class);
        final NanoClock clock = mock(NanoClock.class);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final long nowMs = 1111;
        resolver.doWork(nowMs);

        verify(delegateResolver).doWork(nowMs);
        verifyNoMoreInteractions(delegateResolver);
        verifyNoInteractions(clock, maxTime);
    }

    @Test
    void initShouldCallActualMethod()
    {
        final NameResolver delegateResolver = mock(NameResolver.class);
        final NanoClock clock = mock(NanoClock.class);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final MediaDriver.Context ctx = mock(MediaDriver.Context.class);
        resolver.init(ctx);

        verify(delegateResolver).init(ctx);
        verifyNoMoreInteractions(delegateResolver);
        verifyNoInteractions(clock, maxTime);
    }

    @Test
    void lookupShouldCallActualMethod()
    {
        final NameResolver delegateResolver = mock(NameResolver.class);
        when(delegateResolver.lookup(anyString(), anyString(), anyBoolean()))
            .thenAnswer((Answer<String>)invocation -> invocation.getArgument(0) + "!");
        final NanoClock clock = mock(NanoClock.class);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final String name = "my-host";
        final String endpoint = "endpoint";
        final boolean isReLookup = false;
        assertEquals(name + "!", resolver.lookup(name, endpoint, isReLookup));

        verify(delegateResolver).lookup(name, endpoint, isReLookup);
        verifyNoMoreInteractions(delegateResolver);
        verifyNoInteractions(clock, maxTime);
    }

    @Test
    void resolveShouldMeasureExecutionTime()
    {
        final NameResolver delegateResolver = mock(NameResolver.class);
        when(delegateResolver.resolve(anyString(), anyString(), anyBoolean()))
            .thenAnswer((Answer<InetAddress>)invocation -> InetAddress.getByName(invocation.getArgument(0)));
        final NanoClock clock = mock(NanoClock.class);
        final long beginNs = SECONDS.toNanos(1);
        final long endNs = SECONDS.toNanos(9);
        when(clock.nanoTime()).thenReturn(beginNs, endNs);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final String name = "localhost";
        final String endpoint = "endpoint";
        final boolean isReLookup = true;
        assertEquals(InetAddress.getLoopbackAddress(), resolver.resolve(name, endpoint, isReLookup));

        final InOrder inOrder = inOrder(delegateResolver, clock, maxTime);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).update(beginNs);
        inOrder.verify(delegateResolver).resolve(name, endpoint, isReLookup);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).measureAndUpdate(endNs);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void resolveShouldMeasureExecutionTimeEvenWhenExceptionIsThrown()
    {
        final NameResolver delegateResolver = mock(NameResolver.class);
        final IllegalStateException exception = new IllegalStateException("error");
        when(delegateResolver.resolve(anyString(), anyString(), anyBoolean()))
            .thenThrow(exception);
        final NanoClock clock = mock(NanoClock.class);
        final long beginNs = SECONDS.toNanos(0);
        final long endNs = SECONDS.toNanos(3);
        when(clock.nanoTime()).thenReturn(beginNs, endNs);
        final DutyCycleTracker maxTime = mock(DutyCycleTracker.class);
        final TimeTrackingNameResolver resolver = new TimeTrackingNameResolver(delegateResolver, clock, maxTime);

        final String name = "localhost";
        final String endpoint = "endpoint";
        final boolean isReLookup = true;
        final IllegalStateException error =
            assertThrowsExactly(IllegalStateException.class, () -> resolver.resolve(name, endpoint, isReLookup));
        assertSame(exception, error);

        final InOrder inOrder = inOrder(delegateResolver, clock, maxTime);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).update(beginNs);
        inOrder.verify(delegateResolver).resolve(name, endpoint, isReLookup);
        inOrder.verify(clock).nanoTime();
        inOrder.verify(maxTime).measureAndUpdate(endNs);
        inOrder.verifyNoMoreInteractions();
    }
}
