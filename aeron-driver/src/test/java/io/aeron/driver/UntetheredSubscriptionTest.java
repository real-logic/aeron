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
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.buffer.TestLogFactory;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.agrona.concurrent.status.Position;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class UntetheredSubscriptionTest
{
    private static final long REGISTRATION_ID = 1;
    private static final int TAG_ID = 0;
    private static final int SESSION_ID = 777;
    private static final int STREAM_ID = 3;
    private static final String CHANNEL = CommonContext.IPC_CHANNEL;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int TERM_WINDOW_LENGTH = TERM_BUFFER_LENGTH / 2;
    private static final long TIME_NS = 1000;
    private static final long IMAGE_LIVENESS_TIMEOUT_NS = Configuration.imageLivenessTimeoutNs();
    private static final long UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS = Configuration.untetheredWindowLimitTimeoutNs();
    private static final long UNTETHERED_RESTING_TIMEOUT_NS = Configuration.untetheredRestingTimeoutNs();

    private final RawLog rawLog = TestLogFactory.newLogBuffers(TERM_BUFFER_LENGTH);
    private final AtomicLongPosition publisherLimit = new AtomicLongPosition();
    private IpcPublication ipcPublication;

    @Before
    public void before()
    {
        ipcPublication = new IpcPublication(
            REGISTRATION_ID,
            TAG_ID,
            SESSION_ID,
            STREAM_ID,
            mock(Position.class),
            publisherLimit,
            rawLog,
            TERM_WINDOW_LENGTH,
            Configuration.publicationUnblockTimeoutNs(),
            IMAGE_LIVENESS_TIMEOUT_NS,
            UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS,
            UNTETHERED_RESTING_TIMEOUT_NS,
            TIME_NS,
            mock(SystemCounters.class),
            true);
    }

    @Test
    public void shouldLifeCycleTimeoutsAndRelink()
    {
        final Position tetheredPosition = new AtomicLongPosition();
        tetheredPosition.set(TERM_WINDOW_LENGTH - 1);
        final Position untetheredPosition = new AtomicLongPosition();

        final SubscriptionLink tetheredLink = newLink(1, true);
        final SubscriptionLink untetheredLink = newLink(1, false);

        ipcPublication.addSubscriber(tetheredLink, tetheredPosition);
        ipcPublication.addSubscriber(untetheredLink, untetheredPosition);

        final DriverConductor conductor = mock(DriverConductor.class);
        ipcPublication.updatePublisherLimit();

        final long timeNs = TIME_NS + 1;
        ipcPublication.onTimeEvent(timeNs, 0, conductor);
        verify(conductor, never()).notifyUnavailableImageLink(REGISTRATION_ID, untetheredLink);

        final long windowLimitTimeoutNs = timeNs + UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS;
        ipcPublication.onTimeEvent(windowLimitTimeoutNs, 0, conductor);
        verify(conductor, times(1)).notifyUnavailableImageLink(REGISTRATION_ID, untetheredLink);

        ipcPublication.updatePublisherLimit();
        assertEquals(TERM_WINDOW_LENGTH, publisherLimit.get());

        final long afterLingerTimeoutNs = windowLimitTimeoutNs + UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS;
        ipcPublication.onTimeEvent(afterLingerTimeoutNs, 0, conductor);
        ipcPublication.updatePublisherLimit();
        assertEquals(tetheredPosition.get() + TERM_WINDOW_LENGTH, publisherLimit.get());

        final long afterRestingTimeoutNs = afterLingerTimeoutNs + UNTETHERED_RESTING_TIMEOUT_NS;
        ipcPublication.onTimeEvent(afterRestingTimeoutNs, 0, conductor);

        verify(conductor, times(1)).notifyAvailableImageLink(
            eq(REGISTRATION_ID),
            eq(SESSION_ID),
            eq(untetheredLink),
            anyInt(),
            eq(ipcPublication.joinPosition()),
            eq(rawLog.fileName()),
            eq(CommonContext.IPC_CHANNEL));
    }

    IpcSubscriptionLink newLink(final long registrationId, final boolean isTether)
    {
        final SubscriptionParams params = new SubscriptionParams();
        params.isTether = isTether;

        return new IpcSubscriptionLink(registrationId, STREAM_ID, CHANNEL, null, params);
    }
}
