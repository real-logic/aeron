/*
 * Copyright 2014-2025 Real Logic Limited.
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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ReceiverLivenessTrackerTest
{
    @Test
    void shouldNotBeLiveIfNoReceiversAdded()
    {
        final ReceiverLivenessTracker receiverLivenessTracker = new ReceiverLivenessTracker();
        assertFalse(receiverLivenessTracker.hasReceivers());
    }

    @Test
    void shouldLiveIfOneReceiverAdded()
    {
        final ReceiverLivenessTracker receiverLivenessTracker = new ReceiverLivenessTracker();

        final long receiverId = 10001;
        final long nowNs = 10000000000L;

        receiverLivenessTracker.onStatusMessage(receiverId, nowNs);

        assertTrue(receiverLivenessTracker.hasReceivers());
    }

    @Test
    void shouldBeLiveIfReceiverNotTimedOut()
    {
        final long receiverId = 10001;
        final long nowNs = 10000000000L;
        final long timeoutNs = 5000000L;

        final ReceiverLivenessTracker receiverLivenessTracker = new ReceiverLivenessTracker();
        receiverLivenessTracker.onStatusMessage(receiverId, nowNs);
        receiverLivenessTracker.onIdle(nowNs + (timeoutNs - 1), timeoutNs);
        assertTrue(receiverLivenessTracker.hasReceivers());
    }

    @Test
    void shouldNotBeLiveIfReceiverTimedOutExactly()
    {
        final long receiverId = 10001;
        final long nowNs = 10000000000L;
        final long timeoutNs = 5000000L;

        final ReceiverLivenessTracker receiverLivenessTracker = new ReceiverLivenessTracker();
        receiverLivenessTracker.onStatusMessage(receiverId, nowNs);
        receiverLivenessTracker.onIdle(nowNs + timeoutNs, timeoutNs);
        assertFalse(receiverLivenessTracker.hasReceivers());
    }

    @Test
    void shouldNotBeLiveIfReceiverTimedOutAfter()
    {
        final long receiverId = 10001;
        final long nowNs = 10000000000L;
        final long timeoutNs = 5000000L;

        final ReceiverLivenessTracker receiverLivenessTracker = new ReceiverLivenessTracker();
        receiverLivenessTracker.onStatusMessage(receiverId, nowNs);
        receiverLivenessTracker.onIdle(nowNs + (timeoutNs + 1), timeoutNs);
        assertFalse(receiverLivenessTracker.hasReceivers());
    }

    @Test
    void shouldNotBeLiveIfReceiverRemoved()
    {
        final long receiverId1 = 10001;
        final long receiverId2 = 10002;
        final long receiverId3 = 10003;
        final long nowNs = 10000000000L;

        final ReceiverLivenessTracker receiverLivenessTracker = new ReceiverLivenessTracker();
        receiverLivenessTracker.onStatusMessage(receiverId1, nowNs);
        receiverLivenessTracker.onStatusMessage(receiverId2, nowNs);
        receiverLivenessTracker.onStatusMessage(receiverId3, nowNs);

        receiverLivenessTracker.onRemoteClose(receiverId1);
        assertTrue(receiverLivenessTracker.hasReceivers());

        receiverLivenessTracker.onRemoteClose(receiverId2);
        assertTrue(receiverLivenessTracker.hasReceivers());

        receiverLivenessTracker.onRemoteClose(receiverId3);
        assertFalse(receiverLivenessTracker.hasReceivers());
    }

    @Test
    void shouldReturnFalseIfAlreadyRemoved()
    {
        final long receiverId1 = 10001;
        final long receiverId2 = 10002;
        final long receiverId3 = 10003;
        final long nowNs = 10000000000L;

        final ReceiverLivenessTracker receiverLivenessTracker = new ReceiverLivenessTracker();
        receiverLivenessTracker.onStatusMessage(receiverId1, nowNs);
        receiverLivenessTracker.onStatusMessage(receiverId2, nowNs);
        receiverLivenessTracker.onStatusMessage(receiverId3, nowNs);

        assertTrue(receiverLivenessTracker.onRemoteClose(receiverId1));
        assertFalse(receiverLivenessTracker.onRemoteClose(receiverId1));
        assertTrue(receiverLivenessTracker.hasReceivers());

        assertTrue(receiverLivenessTracker.onRemoteClose(receiverId2));
        assertFalse(receiverLivenessTracker.onRemoteClose(receiverId2));
        assertTrue(receiverLivenessTracker.hasReceivers());

        assertTrue(receiverLivenessTracker.onRemoteClose(receiverId3));
        assertFalse(receiverLivenessTracker.onRemoteClose(receiverId3));
        assertFalse(receiverLivenessTracker.hasReceivers());
    }
}