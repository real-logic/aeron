/*
 * Copyright 2014-2019 Real Logic Ltd.
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

import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.SystemTest;
import io.aeron.archive.client.RecordingTransitionAdapter;
import io.aeron.archive.codecs.RecordingTransitionType;
import io.aeron.archive.status.RecordingPos;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.status.CountersReader;

import static io.aeron.Aeron.NULL_VALUE;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

class Common
{
    static final long MAX_CATALOG_ENTRIES = 1024;
    static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    static final int FRAGMENT_LIMIT = 10;

    static int getRecordingCounterId(final CountersReader counters, final int sessionId)
    {
        int counterId;
        while (NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(counters, sessionId)))
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }

        return counterId;
    }

    static void offer(final Publication publication, final int count, final String prefix)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();

        for (int i = 0; i < count; i++)
        {
            final int length = buffer.putStringWithoutLengthAscii(0, prefix + i);

            while (publication.offer(buffer, 0, length) <= 0)
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }
        }
    }

    static void consume(final Subscription subscription, final int count, final String prefix)
    {
        final MutableInteger received = new MutableInteger(0);

        final FragmentHandler fragmentHandler = new FragmentAssembler(
            (buffer, offset, length, header) ->
            {
                final String expected = prefix + received.value;
                final String actual = buffer.getStringWithoutLengthAscii(offset, length);

                assertEquals(expected, actual);

                received.value++;
            });

        while (received.value < count)
        {
            if (0 == subscription.poll(fragmentHandler, FRAGMENT_LIMIT))
            {
                SystemTest.checkInterruptedStatus();
                Thread.yield();
            }
        }

        assertThat(received.get(), is(count));
    }

    static void awaitPosition(final CountersReader counters, final int counterId, final long position)
    {
        while (counters.getCounterValue(counterId) < position)
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }

    public static void pollForTransition(final RecordingTransitionAdapter recordingTransitionAdapter)
    {
        while (0 == recordingTransitionAdapter.poll())
        {
            SystemTest.checkInterruptedStatus();
            Thread.yield();
        }
    }

    static void awaitTransition(
        final MutableReference<RecordingTransitionType> transitionTypeRef, final RecordingTransitionAdapter adapter)
    {
        transitionTypeRef.set(null);

        do
        {
            pollForTransition(adapter);
        }
        while (transitionTypeRef.get() == null);
    }
}
