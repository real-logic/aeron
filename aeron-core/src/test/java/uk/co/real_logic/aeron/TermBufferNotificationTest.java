/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.LongStream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.TermBufferNotification.TERMS_STORED;

public class TermBufferNotificationTest
{

    private static final int ROLLOVER_TERM_COUNT = TERMS_STORED * 2;
    private static final Runnable NOTHING = () ->
    {

    };

    private final TermBufferNotification notification = new TermBufferNotification();
    private final ByteBuffer termBuffer1 = ByteBuffer.allocate(10);
    private final ByteBuffer termBuffer2 = ByteBuffer.allocate(10);

    @Test
    public void hashesIntoTheAddressingSpace()
    {
        int previousIndex = -1;
        for (long termId = 0L; termId < ROLLOVER_TERM_COUNT; termId++)
        {
            final int index = notification.hash(termId);
            assertThat(index, not(previousIndex));
            assertThat(index, greaterThanOrEqualTo(0));
            assertThat(index, lessThan(TERMS_STORED));
            previousIndex = index;
        }
    }

    @Test
    public void addingATermMakesItReadable()
    {
        writeAndAssertTermBufferRead(0L, termBuffer1);
    }

    @Test
    public void termsCanRollOverIndefinitely()
    {
        LongStream.range(0L, ROLLOVER_TERM_COUNT)
                  .forEach(termId ->
                  {
                      final ByteBuffer termBuffer = (termId % 2) == 0L ? termBuffer1 : termBuffer2;
                      writeAndAssertTermBufferRead(termId, termBuffer);
                  });
    }

    @Test
    public void readingAnUnmappedTermBlocksTheReader() throws InterruptedException
    {
        assertReadingTermBufferBlocks();
    }

    @Test
    public void addingATermNotifiesAReader() throws InterruptedException
    {
        final AtomicBoolean returned = new AtomicBoolean(false);
        final Runnable reader = () ->
        {
            final ByteBuffer buffer = notification.termBuffer(0L);
            returned.set(buffer != null);
        };
        final Runnable writer = () ->
        {
            notification.newTermBufferMapped(0L, termBuffer1);
        };
        withReaderAndWriter(reader, writer, 100);
        assertThat(returned.get(), is(true));
    }

    @Test
    public void removingATermMakesItUnreadable() throws InterruptedException
    {
        writeAndAssertTermBufferRead(0L, termBuffer1);
        notification.endOfTermBuffer(0L);
        assertReadingTermBufferBlocks();
    }

    private void assertReadingTermBufferBlocks() throws InterruptedException
    {
        final AtomicBoolean returned = new AtomicBoolean(false);
        final Runnable reader = () ->
        {
            final ByteBuffer buffer = notification.termBuffer(0L);
            returned.set(buffer != null);
        };
        withReaderAndWriter(reader, NOTHING, 50);

        assertThat(returned.get(), is(false));
    }

    private void writeAndAssertTermBufferRead(final long termId, final ByteBuffer termBuffer)
    {
        notification.newTermBufferMapped(termId, termBuffer);
        assertThat(notification.termBuffer(termId), is(termBuffer));
    }

    private void withReaderAndWriter(final Runnable reader, final Runnable writer, final long timeout) throws InterruptedException
    {
        final ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);
        executor.execute(reader);
        executor.execute(writer);
        final boolean terminated = executor.awaitTermination(timeout, MILLISECONDS);
        if (!terminated)
        {
            executor.shutdownNow();
        }
    }

}
