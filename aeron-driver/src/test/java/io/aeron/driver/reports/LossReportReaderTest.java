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
package io.aeron.driver.reports;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.mockito.InOrder;

import java.nio.ByteBuffer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LossReportReaderTest
{
    private static final int CAPACITY = 1024;
    private final AtomicBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(CAPACITY));
    private final LossReport lossReport = new LossReport(buffer);
    private final LossReportReader.EntryConsumer entryConsumer = mock(LossReportReader.EntryConsumer.class);

    @Test
    public void shouldReadNoEntriesInEmptyReport()
    {
        assertThat(LossReportReader.read(buffer, entryConsumer), is(0));

        verifyZeroInteractions(entryConsumer);
    }

    @Test
    public void shouldReadOneEntry()
    {
        final long initialBytesLost = 32;
        final int timestampMs = 7;
        final int sessionId = 3;
        final int streamId = 1;
        final String channel = "aeron:udp://stuff";
        final String source = "127.0.0.1:8888";

        lossReport.createEntry(initialBytesLost, timestampMs, sessionId, streamId, channel, source);

        assertThat(LossReportReader.read(buffer, entryConsumer), is(1));

        verify(entryConsumer).accept(
            1L, initialBytesLost, timestampMs, timestampMs, sessionId, streamId, channel, source);

        verifyNoMoreInteractions(entryConsumer);
    }

    @Test
    public void shouldReadTwoEntries()
    {
        final long initialBytesLostOne = 32;
        final int timestampMsOne = 7;
        final int sessionIdOne = 3;
        final int streamIdOne = 1;
        final String channelOne = "aeron:udp://stuffOne";
        final String sourceOne = "127.0.0.1:8888";

        final long initialBytesLostTwo = 48;
        final int timestampMsTwo = 17;
        final int sessionIdTwo = 13;
        final int streamIdTwo = 11;
        final String channelTwo = "aeron:udp://stuffTwo";
        final String sourceTwo = "127.0.0.1:9999";

        lossReport.createEntry(initialBytesLostOne, timestampMsOne, sessionIdOne, streamIdOne, channelOne, sourceOne);
        lossReport.createEntry(initialBytesLostTwo, timestampMsTwo, sessionIdTwo, streamIdTwo, channelTwo, sourceTwo);

        assertThat(LossReportReader.read(buffer, entryConsumer), is(2));

        final InOrder inOrder = inOrder(entryConsumer);
        inOrder.verify(entryConsumer).accept(
            1L, initialBytesLostOne, timestampMsOne, timestampMsOne, sessionIdOne, streamIdOne, channelOne, sourceOne);
        inOrder.verify(entryConsumer).accept(
            1L, initialBytesLostTwo, timestampMsTwo, timestampMsTwo, sessionIdTwo, streamIdTwo, channelTwo, sourceTwo);

        verifyNoMoreInteractions(entryConsumer);
    }
}