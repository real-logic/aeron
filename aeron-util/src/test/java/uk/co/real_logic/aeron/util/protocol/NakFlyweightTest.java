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
package uk.co.real_logic.aeron.util.protocol;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class NakFlyweightTest
{
    @Test
    public void canEncodeAndDecodeMultipleSequenceNakPacket()
    {
        final NakFlyweight writer = new NakFlyweight();
        final ByteBuffer buffer = ByteBuffer.allocate(256);
        writer.wrap(buffer);

        writer.channelId(1L);
        writer.sessionId(2L);

        writer.range(3L, 4L, 5L, 6L, 0);

        writer.range(7L, 8L, 9L, 10L, 1);

        writer.countOfRanges(2);

        final NakFlyweight reader = new NakFlyweight();
        reader.wrap(buffer);

        assertThat(reader.channelId(), is(1L));
        assertThat(reader.sessionId(), is(2L));

        assertThat(reader.startTermId(0), is(3L));
        assertThat(reader.startTermOffset(0), is(4L));
        assertThat(reader.endTermId(0), is(5L));
        assertThat(reader.endTermOffset(0), is(6L));

        assertThat(reader.startTermId(1), is(7L));
        assertThat(reader.startTermOffset(1), is(8L));
        assertThat(reader.endTermId(1), is(9L));
        assertThat(reader.endTermOffset(1), is(10L));

        assertThat(reader.countOfRanges(), is(2));
    }
}
