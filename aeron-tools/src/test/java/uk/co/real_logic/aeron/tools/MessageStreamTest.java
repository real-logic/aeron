/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.tools;

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MessageStreamTest
{
    private static final int MAGIC = 0x0dd01221;
    private static final int BUFFER_SIZE = 200;

    private MessageStream messageStream;
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[BUFFER_SIZE]);

    @Test
    public void createSubscriberSide()
    {
        messageStream = new MessageStream();
    }

    @Test
    public void createSixteenByteSize() throws Exception
    {
        messageStream = new MessageStream(16);
    }

    @Test (expected = Exception.class)
    public void createZeroByteSize() throws Exception
    {
        messageStream = new MessageStream(0);
    }

    @Test (expected = Exception.class)
    public void createNegativeByteSize() throws Exception
    {
        messageStream = new MessageStream(-1);
    }

    @Test (expected = Exception.class)
    public void createVerifiableSizeTooSmall() throws Exception
    {
        messageStream = new MessageStream(15);
    }

    @Test
    public void createNonVerifiable() throws Exception
    {
        messageStream = new MessageStream(1, 1, false);
    }

    @Test
    public void createZeroSizeNonVerifiable() throws Exception
    {
        messageStream = new MessageStream(0, 0, false);
    }

    @Test (expected = Exception.class)
    public void createNegativeSizeNonVerifiable() throws Exception
    {
        messageStream = new MessageStream(-1, -1, false);
    }

    @Test (expected = Exception.class)
    public void createMinGreaterThanMaxSize() throws Exception
    {
        messageStream = new MessageStream(20, 16);
    }

    @Test
    public void createMinEqualToMaxSize() throws Exception
    {
        messageStream = new MessageStream(20, 20);
    }

    @Test
    public void createNullInputStream() throws Exception
    {
        messageStream = new MessageStream(16, null);
    }

    @Test
    public void minVerifiableNullInputStream() throws Exception
    {
        messageStream = new MessageStream(16, null);
        assertThat(messageStream.getNext(buffer), is(16));
    }

    @Test
    public void minNonVerifiableNullInputStream() throws Exception
    {
        messageStream = new MessageStream(0, false, null);
        buffer.putStringUtf8(0, "Test test test!", ByteOrder.nativeOrder());
        messageStream.getNext(buffer); /* This shouldn't do anything to the buffer. */
        final String result = buffer.getStringUtf8(0, ByteOrder.nativeOrder());
        assertThat(result, is("Test test test!"));
    }

    @Test
    public void nonVerifiableNullInputStream() throws Exception
    {
        messageStream = new MessageStream(16, false, null);
        messageStream.getNext(buffer);
        /* It's unlikely that a verifiable message header showed up by pure chance. */
        final int magic = buffer.getInt(0);
        assertThat(magic, not(MAGIC));
    }

    @Test
    public void verifiableNullInputStream() throws Exception
    {
        messageStream = new MessageStream(16, true, null);
        messageStream.getNext(buffer);
        final int magic = buffer.getInt(0);
        assertThat(magic, is(MAGIC));
    }

    @Test (expected = Exception.class)
    public void verifiableOneByteTooBig() throws Exception
    {
        messageStream = new MessageStream(BUFFER_SIZE + 1, true, null);
        messageStream.getNext(buffer);
    }

    @Test (expected = Exception.class)
    public void nonVerifiableOneByteTooBig() throws Exception
    {
        messageStream = new MessageStream(BUFFER_SIZE + 1, false, null);
        assertThat(messageStream.getNext(buffer), is(BUFFER_SIZE));
    }

    @Test
    public void verifiableExactSize() throws Exception
    {
        messageStream = new MessageStream(BUFFER_SIZE, true, null);
        assertThat(messageStream.getNext(buffer), is(BUFFER_SIZE));
    }

    @Test
    public void nonVerifiableExactSize() throws Exception
    {
        messageStream = new MessageStream(BUFFER_SIZE, false, null);
        assertThat(messageStream.getNext(buffer), is(BUFFER_SIZE));
    }

    @Test
    public void getZeroSize() throws Exception
    {
        messageStream = new MessageStream(BUFFER_SIZE, false, null);
        assertThat(messageStream.getNext(buffer, 0), is(0));
    }

    @Test (expected = Exception.class)
    public void getNegativeSize() throws Exception
    {
        messageStream = new MessageStream(BUFFER_SIZE, false, null);
        messageStream.getNext(buffer, -1);
    }
}
