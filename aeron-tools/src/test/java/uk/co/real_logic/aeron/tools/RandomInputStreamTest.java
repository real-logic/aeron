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

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
/**
 * Not really a lot to test here. The random number generator can't be mocked in it's current state,
 * so these tests are pretty unreliable. When our random number generator can be mocked, update these tests.
 */
public class RandomInputStreamTest
{
    private RandomInputStream stream;

    @Before
    public void setUp()
    {
        stream = new RandomInputStream();
    }

    @Test
    public void readByte() throws Exception
    {
        final int value = stream.read();
        assertThat(value, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(255)));
    }

    @Test
    public void readByteArraySmall() throws Exception
    {
        final byte[] array = new byte[1];
        final int read = stream.read(array);
        assertThat(read, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(1)));
    }

    @Test
    public void readByteArrayLarge() throws Exception
    {
        final byte[] array = new byte[8192];
        final int read = stream.read(array);
        // currently returns between 0 and 400 bytes
        assertThat(read, both(greaterThanOrEqualTo(0)).and(lessThanOrEqualTo(400)));
    }

    @Test
    public void readByteArrayOffset() throws Exception
    {
        final byte[] array = new byte[8192];
        final int read = stream.read(array, 100, 900);
        assertThat("FAIL: Expected read to return given length",
            read, is(900));
        assertThat("FAIL: Expected byte before read location to be 0",
            array[99], is((byte)0));
        assertThat("FAIL: Expected byte after read location to be 0",
            array[1001], is((byte)0));
    }
}
