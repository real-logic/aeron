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
package io.aeron.samples;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class LogInspectorAsciiFormatBytesTest
{
    private String originalDataFormatProperty;

    private final byte buffer;
    private final char expected;

    public LogInspectorAsciiFormatBytesTest(final int buffer, final int expected)
    {
        this.buffer = (byte)buffer;
        this.expected = (char)expected;
    }

    @Parameters(name = "{index}: ascii format[{0}]={1}")
    public static Iterable<Object[]> data()
    {
        return Arrays.asList(new Object[][]
            {
                { 0x17, 0x17 },
                { 0, 0 },
                { -1, 0 },
                { Byte.MAX_VALUE, Byte.MAX_VALUE },
                { Byte.MIN_VALUE, 0 },
            });
    }

    @Before
    public void before()
    {
        originalDataFormatProperty = System.getProperty(LogInspector.AERON_LOG_DATA_FORMAT_PROP_NAME);
    }

    @After
    public void after()
    {
        if (null == originalDataFormatProperty)
        {
            System.clearProperty(LogInspector.AERON_LOG_DATA_FORMAT_PROP_NAME);
        }
        else
        {
            System.setProperty(LogInspector.AERON_LOG_DATA_FORMAT_PROP_NAME, originalDataFormatProperty);
        }
    }

    @Test
    public void shouldFormatBytesToAscii()
    {
        System.setProperty(LogInspector.AERON_LOG_DATA_FORMAT_PROP_NAME, "ascii");
        final char[] formattedBytes = LogInspector.formatBytes(new UnsafeBuffer(new byte[]{ buffer }), 0, 1);

        Assert.assertEquals(expected, formattedBytes[0]);
    }
}