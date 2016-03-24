/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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
package uk.co.real_logic.aeron.samples;

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
    private static final String FORMAT_KEY = "aeron.log.inspector.data.format";
    private String originalDataFormatProperty;

    private byte buffer;
    private char expected;

    public LogInspectorAsciiFormatBytesTest(final int buffer, final int expected)
    {
        this.buffer = (byte)buffer;
        this.expected = (char)expected;
    }

    @Parameters(name = "{index}: ascii format[{0}]={1}")
    public static Iterable<Object[]> data()
    {
        return Arrays.asList(
            new Object[][]
            {
                { 0x17,  0x17                     },
                { 0,     0                        },
                { -1,    0                        },
                { Byte.MAX_VALUE,  Byte.MAX_VALUE },
                { Byte.MIN_VALUE,  0              },
            });
    }

    @Test
    public void shouldFormatBytesToAscii()
    {
        System.setProperty(FORMAT_KEY, "ascii");
        final char[] formattedBytes = LogInspector.formatBytes(new UnsafeBuffer(new byte[]{ buffer }), 0, 1);

        Assert.assertEquals(expected, formattedBytes[0]);
    }

    @Before
    public void setUp()
    {
        originalDataFormatProperty = System.getProperty(FORMAT_KEY);
    }

    @After
    public void tearDown()
    {
        if (null == originalDataFormatProperty)
        {
            System.clearProperty(FORMAT_KEY);
        }
        else
        {
            System.setProperty(FORMAT_KEY, originalDataFormatProperty);
        }
    }
}