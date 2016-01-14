/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.driver.event;

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class EventEncoderTest
{
    private static final String MESSAGE = "End of the world!";
    private static final String DECLARING_CLASS = EventEncoderTest.class.getName();
    private static final String METHOD = "someMethod";
    private static final String FILE = EventEncoderTest.class.getSimpleName() + ".java";
    private static final int LINE_NUMBER = 10;

    private static final int BUFFER_LENGTH = 1024 * 10;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[BUFFER_LENGTH]);

    @Test
    public void dissectAsExceptionShouldContainTheValuesEncoded()
    {
        final Exception ex = new Exception(MESSAGE);
        ex.fillInStackTrace();

        EventEncoder.encode(buffer, ex);
        final String written = EventDissector.dissectAsException(EventCode.EXCEPTION, buffer, 0);

        assertThat(written, containsString(MESSAGE));
        assertThat(written, containsString(ex.getClass().getName()));
        assertThat(written, containsString(getClass().getName()));
        assertThat(written, containsString(getClass().getSimpleName() + ".java"));
        assertThat(written, containsString("dissectAsExceptionShouldContainTheValuesEncoded"));
        assertThat(written, containsString(":43")); // Line number of ex.fillInStackTrace() above
    }

    @Test
    public void dissectAsInvocationShouldContainTheValuesEncoded()
    {
        final StackTraceElement element = new StackTraceElement(DECLARING_CLASS, METHOD, FILE, LINE_NUMBER);

        EventEncoder.encode(buffer, element);
        final String written = EventDissector.dissectAsInvocation(EventCode.EXCEPTION, buffer, 0);

        assertThat(written, containsString(DECLARING_CLASS));
        assertThat(written, containsString(METHOD));
        assertThat(written, containsString(FILE));
        assertThat(written, containsString(":" + LINE_NUMBER));
    }
}
