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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.junit.Test;
import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(Theories.class)
public class TermBufferLengthTest
{
    public static final int TEST_TERM_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH * 2;

    @DataPoint
    public static final String UNICAST_TEST_CHANNEL =
        "aeron:udp?endpoint=localhost:54325|" + CommonContext.TERM_LENGTH_PARAM_NAME + "=" + TEST_TERM_LENGTH;

    @DataPoint
    public static final String IPC_TEST_CHANNEL =
        "aeron:ipc?" + CommonContext.TERM_LENGTH_PARAM_NAME + "=" + TEST_TERM_LENGTH;

    public static final int STREAM_ID = 1;

    @Theory
    @Test
    public void shouldHaveCorrectTermBufferLength(final String channel)
    {
        final MediaDriver.Context ctx = new MediaDriver.Context()
            .errorHandler(Throwable::printStackTrace)
            .publicationTermBufferLength(TEST_TERM_LENGTH * 2)
            .ipcTermBufferLength(TEST_TERM_LENGTH * 2);

        try (MediaDriver ignore = MediaDriver.launch(ctx);
            Aeron aeron = Aeron.connect();
            Publication publication = aeron.addPublication(channel, STREAM_ID))
        {
            assertThat(publication.termBufferLength(), is(TEST_TERM_LENGTH));
        }
        finally
        {
            ctx.deleteAeronDirectory();
        }
    }
}
