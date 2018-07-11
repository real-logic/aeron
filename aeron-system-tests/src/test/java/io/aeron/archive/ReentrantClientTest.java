/*
 * Copyright 2018 Real Logic Ltd.
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
package io.aeron.archive;

import io.aeron.*;
import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.AeronException;
import org.agrona.ErrorHandler;
import org.agrona.collections.MutableReference;
import org.junit.After;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class ReentrantClientTest
{
    final MediaDriver mediaDriver = MediaDriver.launch(new MediaDriver.Context().dirDeleteOnStart(true));

    @After
    public void after()
    {
        mediaDriver.close();
        mediaDriver.context().deleteAeronDirectory();
    }

    @Test
    public void shouldThrowWhenReentering()
    {
        final MutableReference<Throwable> expectedException = new MutableReference<>();
        final ErrorHandler errorHandler = expectedException::set;

        try (Aeron aeron = Aeron.connect(new Aeron.Context().errorHandler(errorHandler)))
        {
            final String channel = CommonContext.IPC_CHANNEL;
            final AvailableImageHandler handler = (image) -> aeron.addSubscription(channel, 3);

            final Subscription sub = aeron.addSubscription(channel, 1, handler, null);
            final Publication pub = aeron.addPublication(channel, 1);

            sub.close();
            pub.close();

            assertThat(expectedException.get(), instanceOf(AeronException.class));
        }
    }
}
