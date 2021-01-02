/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.exceptions.AeronException;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.MutableReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class ReentrantClientTest
{
    @RegisterExtension
    public final MediaDriverTestWatcher testWatcher = new MediaDriverTestWatcher();

    private final TestMediaDriver mediaDriver = TestMediaDriver.launch(new MediaDriver.Context()
        .errorHandler(Tests::onError)
        .dirDeleteOnStart(true),
        testWatcher);

    @AfterEach
    public void after()
    {
        CloseHelper.close(mediaDriver);
        mediaDriver.context().deleteDirectory();
    }

    @Test
    public void shouldThrowWhenReentering()
    {
        final MutableReference<Throwable> expectedException = new MutableReference<>();
        final ErrorHandler errorHandler = expectedException::set;

        try (Aeron aeron = Aeron.connect(new Aeron.Context().errorHandler(errorHandler)))
        {
            final String channel = CommonContext.IPC_CHANNEL;
            final AvailableImageHandler mockHandler = mock(AvailableImageHandler.class);
            doAnswer((invocation) -> aeron.addSubscription(channel, 3))
                .when(mockHandler).onAvailableImage(any(Image.class));

            final Subscription sub = aeron.addSubscription(channel, 1001, mockHandler, null);
            final Publication pub = aeron.addPublication(channel, 1001);

            verify(mockHandler, timeout(5000L)).onAvailableImage(any(Image.class));

            pub.close();
            sub.close();

            assertThat(expectedException.get(), instanceOf(AeronException.class));
        }
    }
}
