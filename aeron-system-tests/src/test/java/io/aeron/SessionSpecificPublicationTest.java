/*
 * Copyright 2014-2020 Real Logic Limited.
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
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static io.aeron.CommonContext.IPC_MEDIA;
import static io.aeron.CommonContext.UDP_MEDIA;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

@RunWith(value = Parameterized.class)
public class SessionSpecificPublicationTest
{
    private static final String ENDPOINT = "localhost:54325";
    private static final int SESSION_ID_1 = 1077;
    private static final int SESSION_ID_2 = 1078;
    private static final int STREAM_ID = 7;
    private static final int MTU_1 = 4096;
    private static final int MTU_2 = 8192;
    private static final int TERM_LENGTH_1 = 64 * 1024;
    private static final int TERM_LENGTH_2 = 128 * 1024;

    @Parameterized.Parameter
    public String mediaType;

    @Parameterized.Parameter(1)
    public ChannelUriStringBuilder channelBuilder;

    @Parameterized.Parameters(name = "media type = {0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(
            new Object[][]
            {
                { UDP_MEDIA, new ChannelUriStringBuilder().media(UDP_MEDIA).endpoint(ENDPOINT) },
                { IPC_MEDIA, new ChannelUriStringBuilder().media(IPC_MEDIA) }
            }
        );
    }

    private final ErrorHandler mockErrorHandler = mock(ErrorHandler.class);
    private final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
        .errorHandler(mockErrorHandler)
        .dirDeleteOnShutdown(true)
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED);

    private final TestMediaDriver testMediaDriver = TestMediaDriver.launch(mediaDriverContext);
    private final Aeron aeron = Aeron.connect();

    @After
    public void after()
    {
        CloseHelper.close(aeron);
        CloseHelper.close(testMediaDriver);
    }

    @Test(expected = RegistrationException.class)
    public void shouldNotCreateExclusivePublicationWhenSessionIdCollidesWithExistingPublication()
    {
        try (Subscription ignored = aeron.addSubscription(channelBuilder.build(), STREAM_ID);
            Publication publication = aeron.addExclusivePublication(channelBuilder.build(), STREAM_ID))
        {
            while (!publication.isConnected())
            {
                Thread.yield();
                SystemTest.checkInterruptedStatus();
            }

            final int existingSessionId = publication.sessionId();

            final String invalidChannel = channelBuilder.sessionId(existingSessionId).build();

            try (Publication ignored1 = aeron.addExclusivePublication(invalidChannel, STREAM_ID))
            {
                fail("Exception should have been thrown due to duplicate session id");
            }
        }
    }

    @Test(expected = RegistrationException.class)
    public void shouldNotCreatePublicationsSharingSessionIdWithDifferentMtu()
    {
        channelBuilder.sessionId(SESSION_ID_1);

        try (Publication ignored1 = aeron.addPublication(channelBuilder.mtu(MTU_1).build(), STREAM_ID);
            Publication ignored2 = aeron.addPublication(channelBuilder.mtu(MTU_2).build(), STREAM_ID))
        {
            fail("Exception should have been thrown due to non-matching mtu");
        }
    }

    @Test(expected = RegistrationException.class)
    public void shouldNotCreatePublicationsSharingSessionIdWithDifferentTermLength()
    {
        channelBuilder.sessionId(SESSION_ID_1);

        final String channelOne = channelBuilder.termLength(TERM_LENGTH_1).build();
        final String channelTwo = channelBuilder.termLength(TERM_LENGTH_2).build();

        try (Publication ignored1 = aeron.addPublication(channelOne, STREAM_ID);
            Publication ignored2 = aeron.addPublication(channelTwo, STREAM_ID))
        {
            fail("Exception should have been thrown due to non-matching term length");
        }
    }

    @Test(expected = RegistrationException.class)
    public void shouldNotCreateNonExclusivePublicationsWithDifferentSessionIdsForTheSameEndpoint()
    {
        channelBuilder.endpoint(ENDPOINT);

        final String channelOne = channelBuilder.sessionId(SESSION_ID_1).build();
        final String channelTwo = channelBuilder.sessionId(SESSION_ID_2).build();

        try (Publication ignored1 = aeron.addPublication(channelOne, STREAM_ID);
            Publication ignored2 = aeron.addPublication(channelTwo, STREAM_ID))
        {
            fail("Exception should have been thrown due using different session ids");
        }
    }
}
