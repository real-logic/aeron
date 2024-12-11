/*
 * Copyright 2023 Adaptive Financial Consulting Limited.
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
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

@ExtendWith(InterruptingTestCallback.class)
public class UriValidationTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void setup()
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(ThreadingMode.SHARED), testWatcher);

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @ParameterizedTest
    @ValueSource(strings = {"aeron:udp?endpoint=localhost:8080|ssc=false", "aeron:ipc?mtu=2K"})
    @InterruptAfter(10)
    void shouldRejectChannelUrisWhenTooLong(final String baseUri)
    {
        final int streamId = 15;
        final String uri = Tests.generateStringWithSuffix(
            baseUri + "|alias=too-looong-", "x", ChannelUri.MAX_URI_LENGTH);

        assertUriRejected(uri, () -> aeron.addPublication(uri, streamId));
        assertUriRejected(uri, () -> aeron.addSubscription(uri, streamId));
    }

    private void assertUriRejected(final String uri, final Executable executable)
    {
        final RegistrationException exception =
            assertThrowsExactly(RegistrationException.class, executable);
        assertEquals(ErrorCode.INVALID_CHANNEL, exception.errorCode());
        assertThat(exception.getMessage(), containsString(uri.substring(0, ChannelUri.MAX_URI_LENGTH)));
    }
}
