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
import io.aeron.exceptions.ConcurrentConcludeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientContextTest
{
    @Test
    public void shouldPreventCreatingMultipleClientsWithTheSameContext()
    {
        try (MediaDriver mediaDriver = MediaDriver.launch(new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)))
        {
            final Aeron.Context ctx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());

            try (Aeron ignore = Aeron.connect(ctx))
            {
                assertThrows(ConcurrentConcludeException.class, () -> Aeron.connect(ctx));
            }
        }
    }
}
