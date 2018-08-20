/*
 * Copyright 2014-2018 Real Logic Ltd.
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
import org.junit.Test;

public class StartStopSystemTest
{
    @Test
    public void shouldStartAndStopInstantly()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .errorHandler(Throwable::printStackTrace);

        try (MediaDriver ignore = MediaDriver.launchEmbedded(driverCtx))
        {
            final Aeron.Context clientCtx = new Aeron.Context()
                .aeronDirectoryName(driverCtx.aeronDirectoryName());

            //noinspection EmptyTryBlock
            try (Aeron ignored = Aeron.connect(clientCtx))
            {
               // ignore
            }
        }
        finally
        {
            driverCtx.deleteAeronDirectory();
        }
    }
}
