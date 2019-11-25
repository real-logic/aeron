/*
 * Copyright 2014-2019 Real Logic Ltd.
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
package io.aeron.test;

import io.aeron.driver.MediaDriver;

public final class JavaTestMediaDriver implements TestMediaDriver
{
    private MediaDriver mediaDriver;

    private JavaTestMediaDriver(final MediaDriver mediaDriver)
    {
        this.mediaDriver = mediaDriver;
    }

    public void close()
    {
        mediaDriver.close();
    }

    public static JavaTestMediaDriver launch(final MediaDriver.Context context)
    {
        final MediaDriver mediaDriver = MediaDriver.launch(context);
        return new JavaTestMediaDriver(mediaDriver);
    }

    public MediaDriver.Context context()
    {
        return mediaDriver.context();
    }

    public String aeronDirectoryName()
    {
        return mediaDriver.aeronDirectoryName();
    }
}
