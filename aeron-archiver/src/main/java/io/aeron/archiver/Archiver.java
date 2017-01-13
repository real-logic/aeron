/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.archiver;

import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.ShutdownSignalBarrier;

import static io.aeron.driver.MediaDriver.loadPropertiesFiles;

public class Archiver
{
    /**
     * Start an Archiver (which includes a Media Driver) as a stand-alone process.
     *
     * @param args command line arguments
     * @throws Exception if an error occurs
     */
    public static void main(final String[] args) throws Exception
    {
        loadPropertiesFiles(args);

        try (MediaDriver ignored = MediaDriver.launch())
        {
            new ShutdownSignalBarrier().await();

            System.out.println("Shutdown Driver...");
        }
    }
}
