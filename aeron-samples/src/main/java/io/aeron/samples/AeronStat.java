/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.samples;

import java.io.IOException;

/**
 * See {@link io.aeron.tooling.AeronStat}
 */
public class AeronStat
{
    /**
     * Calls {@link io.aeron.tooling.AeronStat#main(String[])}
     *
     * @param args passed to the process.
     * @throws IOException if an error occurs writing to the console.
     * @throws InterruptedException if the thread sleep delay is interrupted.
     */
    public static void main(final String[] args) throws IOException, InterruptedException
    {
        io.aeron.tooling.AeronStat.main(args);
    }
}
