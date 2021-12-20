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
package io.aeron.test;

import org.agrona.concurrent.NanoClock;

public class StubNanoClock implements NanoClock
{
    private final long tick;
    private long time;

    public StubNanoClock(final long initialTime, final long tick)
    {
        this.time = initialTime;
        this.tick = tick;
    }

    public long nextTime()
    {
        return nextTime(tick);
    }

    public long nextTime(final long tick)
    {
        time += tick;
        return time;
    }

    public long nanoTime()
    {
        return time;
    }
}