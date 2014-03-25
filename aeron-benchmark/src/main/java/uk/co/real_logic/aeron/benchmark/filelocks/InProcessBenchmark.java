/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.benchmark.filelocks;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InProcessBenchmark
{

    public static void main(String[] args) throws IOException, InterruptedException
    {
        final Lock lock = new ReentrantLock();
        final Condition pinged = lock.newCondition();
        final Condition ponged = lock.newCondition();
        final ByteBuffer data = ByteBuffer.allocate(10);
        final PingPongBenchmark benchmark = new PingPongBenchmark(lock, pinged, ponged, data);

        Thread pingThread = new Thread(benchmark.pinger());
        pingThread.start();
        benchmark.ponger().run();
        pingThread.join();
    }

}
