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
package uk.co.real_logic.aeron.iodriver;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Main class for JVM-based iodriver
 */
public class IoDriver
{
    public static void main(String[] args)
    {
        try (final EventLoop selectLoop = new EventLoop())
        {
            Executor executor = Executors.newFixedThreadPool(2);

            executor.execute(selectLoop);

            // TODO: need the other thread scanning Term buffers and control buffer or have this thread do it.

            // Example of using FrameHandler. A receiver and a source sending to it.

            RecvFrameHandler rcv = new RecvFrameHandler(new InetSocketAddress(41234), selectLoop);
            SrcFrameHandler src = new SrcFrameHandler(new InetSocketAddress(0),
                                                      new InetSocketAddress("localhost", 41234),
                                                      selectLoop);


            while (true)
            {
                Thread.sleep(1000);
            }
        }
        catch (InterruptedException ie)
        {
            // catch this OK. We should finally close on it also.
        }
        catch (Exception e)
        {
            System.err.println(e);
        }
    }
}
