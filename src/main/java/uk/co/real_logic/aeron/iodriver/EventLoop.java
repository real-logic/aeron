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

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

/**
 * Event loop for JVM based iodriver
 *
 * Contains Selector logic
 *
 * Does not provide timers
 * Deliberately simple and braindead
 */
public class EventLoop implements Closeable
{
    /** Default select timeout (20 msec) */
    public static final long DEFAULT_SELECT_TIMEOUT = 20;

    private Selector selector;
    private volatile int done;

    public EventLoop() throws Exception
    {
        this.selector = Selector.open(); // yes, SelectorProvider, blah, blah
        this.done = 0;
    }

    /*
     * TODO: add handler to call when selected
     */
    public SelectionKey registerForRead(final SelectableChannel channel) throws Exception
    {
        return channel.register(selector, SelectionKey.OP_READ);
    }

    /**
     * Main loop of the EventLoop
     *
     * Everything is done here and bubbles up to handlers.
     */
    public void run()
    {
        try
        {
            while (0 == done)
            {
                select();
                processSelectedKeys();
            }
        }
        catch (Exception e)
        {
            // TODO: log exception
        }
    }

    /**
     * Close EventLoop down. Returns immediately.
     */
    public void close()
    {
        done = 1;
        wakeup();
    }

    /**
     * Wake up EventLoop if blocked.
     */
    public void wakeup()
    {
        selector.wakeup();
    }

    private void select() throws Exception
    {
        int readyChannels = selector.select(DEFAULT_SELECT_TIMEOUT);
    }

    private void selectNow() throws IOException
    {
        int readyChannels = selector.selectNow();
    }

    private void processSelectedKey(final SelectionKey key)
    {
        if (key.isAcceptable())
        {

        }
        else if (key.isConnectable())
        {

        }
        else if (key.isReadable())
        {

        }
        else if (key.isWritable())
        {

        }
    }

    private void processSelectedKeys() throws Exception
    {
        // Try this as we would like to be able to have the JVM optimize the Set<SelectionKey> for us instead of instrumenting it.
        selector.selectedKeys().stream().forEach(this::processSelectedKey);
    }
}
