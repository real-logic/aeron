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
package uk.co.real_logic.aeron.driver;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.Set;
import java.util.function.IntSupplier;

/**
 * Encapsulation of NIO Selector logic for integration into Receiver Thread and Conductor Thread
 */
public class NioSelector implements AutoCloseable
{
    private static final Field SELECTED_KEYS_FIELD;
    private static final Field PUBLIC_SELECTED_KEYS_FIELD;

    static
    {
        Field selectKeysField = null;
        Field publicSelectKeysField = null;

        try
        {
            final Class<?> clazz = Class.forName("sun.nio.ch.SelectorImpl", false, ClassLoader.getSystemClassLoader());

            if (clazz.isAssignableFrom(Selector.open().getClass()))
            {
                selectKeysField = clazz.getDeclaredField("selectedKeys");
                selectKeysField.setAccessible(true);

                publicSelectKeysField = clazz.getDeclaredField("publicSelectedKeys");
                publicSelectKeysField.setAccessible(true);
            }
        }
        catch (final Exception ignore)
        {
        }

        SELECTED_KEYS_FIELD = selectKeysField;
        PUBLIC_SELECTED_KEYS_FIELD = publicSelectKeysField;
    }

    private final Selector selector;
    private final NioSelectedKeySet selectedKeySet;

    /**
     * Construct a selector
     */
    public NioSelector()
    {
        NioSelectedKeySet tmpSet = null;

        try
        {
            this.selector = Selector.open(); // yes, SelectorProvider, blah, blah
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        if (null != PUBLIC_SELECTED_KEYS_FIELD)
        {
            try
            {
                tmpSet = new NioSelectedKeySet();

                SELECTED_KEYS_FIELD.set(selector, tmpSet);
                PUBLIC_SELECTED_KEYS_FIELD.set(selector, tmpSet);
            }
            catch (final Exception ignore)
            {
                tmpSet = null;
            }
        }

        selectedKeySet = tmpSet;
    }

    /**
     * Register channel for read.
     *
     * @param channel to select for read
     * @param obj to associate with read
     * @return SelectionKey for registration for cancel
     */
    public SelectionKey registerForRead(final SelectableChannel channel, final IntSupplier obj)
    {
        try
        {
            return channel.register(selector, SelectionKey.OP_READ, obj);
        }
        catch (final ClosedChannelException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Close NioSelector down. Returns immediately.
     */
    public void close()
    {
        selector.wakeup();
        try
        {
            selector.close();
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Explicit event loop processing as a poll
     *
     * @return the number of frames processed.
     */
    public int processKeys()
    {
        try
        {
            selector.selectNow();

            int handledFrames = 0;
            if (null != PUBLIC_SELECTED_KEYS_FIELD)
            {
                final SelectionKey[] keys = selectedKeySet.keys();

                for (int i = selectedKeySet.size() - 1; i >= 0; i--)
                {
                    final SelectionKey key = keys[i];

                    if (key.isReadable())
                    {
                        handledFrames += ((IntSupplier)key.attachment()).getAsInt();
                    }
                }

                selectedKeySet.reset();
            }
            else
            {
                handledFrames = handleSelectedKeys();
            }

            return handledFrames;
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Explicit call to selectNow but without processing of selected keys.
     */
    public void selectNowWithoutProcessing()
    {
        try
        {
            selector.selectNow();
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private int handleSelectedKeys()
    {
        int handledFrames = 0;
        final Set<SelectionKey> selectedKeys = selector.selectedKeys();
        final Iterator<SelectionKey> iter = selectedKeys.iterator();

        while (iter.hasNext())
        {
            final SelectionKey key = iter.next();
            if (key.isReadable())
            {
                handledFrames += ((IntSupplier)key.attachment()).getAsInt();
            }

            iter.remove();
        }

        return handledFrames;
    }
}
