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
    // TODO: move to Configuration
    public static final String DISABLE_KEYSET_OPTIMIZATION_PROP_NAME = "aeron.disable.nio.keyset.optimization";
    public static final boolean DISABLE_KEYSET_OPTIMIZATION = Boolean.getBoolean(DISABLE_KEYSET_OPTIMIZATION_PROP_NAME);

    private final IntSupplier handleSelectedKeysFunc;
    private final Selector selector;
    private final NioSelectedKeySet selectedKeySet;

    public NioSelector()
    {
        NioSelectedKeySet tmpSet = null;
        IntSupplier selectedKeysFunc = this::handleSelectedKeys;

        try
        {
            this.selector = Selector.open(); // yes, SelectorProvider, blah, blah
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
        }

        /*
         * netty's way of optimizing the terrible NIO HashSet handling for selected keys
         */
        if (!DISABLE_KEYSET_OPTIMIZATION)
        {
            try
            {
                final Class<?> selectorImplClass =
                    Class.forName("sun.nio.ch.SelectorImpl", false, ClassLoader.getSystemClassLoader());

                if (selectorImplClass.isAssignableFrom(selector.getClass()))
                {
                    tmpSet = new NioSelectedKeySet();

                    final Field selectedKeysField = selectorImplClass.getDeclaredField("selectedKeys");
                    final Field publicSelectedKeysField = selectorImplClass.getDeclaredField("publicSelectedKeys");

                    selectedKeysField.setAccessible(true);
                    publicSelectedKeysField.setAccessible(true);

                    selectedKeysField.set(selector, tmpSet);
                    publicSelectedKeysField.set(selector, tmpSet);

                    selectedKeysFunc = this::handleSelectedKeysOptimized;
                }
            }
            catch (final Exception ex)
            {
                tmpSet = null;
            }
        }

        selectedKeySet = tmpSet;
        handleSelectedKeysFunc = selectedKeysFunc;
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
     * Explicit event loop processing as poll
     */
    public int processKeys()
    {
        try
        {
            int handledFrames = 0;
            if (selector.selectNow() > 0)
            {
                handledFrames = handleSelectedKeysFunc.getAsInt();
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

    private static int handleKey(final SelectionKey key)
    {
        int value = 0;

        if (key.isReadable())
        {
            value = ((IntSupplier)key.attachment()).getAsInt();
        }

        return value;
    }

    private int handleSelectedKeysOptimized()
    {
        return selectedKeySet.forEach(NioSelector::handleKey);
    }
}
