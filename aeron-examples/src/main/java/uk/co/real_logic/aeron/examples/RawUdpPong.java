package uk.co.real_logic.aeron.examples;

import uk.co.real_logic.aeron.driver.NioSelectedKeySet;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

import static java.nio.channels.SelectionKey.OP_READ;
import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;
import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see RawUdpPing
 */
public class RawUdpPong
{

    private static final int MESSAGE_SIZE = SIZE_OF_LONG + SIZE_OF_LONG;

    public static final int PONG_PORT = 40123;
    public static final int PING_PORT = 40124;

    public static void main(String[] args) throws IOException
    {
        new RawUdpPong().run();
    }

    private void run() throws IOException
    {
        InetSocketAddress sendAddress = new InetSocketAddress("localhost", PONG_PORT);

        ByteBuffer buffer = ByteBuffer.allocateDirect(MTU_LENGTH_DEFAULT);

        DatagramChannel receiveChannel = DatagramChannel.open();
        setup(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", PING_PORT));

        DatagramChannel sendChannel = DatagramChannel.open();
        setup(sendChannel);

        Selector selector = Selector.open();
        NioSelectedKeySet keySet = keySet(selector);

        receiveChannel.register(selector, OP_READ, this);

        ToIntFunction<SelectionKey> handler = key ->
        {
            try
            {
                buffer.clear();
                receiveChannel.receive(buffer);

                long receivedSequenceNumber = buffer.getLong(0);
                long receivedTimestamp = buffer.getLong(SIZE_OF_LONG);

                buffer.clear();
                buffer.putLong(receivedSequenceNumber);
                buffer.putLong(receivedTimestamp);
                buffer.flip();

                int sent = sendChannel.send(buffer, sendAddress);
                validateDataAmount(sent);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }

            return 1;
        };

        while (true)
        {
            while (selector.selectNow() == 0)
            {
                ;
            }

            keySet.forEach(handler);
        }
    }

    public static void setup(final DatagramChannel channel) throws IOException
    {
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    }

    public static void validateDataAmount(final int amount)
    {
        if (amount != MESSAGE_SIZE)
        {
            throw new IllegalStateException();
        }
    }

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

    public static NioSelectedKeySet keySet(final Selector selector)
    {
        NioSelectedKeySet tmpSet = null;

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

        return tmpSet;
    }

}
