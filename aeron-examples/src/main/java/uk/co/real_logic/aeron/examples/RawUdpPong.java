package uk.co.real_logic.aeron.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;

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
        receiveChannel.register(selector, OP_READ, this);


        while (true)
        {
            while (selector.selectNow() == 0)
            {
                ;
            }

            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext())
            {
                it.next();

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

                it.remove();
            }
        }
    }

    public static void setup(final DatagramChannel channel) throws IOException
    {
        channel.configureBlocking(false);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    }

    private void validateDataAmount(final int amount)
    {
        if (amount != MESSAGE_SIZE)
        {
            throw new IllegalStateException();
        }
    }

}
