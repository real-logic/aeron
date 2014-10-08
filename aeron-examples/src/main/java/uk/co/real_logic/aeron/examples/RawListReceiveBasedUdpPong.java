package uk.co.real_logic.aeron.examples;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;

import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;
import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see RawReceiveBasedUdpPing
 */
public class RawListReceiveBasedUdpPong
{

    private static final int MESSAGE_SIZE = SIZE_OF_LONG + SIZE_OF_LONG;

    public static final int PONG_PORT = 40123;
    public static final int PING_PORT = 40124;

    public static void main(String[] args) throws IOException
    {
        new RawListReceiveBasedUdpPong().run();
    }

    private void run() throws IOException
    {
        InetSocketAddress sendAddress = new InetSocketAddress("localhost", PONG_PORT);

        ByteBuffer buffer = ByteBuffer.allocateDirect(MTU_LENGTH_DEFAULT);

        DatagramChannel receiveChannel = DatagramChannel.open();
        init(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", PING_PORT));

        List<DatagramChannel> receiveChannels = new ArrayList<>();
        receiveChannels.add(receiveChannel);

        DatagramChannel sendChannel = DatagramChannel.open();
        init(sendChannel);

        List<DatagramChannel> sendChannels = new ArrayList<>();
        sendChannels.add(sendChannel);

        while (true)
        {
            buffer.clear();
            for (int i = 0; i < receiveChannels.size(); i++)
            {
                DatagramChannel receiveChannelDereferenced = receiveChannels.get(i);

                buffer.clear();
                while (receiveChannelDereferenced.receive(buffer) == null)
                {
                    ;
                }

                long receivedSequenceNumber = buffer.getLong(0);
                long receivedTimestamp = buffer.getLong(SIZE_OF_LONG);

                buffer.clear();
                buffer.putLong(receivedSequenceNumber);
                buffer.putLong(receivedTimestamp);
                buffer.flip();

                for (int j = 0; j < sendChannels.size(); j++)
                {
                    int sent = sendChannels.get(j).send(buffer, sendAddress);
                    validateDataAmount(sent);
                }
            }
        }
    }

    public static void init(final DatagramChannel channel) throws IOException
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
