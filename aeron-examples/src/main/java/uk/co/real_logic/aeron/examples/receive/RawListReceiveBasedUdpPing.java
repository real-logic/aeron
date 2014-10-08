package uk.co.real_logic.aeron.examples.receive;

import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;
import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;
import static uk.co.real_logic.aeron.examples.RawUdpPong.setup;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see uk.co.real_logic.aeron.examples.receive.RawReceiveBasedUdpPong
 */
public class RawListReceiveBasedUdpPing
{

    private static final int MESSAGE_SIZE = SIZE_OF_LONG + SIZE_OF_LONG;

    public static final int PONG_PORT = 40123;
    public static final int PING_PORT = 40124;

    public static void main(String[] args) throws IOException
    {
        new RawListReceiveBasedUdpPing().run();
    }

    private void run() throws IOException
    {
        final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);

        InetSocketAddress sendAddress = new InetSocketAddress("localhost", PING_PORT);

        ByteBuffer buffer = ByteBuffer.allocateDirect(MTU_LENGTH_DEFAULT);

        DatagramChannel receiveChannel = DatagramChannel.open();
        setup(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", PONG_PORT));

        List<DatagramChannel> receiveChannels = new ArrayList<>();
        receiveChannels.add(receiveChannel);

        DatagramChannel sendChannel = DatagramChannel.open();
        setup(sendChannel);

        List<DatagramChannel> sendChannels = new ArrayList<>();
        sendChannels.add(sendChannel);

        while (true)
        {
            oneIteration(histogram, sendAddress, buffer, receiveChannels, sendChannels);
        }
    }

    private void oneIteration(
            final Histogram histogram, final InetSocketAddress sendAddress, final ByteBuffer buffer,
            final List<DatagramChannel> receiveChannels, final List<DatagramChannel> sendChannels)
        throws IOException
    {
        for (int sequenceNumber = 0; sequenceNumber < 10_000; sequenceNumber++)
        {
            long timestamp = System.nanoTime();

            buffer.clear();
            buffer.putLong(sequenceNumber);
            buffer.putLong(timestamp);
            buffer.flip();

            for (int i = 0; i < sendChannels.size(); i++)
            {
                int sent = sendChannels.get(i).send(buffer, sendAddress);
                validateDataAmount(sent);
            }

            for (int i = 0; i < receiveChannels.size(); i++)
            {
                DatagramChannel receiveChannel = receiveChannels.get(i);
                buffer.clear();
                while (receiveChannel.receive(buffer) == null)
                {
                    ;
                }

                long receivedSequenceNumber = buffer.getLong(0);
                if (receivedSequenceNumber != sequenceNumber)
                {
                    throw new IllegalStateException("Data Loss:" + sequenceNumber + " to " + receivedSequenceNumber);
                }

                long duration = System.nanoTime() - timestamp;
                histogram.recordValue(duration);
            }
        }

        histogram.outputPercentileDistribution(System.out, 1000.0);
        histogram.reset();
    }

    private void validateDataAmount(final int amount)
    {
        if (amount != MESSAGE_SIZE)
        {
            throw new IllegalStateException("Should have sent: " + MESSAGE_SIZE + " but actually sent: " + amount);
        }
    }

}
