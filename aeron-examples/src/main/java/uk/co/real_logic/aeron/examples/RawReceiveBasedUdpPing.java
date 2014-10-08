package uk.co.real_logic.aeron.examples;

import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.TimeUnit;

import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;
import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;
import static uk.co.real_logic.aeron.examples.RawUdpPong.setup;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see RawReceiveBasedUdpPong
 */
public class RawReceiveBasedUdpPing
{

    private static final int MESSAGE_SIZE = SIZE_OF_LONG + SIZE_OF_LONG;

    public static final int PONG_PORT = 40123;
    public static final int PING_PORT = 40124;

    public static void main(String[] args) throws IOException
    {
        new RawReceiveBasedUdpPing().run();
    }

    private void run() throws IOException
    {
        final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);

        InetSocketAddress sendAddress = new InetSocketAddress("localhost", PING_PORT);

        ByteBuffer buffer = ByteBuffer.allocateDirect(MTU_LENGTH_DEFAULT);

        DatagramChannel receiveChannel = DatagramChannel.open();
        setup(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", PONG_PORT));

        DatagramChannel sendChannel = DatagramChannel.open();
        setup(sendChannel);

        while (true)
        {
            oneIteration(histogram, sendAddress, buffer, receiveChannel, sendChannel);
        }
    }

    private void oneIteration(
            final Histogram histogram, final InetSocketAddress sendAddress, final ByteBuffer buffer,
            final DatagramChannel receiveChannel, final DatagramChannel sendChannel)
        throws IOException
    {
        for (int sequenceNumber = 0; sequenceNumber < 10_000; sequenceNumber++)
        {
            long timestamp = System.nanoTime();

            buffer.clear();
            buffer.putLong(sequenceNumber);
            buffer.putLong(timestamp);
            buffer.flip();

            int sent = sendChannel.send(buffer, sendAddress);
            validateDataAmount(sent);


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
