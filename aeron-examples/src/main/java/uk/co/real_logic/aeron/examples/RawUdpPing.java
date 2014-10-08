package uk.co.real_logic.aeron.examples;

import org.HdrHistogram.Histogram;
import uk.co.real_logic.aeron.driver.NioSelectedKeySet;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import static java.nio.channels.SelectionKey.OP_READ;
import static uk.co.real_logic.aeron.common.BitUtil.SIZE_OF_LONG;
import static uk.co.real_logic.aeron.driver.Configuration.MTU_LENGTH_DEFAULT;
import static uk.co.real_logic.aeron.examples.RawUdpPong.setup;

/**
 * Benchmark used to calculate latency of underlying system.
 *
 * @see RawUdpPong
 */
public class RawUdpPing implements ToIntFunction<SelectionKey>
{

    private static final int MESSAGE_SIZE = SIZE_OF_LONG + SIZE_OF_LONG;

    public static final int PONG_PORT = 40123;
    public static final int PING_PORT = 40124;
    private static final InetSocketAddress sendAddress = new InetSocketAddress("localhost", PING_PORT);


    public static void main(String[] args) throws IOException
    {
        new RawUdpPing().run();
    }

    Histogram histogram;
    ByteBuffer buffer = ByteBuffer.allocateDirect(MTU_LENGTH_DEFAULT);
    DatagramChannel receiveChannel;
    private int sequenceNumber;

    private void run() throws IOException
    {
        histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 3);

        receiveChannel = DatagramChannel.open();
        setup(receiveChannel);
        receiveChannel.bind(new InetSocketAddress("localhost", PONG_PORT));

        DatagramChannel sendChannel = DatagramChannel.open();
        setup(sendChannel);

        Selector selector = Selector.open();
        receiveChannel.register(selector, OP_READ, this);
        NioSelectedKeySet keySet = RawUdpPong.keySet(selector);

        while (true)
        {
            oneIteration(histogram, sendAddress, buffer, sendChannel, selector, keySet);
        }
    }

    public int applyAsInt(SelectionKey key)
    {
        try
        {
            buffer.clear();
            receiveChannel.receive(buffer);

            long receivedSequenceNumber = buffer.getLong(0);
            long timestamp = buffer.getLong(SIZE_OF_LONG);

            if (receivedSequenceNumber != sequenceNumber)
            {
                throw new IllegalStateException("Data Loss:" + sequenceNumber + " to " + receivedSequenceNumber);
            }

            long duration = System.nanoTime() - timestamp;
            histogram.recordValue(duration);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return 1;
    }

    private void oneIteration(
            final Histogram histogram, final InetSocketAddress sendAddress, final ByteBuffer buffer,
            final DatagramChannel sendChannel, final Selector selector, final NioSelectedKeySet keySet)
            throws IOException
    {
        for (sequenceNumber = 0; sequenceNumber < 10_000; sequenceNumber++)
        {
            long timestamp = System.nanoTime();

            buffer.clear();
            buffer.putLong(sequenceNumber);
            buffer.putLong(timestamp);
            buffer.flip();

            int sent = sendChannel.send(buffer, sendAddress);
            RawUdpPong.validateDataAmount(sent);

            while (selector.selectNow() == 0)
            {
                ;
            }

            keySet.forEach(this);
        }

        histogram.outputPercentileDistribution(System.out, 1000.0);
        histogram.reset();
    }

}
