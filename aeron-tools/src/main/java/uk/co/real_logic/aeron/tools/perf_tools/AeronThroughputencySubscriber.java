package uk.co.real_logic.aeron.tools.perf_tools;

import org.apache.commons.cli.*;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Created by philipjohnson1 on 4/2/15.
 */
public class AeronThroughputencySubscriber
{
    private Aeron.Context ctx = null;
    private FragmentAssemblyAdapter dataHandler = null;
    private Aeron aeron = null;
    private Publication pub = null;
    private Subscription sub = null;
    private final int pubStreamId = 11;
    private final int subStreamId = 10;
    private String pubChannel = "udp://localhost:44444";
    private String reflectChannel = "udp://localhost:55555";
    private boolean running = true;
    private BufferClaim bufferClaim = null;
    private Options options;

    public AeronThroughputencySubscriber(String[] args)
    {
        try
        {
            parseArgs(args);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        ctx = new Aeron.Context();
        dataHandler = new FragmentAssemblyAdapter(this::msgHandler);
        aeron = Aeron.connect(ctx);
        pub = aeron.addPublication(pubChannel, pubStreamId);
        sub = aeron.addSubscription(reflectChannel, subStreamId, dataHandler);
        bufferClaim = new BufferClaim();

        while (running)
        {
            sub.poll(1);
        }


        sub.close();
        pub.close();
        ctx.close();
        aeron.close();
    }

    public void msgHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        int iterations = 0;
        if (buffer.getByte(offset) == (byte)'q')
        {
            running = false;
            return;
        }
        else
        {
            while (pub.tryClaim(length, bufferClaim) < 0L)
            {
                iterations++;
            }
            if (iterations > 10)
            {
                System.out.println("Took too many tries: " + iterations);
            }
            try
            {
                final MutableDirectBuffer newBuffer = bufferClaim.buffer();
                final int newOffset = bufferClaim.offset();
                 newBuffer.putBytes(newOffset, buffer, offset, length);
            }
            catch (final Exception e)
            {
                e.printStackTrace();
            }
            finally
            {
                bufferClaim.commit();
            }
        }
    }

    private void parseArgs(final String[] args) throws ParseException
    {
        options = new Options();
        options.addOption("c", "claim", false, "Use Try/Claim");
        options.addOption("", "pubChannel", false, "Primary publishing channel");
        options.addOption("", "reflectChannel", false, "Reflection channel");

        final CommandLineParser parser = new GnuParser();
        final CommandLine command = parser.parse(options, args);


        if (command.hasOption("pubChannel"))
        {
            pubChannel = command.getOptionValue("pubChannel", "udp://localhost:44444");
        }

        if (command.hasOption("reflectChannel"))
        {
            reflectChannel = command.getOptionValue("reflecthannel", "udp://localhost:55555");
        }
    }

    public static void main(final String[] args)
    {
        new AeronThroughputencySubscriber(args);
    }
}
