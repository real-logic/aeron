/*
 * Copyright 2015 Kaazing Corporation
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
package uk.co.real_logic.aeron.tools.perf_tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.FragmentAssemblyAdapter;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

public class AeronLatencyUnderLoadSubscriber
{
    private final Publication pub;
    private final BufferClaim bufferClaim;
    private String pubChannel = "udp://localhost:44444";
    private String reflectChannel = "udp://localhost:55555";
    private volatile boolean running = true;

    public AeronLatencyUnderLoadSubscriber(final String[] args)
    {
        try
        {
            parseArgs(args);
        }
        catch (final ParseException ex)
        {
           throw new RuntimeException(ex);
        }

        final Aeron.Context ctx = new Aeron.Context();
        final FragmentAssemblyAdapter dataHandler = new FragmentAssemblyAdapter(this::msgHandler);
        final Aeron aeron = Aeron.connect(ctx);
        final int pubStreamId = 11;
        pub = aeron.addPublication(reflectChannel, pubStreamId);
        final int subStreamId = 10;
        final Subscription sub = aeron.addSubscription(pubChannel, subStreamId, dataHandler);
        bufferClaim = new BufferClaim();

        while (running)
        {
            sub.poll(1);
        }

        try
        {
            Thread.sleep(500);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        aeron.close();
    }

    public void msgHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (buffer.getByte(offset) == (byte)'q')
        {
            running = false;
        }
        else
        {
            while (pub.tryClaim(length, bufferClaim) < 0L)
            {
            }
            final MutableDirectBuffer newBuffer = bufferClaim.buffer();
            final int newOffset = bufferClaim.offset();
            newBuffer.putBytes(newOffset, buffer, offset, length);
            bufferClaim.commit();
        }
    }

    private void parseArgs(final String[] args) throws ParseException
    {
        final Options options = new Options();
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
        new AeronLatencyUnderLoadSubscriber(args);
    }
}
