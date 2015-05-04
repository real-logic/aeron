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

import java.util.concurrent.atomic.AtomicBoolean;

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

public class AeronPong
{
    private Aeron.Context ctx = null;
    private FragmentAssemblyAdapter dataHandler = null;
    private Aeron aeron = null;
    private Publication pongPub = null;
    private Subscription pingSub = null;
    private final int pingStreamId = 10;
    private final int pongStreamId = 11;
    private String pingChannel = "udp://localhost:44444";
    private String pongChannel = "udp://localhost:55555";
    private final AtomicBoolean running = new AtomicBoolean(true);
    private boolean claim = false;
    private BufferClaim bufferClaim = null;
    private Options options;

    public AeronPong(final String[] args)
    {
        try
        {
            parseArgs(args);
        }
        catch (final Exception e)
        {
            e.printStackTrace();
        }

        ctx = new Aeron.Context();
        if (claim)
        {
            dataHandler = new FragmentAssemblyAdapter(this::pingHandlerClaim);
        }
        else
        {
            dataHandler = new FragmentAssemblyAdapter(this::pingHandler);
        }
        aeron = Aeron.connect(ctx);
        pongPub = aeron.addPublication(pongChannel, pongStreamId);
        pingSub = aeron.addSubscription(pingChannel, pingStreamId, dataHandler);
        this.claim = claim;
        if (claim)
        {
            bufferClaim = new BufferClaim();
        }
    }

    public void run()
    {
        while (running.get())
        {
            pingSub.poll(1);
        }
    }

    public void shutdown()
    {
        aeron.close();
    }

    private void parseArgs(final String[] args) throws ParseException
    {
        options = new Options();
        options.addOption("c", "claim", false, "Use Try/Claim");
        options.addOption("", "pongChannel", false, "Pong channel");
        options.addOption("", "pingChannel", false, "Ping channel");

        final CommandLineParser parser = new GnuParser();
        final CommandLine command = parser.parse(options, args);

        if (command.hasOption("claim"))
        {
            claim = true;
        }
        else
        {
            claim = false;
        }

        if (command.hasOption("pingChannel"))
        {
            pingChannel = command.getOptionValue("pingChannel", "udp://localhost:44444");
        }

        if (command.hasOption("pongChannel"))
        {
            pongChannel = command.getOptionValue("pongChannel", "udp://localhost:55555");
        }
    }

    private void pingHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (buffer.getByte(offset + 0) == (byte)'q')
        {
            running.set(false);
            return;
        }
        while (pongPub.offer(buffer, offset, length) < 0L)
        {
        }
    }

    private void pingHandlerClaim(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        if (buffer.getByte(offset + 0) == (byte)'q')
        {
            running.set(false);
            return;
        }
        if (pongPub.tryClaim(length, bufferClaim) >= 0)
        {
            try
            {
                final MutableDirectBuffer newBuffer = bufferClaim.buffer();
                newBuffer.putBytes(bufferClaim.offset(), buffer, offset, length);
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
        else
        {
            pingHandlerClaim(buffer, offset, length, header);
        }
    }

    public static void main(final String[] args)
    {
        final AeronPong pong = new AeronPong(args);
        pong.run();
        pong.shutdown();
    }
}
