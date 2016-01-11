/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
package uk.co.real_logic.aeron.samples;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

import uk.co.real_logic.aeron.CncFileDescriptor;
import uk.co.real_logic.aeron.CommonContext;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.CountersManager;
import uk.co.real_logic.agrona.concurrent.SigInt;

/**
 * Application to print out counters and their labels A command-and-control (cnc) file is maintained by media driver in shared
 * memory. This application reads the the cnc file and prints the counters. Layout of the cnc file is described in
 * {@link CncFileDescriptor}.
 */
public class AeronStat
{
    private final AtomicBuffer valuesBuffer;
    private final CountersManager countersManager;

    public AeronStat()
    {
        final File cncFile = CommonContext.newDefaultCncFile();
        System.out.println("Command `n Control file " + cncFile);

        final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");
        final DirectBuffer metaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = metaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));

        if (CncFileDescriptor.CNC_VERSION != cncVersion)
        {
            throw new IllegalStateException("CNC version not supported: version=" + cncVersion);
        }

        final AtomicBuffer labelsBuffer = CncFileDescriptor.createCounterLabelsBuffer(cncByteBuffer, metaDataBuffer);
        valuesBuffer = CncFileDescriptor.createCounterValuesBuffer(cncByteBuffer, metaDataBuffer);
        countersManager = new CountersManager(labelsBuffer, valuesBuffer);
    }

    public void output(final PrintStream out)
    {
        out.format("%1$tH:%1$tM:%1$tS - Aeron Stat\n", new Date());
        out.println("=========================");

        this.countersManager.forEach(
            (id, label) ->
            {
                final int offset = CountersManager.counterOffset(id);
                final long value = valuesBuffer.getLongVolatile(offset);
                out.format("%3d: %,20d - %s\n", id, value, label);
            });
    }

    public void encode(final ByteBuffer buffer)
    {
        buffer.putLong(System.currentTimeMillis());

        countersManager.forEach(
            (id, label) ->
            {
                buffer.putInt(id);
                buffer.putInt(label.length());
                buffer.put(label.getBytes());
                buffer.putLong(valuesBuffer.getLongVolatile(CountersManager.counterOffset(id)));
            });
    }

    public static void main(final String[] args) throws Exception
    {
        final AeronStat aeronStat = new AeronStat();
        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (running.get())
        {
            System.out.print("\033[H\033[2J");
            aeronStat.output(System.out);
            Thread.sleep(1000);
        }
    }
}
