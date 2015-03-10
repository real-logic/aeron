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

import uk.co.real_logic.aeron.common.CncFileDescriptor;
import uk.co.real_logic.aeron.common.CommonContext;
import uk.co.real_logic.aeron.common.concurrent.SigInt;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.CountersManager;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * App to print out status counters and labels
 */
public class AeronStat
{
    public static void main(final String[] args) throws Exception
    {
        SamplesUtil.useSharedMemoryOnLinux();

        final File cncFile = CommonContext.newDefaultCncFile();

        System.out.println("Command `n Control file " + cncFile);

        final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");

        final DirectBuffer metaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);

        final int cncVersion = metaDataBuffer.getInt(CncFileDescriptor.cncVersionOffset(0));
        if (CncFileDescriptor.CNC_VERSION != cncVersion)
        {
            throw new IllegalStateException("CNC version not understood: version=" + cncVersion);
        }

        final AtomicBuffer labelsBuffer = CncFileDescriptor.createCounterLabelsBuffer(cncByteBuffer, metaDataBuffer);
        final AtomicBuffer valuesBuffer = CncFileDescriptor.createCounterValuesBuffer(cncByteBuffer, metaDataBuffer);

        final CountersManager countersManager = new CountersManager(labelsBuffer, valuesBuffer);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (running.get())
        {
            System.out.print("\033[H\033[2J");
            System.out.format("%1$tH:%1$tM:%1$tS - Aeron Stat\n", new Date());
            System.out.println("=========================");

            countersManager.forEach(
                (id, label) ->
                {
                    final int offset = CountersManager.counterOffset(id);
                    final long value = valuesBuffer.getLongVolatile(offset);

                    System.out.format("%3d: %,20d - %s\n", id, value, label);
                });

            Thread.sleep(1000);
        }
    }
}
