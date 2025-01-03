/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.samples;

import io.aeron.driver.reports.LossReportReader;
import io.aeron.driver.reports.LossReportUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.io.File;
import java.nio.MappedByteBuffer;

import static io.aeron.CommonContext.AERON_DIR_PROP_DEFAULT;
import static io.aeron.CommonContext.AERON_DIR_PROP_NAME;
import static java.lang.System.getProperty;

/**
 * Application that prints a report of loss observed by stream to {@link System#out}.
 */
public class LossStat
{
    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        final String aeronDirectoryName = getProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT);
        final File lossReportFile = LossReportUtil.file(aeronDirectoryName);

        if (!lossReportFile.exists())
        {
            System.err.print("Loss report does not exist: " + lossReportFile);
            System.exit(1);
        }

        final MappedByteBuffer mappedByteBuffer = SamplesUtil.mapExistingFileReadOnly(lossReportFile);
        final AtomicBuffer buffer = new UnsafeBuffer(mappedByteBuffer);

        System.out.println(LossReportReader.LOSS_REPORT_CSV_HEADER);
        final int entriesRead = LossReportReader.read(buffer, LossReportReader.defaultEntryConsumer(System.out));
        System.out.println(entriesRead + " loss entries");
    }
}
