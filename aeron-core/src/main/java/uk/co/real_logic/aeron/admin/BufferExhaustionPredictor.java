/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.admin;

import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * Predicts when buffer exhaustion is going to happen for a given
 * Term, so new term buffers can be mapped.
 */
public class BufferExhaustionPredictor
{
    // TODO: parameterize and adaptively tune this
    private static final long minOkWindowToExhaustionInNanos = 100;

    private static final int DATA_COLLECTION_WINDOW = 10;

    private long bufferSize;
    private List<DataWriteRate> writeRates;
    private long bufferConsumed;

    private int writeCursor;

    public BufferExhaustionPredictor()
    {
    }

    public void onDataWritten(final long amountInBytes, final long nanoTime)
    {
        bufferConsumed += amountInBytes;

        DataWriteRate writeRate = writeRates.get(writeCursor);
        writeRate.nanoTime(nanoTime);
        writeRate.amountInBytes(amountInBytes);
        writeCursor = next(writeCursor);
    }

    public void reset(final long bufferSize) {
        this.bufferSize = bufferSize;
        bufferConsumed = 0;

        writeCursor = 0;
        writeRates = IntStream.range(0, DATA_COLLECTION_WINDOW)
                              .mapToObj(i -> new DataWriteRate())
                              .collect(toList());
    }

    /**
     * @return true if its time to request a new term buffer
     */
    public boolean predictExhaustion(final long currentTime)
    {
        long period = currentTime - startTime();

        int readCursor = writeCursor;
        long dataWriteTally = 0;
        do
        {
            dataWriteTally += writeRates.get(readCursor).amountInBytes();

            readCursor = next(readCursor);
        }
        while (readCursor != writeCursor);

        double bytesWrittenPerNano = (double) dataWriteTally / period;
        double remainingBytesInBuffer = bufferSize - bufferConsumed;
        double nanosBeforeExhaustion = remainingBytesInBuffer / bytesWrittenPerNano;

        return nanosBeforeExhaustion < minOkWindowToExhaustionInNanos;
    }

    private long startTime()
    {
        long time = writeRates.get(writeCursor).nanoTime();

        if (time == 0)
        {
            return writeRates.get(0).nanoTime();
        }

        return time;
    }

    private int next(final int cursor)
    {
        return (cursor + 1) % DATA_COLLECTION_WINDOW;
    }

}
