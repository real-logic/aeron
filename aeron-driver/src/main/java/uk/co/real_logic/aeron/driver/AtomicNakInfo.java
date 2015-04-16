/*
 * Copyright 2015 Real Logic Ltd.
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

package uk.co.real_logic.aeron.driver;

/**
 * Holder of NAK information that allows atomic exchange
 */
public class AtomicNakInfo
{
    private volatile long beginChange = -1;
    private volatile long endChange = -1;

    private int lossTermId;
    private int lossTermOffset;
    private int lossLength;

    private long lastChangeNumber = -1;

    public void info(final int termId, final int termOffset, final int length)
    {
        final long changeNumber = beginChange + 1;

        beginChange = changeNumber;

        lossTermId = termId;
        lossTermOffset = termOffset;
        lossLength = length;

        endChange = changeNumber;
    }

    public int sendPending(final NakMessageSender handler)
    {
        final long changeNumber = endChange;
        int workCount = 0;

        if (changeNumber != lastChangeNumber)
        {
            final int termId = lossTermId;
            final int termOffset = lossTermOffset;
            final int length = lossLength;

            if (changeNumber == beginChange)
            {
                handler.send(termId, termOffset, length);
                lastChangeNumber = changeNumber;
                workCount = 1;
            }
        }

        return workCount;
    }
}
