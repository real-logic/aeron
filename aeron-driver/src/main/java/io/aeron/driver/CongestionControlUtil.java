/*
 * Copyright 2014-2018 Real Logic Ltd.
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
package io.aeron.driver;

public class CongestionControlUtil
{
    private static final int FORCE_STATUS_MESSAGE_BIT = 0x1;

    public static long packOutcome(final int receiverWindowLength, final boolean forceStatusMessage)
    {
        final int flags = forceStatusMessage ? FORCE_STATUS_MESSAGE_BIT : 0x0;

        return ((long)flags << 32) | receiverWindowLength;
    }

    public static int receiverWindowLength(final long outcome)
    {
        return (int)outcome;
    }

    public static boolean shouldForceStatusMessage(final long outcome)
    {
        return ((int)(outcome >>> 32) & FORCE_STATUS_MESSAGE_BIT) == FORCE_STATUS_MESSAGE_BIT;
    }

    public static long positionThreshold(final long position)
    {
        return position / 4;
    }
}
