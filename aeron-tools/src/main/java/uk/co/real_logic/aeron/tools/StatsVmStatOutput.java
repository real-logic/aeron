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
package uk.co.real_logic.aeron.tools;

public class StatsVmStatOutput implements StatsOutput
{
    private final String titles[] =
        {
            "Bytes", "Failed Offers", "NAKs", "SMs", "Heartbeats", "RX", "Flow Control", "Invalid", "Driver",
            "Short Sends", "Keep", "FC Limits"
        };
    private final String subTitles[] =
        {
            "In/Out", "RP/SP/DCP", "In/Out", "In/Out", "In/Out", "Sent", "Under/Over", "Packets", "Exceptions",
            "DP/StatM/SM/NM", "Alives", "Applied"
        };
    private final String formats[] =
        {
            "%1$-18s", "%1$-18s", "%1$-10s", "%1$-14s", "%1$-12s", "%1$-6s", "%1$-14s", "%1$-9s",
            "%1$-12s", "%1$-18s", "%1$-7s", "%1$-7s"
        };
    private int iterations;

    public StatsVmStatOutput()
    {
        iterations = 0;
    }

    public void format(final String[] keys, final long[] vals) throws Exception
    {
        if (iterations % 20 == 0)
        {
            for (int i = 0; i < titles.length; i++)
            {
                System.out.format(formats[i], titles[i]);
            }
            System.out.println();
            for (int i = 0; i < titles.length; i++)
            {
                System.out.format(formats[i], subTitles[i]);
            }
            System.out.println();
        }
        System.out.format(formats[0], humanReadableByteCount(vals[1], false) + "/" + humanReadableByteCount(vals[0], false));
        System.out.format(formats[1], humanReadableCount(vals[2], true) + "/" +
            humanReadableCount(vals[3], true) + "/" + humanReadableCount(vals[4], true));
        System.out.format(formats[2], humanReadableCount(vals[6], true) + "/" + humanReadableCount(vals[5], true));
        System.out.format(formats[3], humanReadableCount(vals[8], true) + "/" + humanReadableCount(vals[7], true));
        System.out.format(formats[4], humanReadableCount(vals[9], true) + "/" + humanReadableCount(vals[10], true));
        System.out.format(formats[5], humanReadableCount(vals[11], true));
        System.out.format(formats[6], humanReadableCount(vals[12], true) + "/" + humanReadableCount(vals[13], true));
        System.out.format(formats[7], humanReadableCount(vals[14], true));
        System.out.format(formats[8], humanReadableCount(vals[15], true));
        System.out.format(formats[9], humanReadableCount(vals[16], true) + "/" +
            humanReadableCount(vals[17], true) + "/" + humanReadableCount(vals[18], true) + "/" +
            humanReadableCount(vals[19], true));
        System.out.format(formats[10], humanReadableCount(vals[20], true));
        System.out.format(formats[11], humanReadableCount(vals[21], true));
        System.out.println();
        iterations++;
    }

    public void close() throws Exception
    {
    }

    private String humanReadableByteCount(final long bytes, final boolean si)
    {
        final int unit = si ? 1000 : 1024;
        if (bytes < unit)
        {
            return bytes + "B";
        }
        final int exp = (int)(Math.log(bytes) / Math.log(unit));
        final String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");

        return String.format("%.1f%sB", bytes / Math.pow(unit, exp), pre);
    }

    private String humanReadableCount(final long val, final boolean si)
    {
        final int unit = si ? 1000 : 1024;
        if (val < unit)
        {
            return val + "";
        }
        final int exp = (int)(Math.log(val) / Math.log(unit));
        final String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");

        return String.format("%.1f%s", val / Math.pow(unit, exp), pre);
    }
}
