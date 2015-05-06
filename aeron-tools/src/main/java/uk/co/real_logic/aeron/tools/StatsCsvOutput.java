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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class StatsCsvOutput implements StatsOutput
{
    public static final String DEFAULT_FILE = "stats.csv";

    private String file;
    private FileWriter out;
    private boolean firstTime = true;

    public StatsCsvOutput(final String file)
    {
        if (file != null)
        {
            this.file = file;
        }
        else
        {
            this.file = DEFAULT_FILE;
        }

        try
        {
            System.out.println("Output file: " + this.file);
            final File outFile = new File(this.file);
            outFile.createNewFile();
            out = new FileWriter(outFile);
        }
        catch (final IOException e)
        {
            e.printStackTrace();
        }
    }

    public void format(final String[] keys, final long[] vals) throws Exception
    {
        if (firstTime)
        {
            for (int i = 0; i < keys.length - 1; i++)
            {
                out.write(keys[i] + ",");
            }
            out.write(keys[keys.length - 1] + "\n");
            firstTime = false;
        }

        for (int i = 0; i < vals.length - 1; i++)
        {
            out.write(vals[i] + ",");
        }
        out.write(vals[vals.length - 1] + "\n");
        out.flush();
    }

    public void close() throws Exception
    {
        out.close();
    }
}
