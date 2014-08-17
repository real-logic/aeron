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
package uk.co.real_logic.aeron.common.concurrent;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;

/**
 * Barrier to block the calling thread until a command is given on the provided {@link InputStream}.
 */
public class CommandBarrier
{
    final String label;
    final InputStream in;
    final PrintStream out;

    /**
     * Create a barrier that will display the provided label and interact via the provided streams.
     *
     * @param label to prompt the user.
     * @param in from which commands will be read.
     * @param out to which the prompt will be written.
     */
    public CommandBarrier(final String label, final InputStream in, final PrintStream out)
    {
        this.label = label;
        this.in = in;
        this.out = out;
    }

    /**
     * Await for input that matches the provided command.
     *
     * @param cmd to be awaited from the input stream.
     */
    public void await(final String cmd)
    {
        try (final BufferedReader reader = new BufferedReader(new InputStreamReader(in)))
        {
            while (true)
            {
                out.printf("\n%s : ", label).flush();

                final String line = reader.readLine();
                if (cmd.equalsIgnoreCase(line))
                {
                    break;
                }
            }
        }
        catch (final Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
