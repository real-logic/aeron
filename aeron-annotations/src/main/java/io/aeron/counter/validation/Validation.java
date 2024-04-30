/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.counter.validation;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

class Validation
{
    private final String name;
    private boolean valid = false;

    private String message;

    private ByteArrayOutputStream ba_out;

    private PrintStream ps_out;

    Validation(final String name)
    {
        this.name = name;
    }

    boolean isValid()
    {
        return valid;
    }

    void close()
    {
        if (this.ps_out != null)
        {
            this.ps_out.close();
        }
    }

    void valid(final String message)
    {
        this.valid = true;
        this.message = message;
    }

    void invalid(final String message)
    {
        this.valid = false;
        this.message = message;
    }

    PrintStream out()
    {
        if (this.ps_out == null)
        {
            this.ba_out = new ByteArrayOutputStream();
            this.ps_out = new PrintStream(ba_out);
        }

        return ps_out;
    }

    void printOn(final PrintStream out)
    {
        out.println(name);
        out.println(" " + (this.valid ? "+" : "-") + " " + this.message);
        if (this.ps_out != null)
        {
            out.println(this.ba_out);
        }
    }
}
