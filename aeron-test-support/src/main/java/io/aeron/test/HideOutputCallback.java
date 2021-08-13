/*
 * Copyright 2014-2021 Real Logic Limited.
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
package io.aeron.test;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

public class HideOutputCallback implements BeforeEachCallback, AfterEachCallback
{
    private static final OutputStream NULL_OUTPUT = new OutputStream()
    {
        public void write(final int b) throws IOException
        {
            // No-op
        }
    };

    private PrintStream out;
    private PrintStream err;

    public void beforeEach(final ExtensionContext context)
    {
        out = System.out;
        err = System.err;

        System.setOut(new PrintStream(NULL_OUTPUT));
        System.setErr(new PrintStream(NULL_OUTPUT));
    }

    public void afterEach(final ExtensionContext context)
    {
        System.setOut(out);
        System.setErr(err);
    }
}
