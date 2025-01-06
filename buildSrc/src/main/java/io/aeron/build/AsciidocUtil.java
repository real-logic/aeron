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
package io.aeron.build;

import java.io.File;
import java.util.List;

import static java.util.Arrays.asList;

final class AsciidocUtil
{
    private static final List<String> EXTENSIONS = asList(".adoc", ".asciidoc");

    private AsciidocUtil()
    {
    }

    public static File[] filterAsciidocFiles(final File directory)
    {
        final File[] asciidocFiles = directory.listFiles(
            (dir, name) ->
            {
                for (final String extension : EXTENSIONS)
                {
                    if (name.endsWith(extension))
                    {
                        return true;
                    }
                }
                return false;
            });

        return null != asciidocFiles ? asciidocFiles : new File[0];
    }
}
