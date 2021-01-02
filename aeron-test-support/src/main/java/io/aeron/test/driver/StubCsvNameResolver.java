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
package io.aeron.test.driver;

import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.NameResolver;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

public class StubCsvNameResolver implements NameResolver
{
    private final List<String[]> lookupNames = new ArrayList<>();

    public StubCsvNameResolver(final String stubLookupConfiguration)
    {
        final String[] lines = stubLookupConfiguration.split("\\|");
        for (final String line : lines)
        {
            final String[] params = line.split(",");
            if (4 != params.length)
            {
                throw new IllegalArgumentException("Expect 4 elements per row");
            }

            lookupNames.add(params);
        }
    }

    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
    {
        return DefaultNameResolver.INSTANCE.resolve(name, uriParamName, isReResolution);
    }

    public String lookup(final String name, final String uriParamName, final boolean isReLookup)
    {
        for (final String[] lookupName : lookupNames)
        {
            if (lookupName[1].equals(uriParamName) && lookupName[0].equals(name))
            {
                return isReLookup ? lookupName[2] : lookupName[3];
            }
        }

        return name;
    }
}
