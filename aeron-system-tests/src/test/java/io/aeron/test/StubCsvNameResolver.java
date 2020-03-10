package io.aeron.test;

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
