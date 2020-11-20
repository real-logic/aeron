package io.aeron.cluster;

import io.aeron.driver.DefaultNameResolver;
import io.aeron.driver.NameResolver;
import java.net.InetAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TestNameResolver implements NameResolver
{

    private static final String NULL_ADDRESS = "localhost:7";

    private final ConcurrentMap<String, String> addressByMember = new ConcurrentHashMap<>();

    public void populate(final String name, final String address)
    {
        addressByMember.put(name, address);
    }

    @Override
    public InetAddress resolve(final String name, final String uriParams, final boolean isReResolve)
    {
        return DefaultNameResolver.INSTANCE.resolve(name, uriParams, isReResolve);
    }

    @Override
    public String lookup(final String name, final String uriParamName, final boolean isReLookup)
    {
        final String[] split = name.split(":");
        final String key = split[0];
        final int port = Integer.parseInt(split[1]);

        final String address = addressByMember.get(key);

        if (address == null)
        {
            // return name; -> fail immediately by java.net.UnknownHostException:
            //   Unresolved - endpoint=local-1:20221, name-and-port=local-1:20221
            return NULL_ADDRESS;
        }

        return address + ":" + port;
    }
}

