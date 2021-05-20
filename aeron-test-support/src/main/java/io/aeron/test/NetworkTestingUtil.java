package io.aeron.test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

public class NetworkTestingUtil
{
    /**
     * Return an error message if this address can't be bound, null if successful.
     * @param address The address to attempt to bind to.
     * @return null if successful, error message otherwise.
     */
    public static String isBindAddressAvailable(String address)
    {
        try (ServerSocket ignored = new ServerSocket(0, 1024, InetAddress.getByName(address)))
        {
            return null;
        }
        catch (Exception e)
        {
            return "Binding to " + address + " failed, error: " + e.getMessage();
        }
    }
}
