package uk.co.real_logic.aeron.common;

import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

interface NetworkInterfaceShim
{
    Enumeration<NetworkInterface> getNetworkInterfaces() throws SocketException;
    List<InterfaceAddress> getInterfaceAddresses(NetworkInterface ifc);
    boolean isLoopback(NetworkInterface ifc) throws SocketException;

    NetworkInterfaceShim DEFAULT = new NetworkInterfaceShim()
    {
        @Override
        public Enumeration<NetworkInterface> getNetworkInterfaces() throws SocketException
        {
            return NetworkInterface.getNetworkInterfaces();
        }

        @Override
        public List<InterfaceAddress> getInterfaceAddresses(NetworkInterface ifc)
        {
            return ifc.getInterfaceAddresses();
        }

        @Override
        public boolean isLoopback(NetworkInterface ifc) throws SocketException
        {
            return ifc.isLoopback();
        }
    };

}
