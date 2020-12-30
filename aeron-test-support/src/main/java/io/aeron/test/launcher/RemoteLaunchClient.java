package io.aeron.test.launcher;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;

public final class RemoteLaunchClient implements AutoCloseable
{
    private final String host;
    private final int port;
    private SocketChannel clientChannel;

    private RemoteLaunchClient(final String host, final int port)
    {
        this.host = host;
        this.port = port;
    }

    public static RemoteLaunchClient connect(final String host, final int port) throws IOException
    {
        final RemoteLaunchClient remoteLaunchClient = new RemoteLaunchClient(host, port);
        remoteLaunchClient.init();
        return remoteLaunchClient;
    }

    private void init() throws IOException
    {
        clientChannel = SocketChannel.open(new InetSocketAddress(host, port));
    }

    public ReadableByteChannel execute(final String... command) throws IOException
    {
        return execute(true, command);
    }

    public SocketChannel execute(final boolean usingBlockingIo, final String... command) throws IOException
    {
        final ObjectOutputStream out = new ObjectOutputStream(Channels.newOutputStream(clientChannel));
        out.writeObject(command);
        out.flush();
        clientChannel.configureBlocking(usingBlockingIo);
        return clientChannel;
    }

    public void executeBlocking(final OutputStream out, final String... command) throws IOException
    {
        try (ReadableByteChannel commandResponse = execute(command))
        {
            final ByteBuffer buffer = ByteBuffer.allocate(4096);
            while (commandResponse.isOpen())
            {
                final int read = commandResponse.read(buffer);
                if (read < 0)
                {
                    break;
                }

                buffer.flip();
                out.write(buffer.array(), buffer.position(), buffer.limit());
                buffer.clear();
            }
        }
    }

    public void close() throws Exception
    {
        clientChannel.close();
    }
}

