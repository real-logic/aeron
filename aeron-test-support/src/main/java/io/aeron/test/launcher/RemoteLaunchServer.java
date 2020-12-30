package io.aeron.test.launcher;

import org.agrona.CloseHelper;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;

public class RemoteLaunchServer
{

    private final ServerSocketChannel serverChannel;
    private final Collection<Connection> connections = new ConcurrentLinkedDeque<>();

    public static void main(final String[] args) throws IOException
    {
        final String host = System.getProperty("aeron.test.launch.host", "0.0.0.0");
        final int port = Integer.getInteger("aeron.test.launch.port", 11112);

        final RemoteLaunchServer server = new RemoteLaunchServer(host, port);
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));

        server.run();
    }

    public RemoteLaunchServer(final String host, final int port) throws IOException
    {
        serverChannel = ServerSocketChannel.open();
        final InetSocketAddress local = new InetSocketAddress(host, port);
        serverChannel.bind(local);
    }

    public void run()
    {
        Thread.setDefaultUncaughtExceptionHandler((t, e) ->
        {
            System.err.println("Uncaught exception on: " + t);
            e.printStackTrace(System.err);
        });

        try
        {
            while (!Thread.currentThread().isInterrupted())
            {
                final SocketChannel connectionChannel = serverChannel.accept();
                final Connection connection = new Connection(connectionChannel);
                connection.start();
                connections.add(connection);
            }
        }
        catch (final AsynchronousCloseException e)
        {
            // Normal close
        }
        catch (final IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void close()
    {
        try
        {
            serverChannel.close();
        }
        catch (final IOException e)
        {
            e.printStackTrace();
        }

        connections.forEach(Connection::stop);
    }

    static class Connection
    {
        private final SocketChannel connectionChannel;
        private final AtomicReference<State> currentState = new AtomicReference<>(State.CREATED);
        private volatile ProcessResponseReader responseReader;
        private volatile Thread requestThread;
        private volatile Thread responseThread;
        private Process process = null;

        private enum State
        {
            CREATED,
            STARTING,
            PENDING,
            RUNNING,
            CLOSING
        }

        Connection(final SocketChannel connectionChannel)
        {
            this.connectionChannel = connectionChannel;
        }

        public void start()
        {
            requestThread = new Thread(this::runRequests);
            if (currentState.compareAndSet(State.CREATED, State.STARTING))
            {
                requestThread.start();
            }
        }

        public void stop()
        {
            final State state = this.currentState.get();
            do
            {
                switch (state)
                {
                    case CREATED:
                        if (currentState.compareAndSet(State.CREATED, State.CLOSING))
                        {
                            return;
                        }
                        break;

                    case STARTING:
                        break;

                    case PENDING:
                        if (currentState.compareAndSet(State.PENDING, State.CLOSING))
                        {
                            try
                            {
                                connectionChannel.close();
                            }
                            catch (final IOException e)
                            {
                                e.printStackTrace();
                            }
                            return;
                        }
                        break;

                    case RUNNING:
                        if (currentState.compareAndSet(State.RUNNING, State.CLOSING))
                        {
                            responseReader.markClosed();
                            try
                            {
                                connectionChannel.close();
                            }
                            catch (final IOException e)
                            {
                                e.printStackTrace();
                            }
                            process.destroy();
                            return;
                        }
                        break;

                    case CLOSING:
                        return;
                }
            }
            while (true);
        }

        private void runRequests()
        {
            try
            {
                if (!currentState.compareAndSet(State.STARTING, State.PENDING))
                {
                    throw new IllegalStateException("Should not happen");
                }

                final ObjectInputStream requestIn = new ObjectInputStream(Channels.newInputStream(connectionChannel));
                while (!Thread.currentThread().isInterrupted())
                {
                    final Object o = requestIn.readObject();
                    if (o instanceof String[])
                    {
                        final State state = currentState.get();
                        switch (state)
                        {
                            case PENDING:
                                if (!currentState.compareAndSet(State.PENDING, State.STARTING))
                                {
                                    return;
                                }
                                currentState.set(startProcess((String[])o));

                                break;

                            case STARTING:
                                throw new IllegalStateException("Should not happen");

                            case RUNNING:
                                // Allow submission of more commands???
                                break;

                            case CLOSING:
                                return;
                        }
                    }
                    else
                    {
                        if (currentState.compareAndSet(State.RUNNING, State.CLOSING) && null != process)
                        {
                            responseReader.markClosed();
                            CloseHelper.close(connectionChannel);
                            process.destroy();
                        }
                    }
                }
            }
            catch (final EOFException e)
            {
                if (currentState.compareAndSet(State.RUNNING, State.CLOSING) && null != process)
                {
                    responseReader.markClosed();
                    CloseHelper.close(connectionChannel);
                    process.destroy();
                }
            }
            catch (final AsynchronousCloseException e)
            {
                if (currentState.compareAndSet(State.RUNNING, State.CLOSING) && null != process)
                {
                    responseReader.markClosed();
                    process.destroy();
                }
            }
            catch (final IOException | ClassNotFoundException e)
            {
                System.err.println("Error occurred");
                e.printStackTrace();

                if (currentState.compareAndSet(State.RUNNING, State.CLOSING) && null != process)
                {
                    responseReader.markClosed();
                    CloseHelper.close(connectionChannel);
                    process.destroy();
                }
            }
        }

        private State startProcess(final String[] command) throws IOException
        {
            try
            {
                final Process p = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start();

                process = p;

                responseReader = new ProcessResponseReader(connectionChannel);
                responseThread = new Thread(() -> responseReader.runResponses(p.getInputStream()));
                responseThread.start();

                return State.RUNNING;
            }
            catch (final IOException e)
            {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                e.printStackTrace(new PrintStream(baos));
                connectionChannel.write(ByteBuffer.wrap(baos.toByteArray()));
                connectionChannel.close();

                if (!currentState.compareAndSet(State.STARTING, State.CLOSING))
                {
                    throw new IllegalStateException("Should not happen");
                }

                return State.CLOSING;
            }
        }
    }

    private static final class ProcessResponseReader
    {
        private final SocketChannel connectionChannel;
        private volatile boolean isClosed = false;

        private ProcessResponseReader(final SocketChannel connectionChannel)
        {
            this.connectionChannel = connectionChannel;
        }

        private void runResponses(final InputStream processOutput)
        {
            final ByteBuffer data = ByteBuffer.allocate(1024);
            try
            {
                while (!isClosed)
                {
                    final int read = processOutput.read(data.array());
                    if (-1 != read)
                    {
                        data.position(0).limit(read);
                        connectionChannel.write(data);
                    }
                    else
                    {
                        connectionChannel.close();
                        break;
                    }
                }
            }
            catch (final IOException e)
            {
                if (!isClosed)
                {
                    throw new RuntimeException(e);
                }
                else
                {
                    System.out.println("Process closed");
                }
            }
        }

        public void markClosed()
        {
            isClosed = true;
        }
    }
}
