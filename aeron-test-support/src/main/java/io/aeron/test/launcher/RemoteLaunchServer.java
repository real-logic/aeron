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
package io.aeron.test.launcher;

import io.aeron.test.NullOutputStream;
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
        Thread.setDefaultUncaughtExceptionHandler(
            (t, e) ->
            {
                System.err.println("Uncaught exception on: " + t);
                e.printStackTrace(System.out);
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
        catch (final AsynchronousCloseException ignore)
        {
            // Normal close
        }
        catch (final IOException ex)
        {
            throw new RuntimeException(ex);
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
            e.printStackTrace(System.out);
        }

        connections.forEach(Connection::stop);
    }

    static class Connection
    {
        private final SocketChannel connectionChannel;
        private final AtomicReference<State> currentState = new AtomicReference<>(State.CREATED);
        private volatile ProcessResponseReader responseReader;
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
            final Thread requestThread = new Thread(this::runRequests);
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
                                e.printStackTrace(System.out);
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
                                e.printStackTrace(System.out);
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

                try (ObjectInputStream in = new ObjectInputStream(Channels.newInputStream(connectionChannel)))
                {
                    while (!Thread.currentThread().isInterrupted())
                    {
                        final Object o = in.readObject();
                        if (o instanceof String[])
                        {
                            final State state = currentState.get();
                            switch (state)
                            {
                                case CREATED:
                                    break;

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
            }
            catch (final EOFException ex)
            {
                if (currentState.compareAndSet(State.RUNNING, State.CLOSING) && null != process)
                {
                    responseReader.markClosed();
                    CloseHelper.close(connectionChannel);
                    process.destroy();
                }
            }
            catch (final AsynchronousCloseException ex)
            {
                if (currentState.compareAndSet(State.RUNNING, State.CLOSING) && null != process)
                {
                    responseReader.markClosed();
                    process.destroy();
                }
            }
            catch (final IOException | ClassNotFoundException ex)
            {
                System.out.println("Error occurred");
                ex.printStackTrace(System.out);

                if (currentState.compareAndSet(State.RUNNING, State.CLOSING) && null != process)
                {
                    responseReader.markClosed();
                    CloseHelper.close(connectionChannel);
                    process.destroy();
                }
            }
        }

        private long pid()
        {
            return ProcessHandle.current().pid();
        }

        private State startProcess(final String[] command) throws IOException
        {
            try
            {
                final Process p = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .start();
                process = p;

                final long startTimeMs = System.currentTimeMillis();
                System.out.println("[" + pid() + "] Started: " + String.join(" ", command));
                final PrintStream stdOutputStream = parseBaseDirectory(command);

                responseReader = new ProcessResponseReader(connectionChannel, pid(), stdOutputStream);
                final Thread responseThread = new Thread(() -> responseReader.runResponses(p.getInputStream()));
                responseThread.setDaemon(true);
                responseThread.start();

                final Thread processMonitorThread = new Thread(
                    () ->
                    {
                        try
                        {
                            final int exitCode = process.waitFor();
                            final long endTimeMs = System.currentTimeMillis();
                            System.out.println(
                                "[" + pid() + "] Exited with code: " + exitCode +
                                " after: " + (endTimeMs - startTimeMs) + "ms");
                        }
                        catch (final InterruptedException ex)
                        {
                            System.out.println("[" + pid() + "] Unexpected exception waiting on exit code");
                            ex.printStackTrace(System.out);
                        }
                    });
                processMonitorThread.start();

                return State.RUNNING;
            }
            catch (final IOException ex)
            {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ex.printStackTrace(new PrintStream(baos));
                connectionChannel.write(ByteBuffer.wrap(baos.toByteArray()));
                connectionChannel.close();

                if (!currentState.compareAndSet(State.STARTING, State.CLOSING))
                {
                    throw new IllegalStateException("Should not happen");
                }

                return State.CLOSING;
            }
        }

        private PrintStream parseBaseDirectory(final String[] command)
        {
            final String baseDirPrefix = "-Daeron.cluster.tutorial.baseDir=";
            for (final String arg : command)
            {
                final String trimmedArg = arg.trim();
                if (trimmedArg.startsWith(baseDirPrefix))
                {
                    final File file = new File(trimmedArg.substring(baseDirPrefix.length()), "command.out");
                    try
                    {
                        return new PrintStream(file.getAbsolutePath(), "UTF-8");
                    }
                    catch (final Exception ex)
                    {
                        System.err.println("Failed to create stream for command std: " + ex.getMessage());
                    }
                }
            }

            return new PrintStream(new NullOutputStream());
        }
    }

    private static final class ProcessResponseReader
    {
        private final SocketChannel connectionChannel;
        private final long pid;
        private final PrintStream stdOutputStream;
        private volatile boolean isClosed = false;

        private ProcessResponseReader(
            final SocketChannel connectionChannel,
            final long pid,
            final PrintStream stdOutputStream)
        {
            this.connectionChannel = connectionChannel;
            this.pid = pid;
            this.stdOutputStream = stdOutputStream;
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
                        stdOutputStream.write(data.array(), 0, read);
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
                    System.out.println("[" + pid + "] Process closed");
                }
            }
            finally
            {
                CloseHelper.quietClose(stdOutputStream);
            }
        }

        public void markClosed()
        {
            isClosed = true;
        }
    }
}
