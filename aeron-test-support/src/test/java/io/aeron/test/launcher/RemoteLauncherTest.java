package io.aeron.test.launcher;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@EnabledOnOs(OS.LINUX)
public class RemoteLauncherTest
{
    @Test
    void shouldLaunchShortLivedCommand() throws IOException, InterruptedException
    {
        final RemoteLaunchServer server = new RemoteLaunchServer("localhost", 11112);
        final Thread t = new Thread(server::run);
        t.start();
        final RemoteLaunchClient client = RemoteLaunchClient.connect("localhost", 11112);
        try
        {
            final String message = "helloworld";
            assertTimeoutPreemptively(
                Duration.ofMillis(5_000),
                () ->
                {
                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    client.executeBlocking(out, "/usr/bin/echo", message);

                    final byte[] expectedBytes = message.getBytes(StandardCharsets.UTF_8);
                    final byte[] actualBytes = Arrays.copyOf(out.toByteArray(), expectedBytes.length);
                    assertArrayEquals(expectedBytes, actualBytes);
                }
            );
        }
        finally
        {
            server.close();
            t.join();
        }
    }

    @Test
    void shouldResolveAeronAllJar()
    {
        assertNotNull(FileResolveUtil.resolveAeronAllJar());
    }
}
