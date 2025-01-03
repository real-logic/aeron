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

import org.junit.jupiter.api.Disabled;
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
    @Disabled
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
                    client.executeBlocking(out, "echo", message);

                    final byte[] expectedBytes = message.getBytes(StandardCharsets.UTF_8);
                    final byte[] actualBytes = Arrays.copyOf(out.toByteArray(), expectedBytes.length);
                    assertArrayEquals(expectedBytes, actualBytes, out + " vs " + new String(actualBytes));
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
