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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.test.HideOutputCallback;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(InterruptingTestCallback.class)
public class ErrorAndEventLoggingTest
{
    @RegisterExtension
    public final MediaDriverTestWatcher watcher = new MediaDriverTestWatcher();

    private final ArrayList<AutoCloseable> closeables = new ArrayList<>();

    private MediaDriver.Context context;
    private Aeron aeron;
    private TestMediaDriver driver;

    private void launch()
    {
        driver = TestMediaDriver.launch(context, watcher);
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
    }

    @AfterEach
    public void after()
    {
        deleteBackupFiles("-event.log", "-error.log");

        CloseHelper.closeAll(closeables);
        CloseHelper.closeAll(aeron, driver);
        if (null != driver)
        {
            driver.context().deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(5)
    @ExtendWith(HideOutputCallback.class)
    void shouldBackupEventAndErrorLogFiles() throws IOException
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        context = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .errorHandler(null)
            .resolverName("A")
            .resolverInterface("0.0.0.0:8050")
            .resolverBootstrapNeighbor("localhost:8051")
            .dirDeleteOnStart(false);

        launch();

        final long initialErrorCount = aeron.countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());

        addPublication("aeron:udp?endpoint=localhost:9999|mtu=1408", 1000);
        addSubscription("aeron:udp?endpoint=localhost:9999|rcv-wnd=1376", 1000);

        Tests.awaitCounterDelta(aeron.countersReader(), SystemCounterDescriptor.ERRORS.id(), initialErrorCount, 1);

        final Matcher<String> exceptionMessageMatcher = allOf(
            containsString("mtuLength="),
            containsString("> initialWindowLength="));

        SystemTests.waitForErrorToOccur(driver.aeronDirectoryName(), exceptionMessageMatcher, Tests.SLEEP_1_MS);

        try (
            MediaDriver otherDriver1 = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .resolverName("B")
                .resolverInterface("0.0.0.0:8051")
                .resolverBootstrapNeighbor("localhost:8050")
                .dirDeleteOnShutdown(true));
            MediaDriver otherDriver2 = MediaDriver.launchEmbedded(new MediaDriver.Context()
                .resolverName("B")
                .resolverInterface("0.0.0.0:8052")
                .resolverBootstrapNeighbor("localhost:8050")
                .dirDeleteOnShutdown(true)))
        {
            SystemTests.waitForEventToOccur(
                driver.aeronDirectoryName(),
                containsString("Address changed for name: B"),
                Tests.SLEEP_1_MS);
        }

        CloseHelper.closeAll(closeables);
        closeables.clear();
        CloseHelper.closeAll(aeron, driver);
        aeron = null;
        driver = null;

        context = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .errorHandler(null)
            .dirDeleteOnStart(false);

        launch();

        final File backupErrorLogFile = findBackupErrorLogFile(aeronDirectoryName);

        assertNotNull(backupErrorLogFile);
        assertTrue(backupErrorLogFile.exists());
        assertTrue(0 < backupErrorLogFile.length());

        final File backupEventLogFile = findBackupEventLogFile(aeronDirectoryName);

        assertNotNull(backupEventLogFile);
        assertTrue(backupEventLogFile.exists());
        assertTrue(0 < backupEventLogFile.length());
    }

    private Publication addPublication(final String channel, final int streamId)
    {
        final Publication pub = aeron.addPublication(channel, streamId);
        closeables.add(pub);
        return pub;
    }

    private Subscription addSubscription(final String channel, final int streamId)
    {
        final Subscription sub = aeron.addSubscription(channel, streamId);
        closeables.add(sub);
        return sub;
    }

    private File findBackupErrorLogFile(final String aeronDirectoryName)
    {
        return findBackupFile(aeronDirectoryName, "-error.log");
    }

    private File findBackupEventLogFile(final String aeronDirectoryName)
    {
        return findBackupFile(aeronDirectoryName, "-event.log");
    }

    private File findBackupFile(final String aeronDirectoryName, final String suffix)
    {
        final File aeronDirectory = new File(aeronDirectoryName);
        final File[] aeronErrorBackupFiles = aeronDirectory.getParentFile().listFiles(
            (dir, name) -> name.startsWith(aeronDirectory.getName()) && name.endsWith(suffix));

        return null != aeronErrorBackupFiles && 1 == aeronErrorBackupFiles.length ? aeronErrorBackupFiles[0] : null;
    }

    private void deleteBackupFiles(final String... suffixes)
    {
        for (final String suffix : suffixes)
        {
            final File backupErrorLogFile = findBackupFile(aeron.context().aeronDirectoryName(), suffix);
            if (null != backupErrorLogFile)
            {
                //noinspection ResultOfMethodCallIgnored
                backupErrorLogFile.delete();
            }
        }
    }
}
