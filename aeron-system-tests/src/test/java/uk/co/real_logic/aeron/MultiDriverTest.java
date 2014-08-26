/*
 * Copyright 2014 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.real_logic.aeron;

import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.aeron.common.IoUtil;
import uk.co.real_logic.aeron.common.concurrent.AtomicBuffer;
import uk.co.real_logic.aeron.driver.MediaDriver;

import java.io.File;

import static org.mockito.Mockito.mock;

/**
 * Tests requiring multiple embedded drivers
 */
public class MultiDriverTest
{
    public static final String UNICAST_URI = "udp://localhost:54325";
    public static final String MULTICAST_URI = "udp://localhost@224.20.30.39:54326";

    private static final int STREAM_ID = 1;
    private static final int SESSION_ID = 2;

    private final MediaDriver.Context driverAContext = new MediaDriver.Context();
    private final MediaDriver.Context driverBContext = new MediaDriver.Context();
    private final Aeron.Context aeronAContext = new Aeron.Context();
    private final Aeron.Context aeronBContext = new Aeron.Context();

    private Aeron clientA;
    private Aeron clientB;
    private MediaDriver driverA;
    private MediaDriver driverB;
    private Subscription subscription;
    private Publication publication;

    private AtomicBuffer buffer = new AtomicBuffer(new byte[4096]);
    private DataHandler dataHandler = mock(DataHandler.class);

    private void setup(final String channel)
    {
        final String baseDirA = IoUtil.tmpDirName() + "aeron-system-tests" + File.separator + "A";
        final String baseDirB = IoUtil.tmpDirName() + "aeron-system-tests" + File.separator + "B";

        driverAContext.dirsDeleteOnExit(true);
        driverAContext.adminDirName(baseDirA + File.separator + "conductor");
        driverAContext.dataDirName(baseDirA + File.separator + "data");
        driverAContext.countersDirName(baseDirA + File.separator + "counters");
        driverAContext.eventLocationsFile(new File(baseDirA + File.separator + "events"));

        aeronAContext.adminDirName(driverAContext.adminDirName());
        aeronAContext.dataDirName(driverAContext.dataDirName());
        aeronAContext.countersDirName(driverAContext.countersDirName());

        driverBContext.dirsDeleteOnExit(true);
        driverBContext.adminDirName(baseDirB + File.separator + "conductor");
        driverBContext.dataDirName(baseDirB + File.separator + "data");
        driverBContext.countersDirName(baseDirB + File.separator + "counters");
        driverBContext.eventLocationsFile(new File(baseDirB + File.separator + "events"));

        aeronBContext.adminDirName(driverBContext.adminDirName());
        aeronBContext.dataDirName(driverBContext.dataDirName());
        aeronBContext.countersDirName(driverBContext.countersDirName());

        driverA = MediaDriver.launch(driverAContext);
        driverB = MediaDriver.launch(driverBContext);
        clientA = Aeron.connect(aeronAContext);
        clientB = Aeron.connect(aeronBContext);
        publication = clientA.addPublication(channel, STREAM_ID, SESSION_ID);
        subscription = clientB.addSubscription(channel, STREAM_ID, dataHandler);
    }

    @After
    public void closeEverything()
    {
        if (null != publication)
        {
            publication.release();
        }

        if (null != subscription)
        {
            subscription.close();
        }

        clientB.close();
        clientA.close();
        driverB.close();
        driverA.close();
    }

    @Test(timeout = 10000)
    public void shouldSpinUpAndShutdown()
    {
        setup(UNICAST_URI);
    }
}
