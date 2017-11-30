/*
 * Copyright 2017 Real Logic Ltd.
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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.CloseHelper;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class SequencerAgentTest
{
    private MediaDriver driver;

    @Before
    public void before()
    {
        driver = MediaDriver.launch(
            new MediaDriver.Context()
                .threadingMode(ThreadingMode.DEDICATED)
                .spiesSimulateConnection(true)
                .errorHandler(Throwable::printStackTrace)
                .dirDeleteOnStart(true));
    }

    @After
    public void after()
    {
        CloseHelper.close(driver);

        driver.context().deleteAeronDirectory();
    }

    @Test(timeout = 10_000, expected = IllegalStateException.class)
    public void shouldLimitActiveSessions()
    {
        try (Aeron aeron = Aeron.connect())
        {
            final ConsensusModule.Context ctx = new ConsensusModule.Context()
                .errorCounter(mock(AtomicCounter.class))
                .errorHandler(Throwable::printStackTrace)
                .aeron(aeron)
                .maxActiveSessions(1);

            ctx.conclude();

            final AgentRunner agentRunner = new AgentRunner(
                new SleepingMillisIdleStrategy(1),
                Throwable::printStackTrace,
                null,
                new SequencerAgent(ctx));
            AgentRunner.startOnThread(agentRunner);

            AeronCluster.connect(new AeronCluster.Context().aeron(aeron).ownsAeronClient(false));
            AeronCluster.connect(new AeronCluster.Context().aeron(aeron).ownsAeronClient(false));

            agentRunner.close();
        }
    }
}