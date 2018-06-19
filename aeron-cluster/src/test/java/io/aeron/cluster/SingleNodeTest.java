/*
 * Copyright 2014-2018 Real Logic Ltd.
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

import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import org.junit.Ignore;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@Ignore
public class SingleNodeTest
{
    @Test(timeout = 10_000L)
    public void shouldBeAbleToComeUpInDefaultConfig()
    {
        final ClusteredService mockService = mock(ClusteredService.class);

        try (ConsensusModuleHarness harness = new ConsensusModuleHarness(
            new ConsensusModule.Context(), mockService, null, true, true, false))
        {
            harness.awaitServiceOnStart();
            harness.awaitServiceOnRoleChange(Cluster.Role.LEADER);
        }
    }

    @Test(timeout = 10_000L)
    public void shouldBeAbleToLoadUpFromPreviousLog()
    {
        ConsensusModuleHarness.makeRecordingLog(10, 100, null, null, new ConsensusModule.Context());

        final ClusteredService mockService = mock(ClusteredService.class);

        try (ConsensusModuleHarness harness = new ConsensusModuleHarness(
            new ConsensusModule.Context(), mockService, null, false, true, false))
        {
            harness.awaitServiceOnStart();
            harness.awaitServiceOnRoleChange(Cluster.Role.LEADER);
            harness.awaitServiceOnMessageCounter(10);

            verify(mockService, times(10))
                .onSessionMessage(any(ClientSession.class), anyLong(), anyLong(), any(), anyInt(), eq(100), any());
        }
    }
}
