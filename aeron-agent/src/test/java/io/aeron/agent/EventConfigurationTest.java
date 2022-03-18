/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.agent;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.EnumSet;
import java.util.Set;

import static io.aeron.agent.ArchiveEventCode.*;
import static io.aeron.agent.ClusterEventCode.*;
import static io.aeron.agent.DriverEventCode.*;
import static io.aeron.agent.EventConfiguration.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class EventConfigurationTest
{
    @Test
    public void nullValueMeansNoEventsEnabled()
    {
        assertEquals(getDriverEventCodes(null), EnumSet.noneOf(DriverEventCode.class));
    }

    @Test
    public void malformedPropertyShouldDefaultToProductionEventCodes()
    {
        final PrintStream err = System.err;
        final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        System.setErr(new PrintStream(stderr));
        try
        {
            final Set<DriverEventCode> enabledEventCodes = getDriverEventCodes("list of invalid options");
            assertEquals(EnumSet.noneOf(DriverEventCode.class), enabledEventCodes);
            assertThat(stderr.toString(), startsWith("unknown event code: list of invalid options"));
        }
        finally
        {
            System.setErr(err);
        }
    }

    @Test
    public void allDriverEventsShouldBeEnabled()
    {
        assertEquals(EnumSet.allOf(DriverEventCode.class), getDriverEventCodes("all"));
    }

    @Test
    public void adminDriverEventsShouldBeEnabled()
    {
        assertEquals(
            EnumSet.complementOf(EnumSet.of(
            FRAME_IN,
            FRAME_OUT,
            NAME_RESOLUTION_NEIGHBOR_ADDED,
            NAME_RESOLUTION_NEIGHBOR_REMOVED)),
            getDriverEventCodes("admin"));
    }

    @Test
    public void driverEventCodesShouldBeParsedAsListOfEventCodes()
    {
        final Set<DriverEventCode> expectedCodes = EnumSet.of(
            FRAME_OUT,
            FRAME_IN,
            CMD_IN_CLIENT_CLOSE,
            UNTETHERED_SUBSCRIPTION_STATE_CHANGE);
        assertEquals(expectedCodes,
            getDriverEventCodes("FRAME_OUT,FRAME_IN,CMD_IN_CLIENT_CLOSE,UNTETHERED_SUBSCRIPTION_STATE_CHANGE,"));
    }

    @Test
    public void allClusterEventsShouldBeEnabled()
    {
        assertEquals(EnumSet.allOf(ClusterEventCode.class), getClusterEventCodes("all"));
    }

    @Test
    public void noPositionEventsShouldEnableAllClusterEventsExcludingThePositionRelatedOnes()
    {
        assertEquals(
            EnumSet.complementOf(EnumSet.of(COMMIT_POSITION, APPEND_POSITION, CANVASS_POSITION)),
            getClusterEventCodes("no-position-events"));
    }

    @Test
    public void clusterEventCodesShouldBeParsedAsListOfEventCodes()
    {
        assertEquals(EnumSet.of(
            STATE_CHANGE,
            NEW_LEADERSHIP_TERM,
            ROLE_CHANGE),
            getClusterEventCodes("STATE_CHANGE,NEW_LEADERSHIP_TERM,ROLE_CHANGE,"));
    }

    @Test
    public void allArchiveEventsShouldBeEnabled()
    {
        assertEquals(EnumSet.allOf(ArchiveEventCode.class), getArchiveEventCodes("all"));
    }

    @Test
    public void archiveEventsShouldBeParsedAsListOfEventCodes()
    {
        assertEquals(EnumSet.of(CATALOG_RESIZE, CMD_IN_TAGGED_REPLICATE),
            getArchiveEventCodes("CATALOG_RESIZE,CMD_IN_TAGGED_REPLICATE,"));
    }

    @Test
    void shouldDisableSpecificDriverEvents()
    {
        EventConfiguration.init(
            "all",
            "FRAME_IN,FRAME_OUT",
            null,
            null,
            null,
            null);
        assertEquals(DriverEventCode.values().length - 2, DRIVER_EVENT_CODES.size());
        assertFalse(DRIVER_EVENT_CODES.contains(FRAME_IN));
        assertFalse(DRIVER_EVENT_CODES.contains(FRAME_OUT));
    }

    @Test
    void shouldDisableSpecificArchiverEvents()
    {
        EventConfiguration.init(
            null,
            null,
            "all",
            CMD_IN_ATTACH_SEGMENTS.name() + "," + CMD_IN_CONNECT,
            null,
            null);
        assertEquals(ArchiveEventCode.values().length - 2, ARCHIVE_EVENT_CODES.size());
        assertFalse(ARCHIVE_EVENT_CODES.contains(CMD_IN_ATTACH_SEGMENTS));
        assertFalse(ARCHIVE_EVENT_CODES.contains(CMD_IN_CONNECT));
    }

    @Test
    void shouldDisableSpecificClusterEvents()
    {
        EventConfiguration.init(
            null,
            null,
            null,
            null,
            "all",
            ROLE_CHANGE.name() + "," + ELECTION_STATE_CHANGE);
        assertEquals(ClusterEventCode.values().length - 2, CLUSTER_EVENT_CODES.size());
        assertFalse(CLUSTER_EVENT_CODES.contains(ROLE_CHANGE));
        assertFalse(CLUSTER_EVENT_CODES.contains(ELECTION_STATE_CHANGE));
    }
}
