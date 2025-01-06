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
package io.aeron.cluster;

import org.junit.jupiter.api.Test;

import static io.aeron.Aeron.NULL_VALUE;
import static org.junit.jupiter.api.Assertions.*;

class LogSourceValidatorTest
{
    @Test
    void leaderLogSourceTypeShouldOnlyAcceptLeader()
    {
        final LogSourceValidator logSourceValidator = new LogSourceValidator(ClusterBackup.SourceType.LEADER);

        final long leaderMemberId = 123;
        final long followerMemberId = 456;

        assertTrue(logSourceValidator.isAcceptable(leaderMemberId, leaderMemberId));
        assertFalse(logSourceValidator.isAcceptable(leaderMemberId, followerMemberId));
        assertFalse(logSourceValidator.isAcceptable(NULL_VALUE, NULL_VALUE));
        assertFalse(logSourceValidator.isAcceptable(leaderMemberId, NULL_VALUE));
        assertFalse(logSourceValidator.isAcceptable(NULL_VALUE, followerMemberId));
    }

    @Test
    void followerLogSourceTypeShouldOnlyAcceptFollowerAndUnknown()
    {
        final LogSourceValidator logSourceValidator = new LogSourceValidator(ClusterBackup.SourceType.FOLLOWER);

        final long leaderMemberId = 123;
        final long followerMemberId = 456;

        assertFalse(logSourceValidator.isAcceptable(leaderMemberId, leaderMemberId));
        assertTrue(logSourceValidator.isAcceptable(leaderMemberId, followerMemberId));
        assertTrue(logSourceValidator.isAcceptable(NULL_VALUE, NULL_VALUE));
        assertTrue(logSourceValidator.isAcceptable(leaderMemberId, NULL_VALUE));
        assertTrue(logSourceValidator.isAcceptable(NULL_VALUE, followerMemberId));
    }


    @Test
    void anyLogSourceTypeShouldAny()
    {
        final LogSourceValidator logSourceValidator = new LogSourceValidator(ClusterBackup.SourceType.ANY);

        final long leaderMemberId = 123;
        final long followerMemberId = 456;

        assertTrue(logSourceValidator.isAcceptable(leaderMemberId, leaderMemberId));
        assertTrue(logSourceValidator.isAcceptable(leaderMemberId, followerMemberId));
        assertTrue(logSourceValidator.isAcceptable(NULL_VALUE, NULL_VALUE));
        assertTrue(logSourceValidator.isAcceptable(leaderMemberId, NULL_VALUE));
        assertTrue(logSourceValidator.isAcceptable(NULL_VALUE, followerMemberId));
    }
}