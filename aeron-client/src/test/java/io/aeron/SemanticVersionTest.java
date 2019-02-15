/*
 * Copyright 2019 Real Logic Ltd.
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

package io.aeron;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SemanticVersionTest
{
    @Test
    public void shouldComposeValidVersion()
    {
        final int major = 17;
        final int minor = 9;
        final int patch = 127;

        final int version = SemanticVersion.compose(major, minor, patch);

        assertEquals(major, SemanticVersion.major(version));
        assertEquals(minor, SemanticVersion.minor(version));
        assertEquals(patch, SemanticVersion.patch(version));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDetectNegativeMajor()
    {
        SemanticVersion.compose(-1, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDetectNegativeMinor()
    {
        SemanticVersion.compose(1, -1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDetectNegativePatch()
    {
        SemanticVersion.compose(1, 1, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDetectExcessiveMajor()
    {
        SemanticVersion.compose(256, 1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDetectExcessiveMinor()
    {
        SemanticVersion.compose(1, 256, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldDetectExcessivePatch()
    {
        SemanticVersion.compose(1, 1, 256);
    }
}