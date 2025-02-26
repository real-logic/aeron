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
package io.aeron;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.JRE;

import static org.junit.jupiter.api.Assertions.assertEquals;

class VerifyBuildTimePropertiesTest
{
    private static final String BUILD_JAVA_VERSION_ENV_VAR = "BUILD_JAVA_VERSION";

    @Test
    @EnabledIfEnvironmentVariable(named = BUILD_JAVA_VERSION_ENV_VAR, matches = ".+")
    void checkVersion()
    {
        String version = System.getenv(BUILD_JAVA_VERSION_ENV_VAR);

        if (version.indexOf('.') > 0)
        {
            version = version.substring(0, version.indexOf('.'));
        }

        if (version.indexOf('-') > 0)
        {
            version = version.substring(0, version.indexOf('-'));
        }

        final String currentVersion = JRE.currentJre().name();
        assertEquals(version, currentVersion.substring(currentVersion.indexOf('_') + 1));
    }
}
