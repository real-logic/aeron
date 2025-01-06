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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class PrintEnvInfoTest
{
    @Disabled
    @Test
    void test()
    {
        System.out.println("=========================");
        System.out.println("[PrintEnvInfo] System properties:");
        System.out.println("=========================");
        System.getProperties().entrySet().stream()
            .filter((e) -> ((String)e.getKey()).contains("java."))
            .forEach((e) -> System.out.println("- " + e.getKey() + ": " + e.getValue()));

        System.out.println("\n=========================");
        System.out.println("[PrintEnvInfo] ENV variables:");
        System.out.println("=========================");
        for (final String env : new String[]{ "JAVA_HOME", "BUILD_JAVA_HOME", "BUILD_JAVA_VERSION", "PATH" })
        {
            System.out.println("- " + env + ": " + System.getenv(env));
        }
    }
}
