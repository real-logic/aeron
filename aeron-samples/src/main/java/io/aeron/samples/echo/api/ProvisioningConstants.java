/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.samples.echo.api;

/**
 * Constants used by the provisioning API.
 */
public class ProvisioningConstants
{
    /**
     * TODO.
     */
    public static final String IO_AERON_TYPE_PROVISIONING_NAME_TESTING = "io.aeron:type=Provisioning,name=testing";

    /**
     * TODO.
     */
    public static final String IO_AERON_TYPE_ECHO_PAIR_PREFIX = "io.aeron:type=EchoPair,name=";

    /**
     * Generate MBean echo pair object name from the specified correlationId.
     *
     * @param correlationId user defined correlationId.
     * @return a legal JMX object name.
     */
    public static String echoPairObjectName(final long correlationId)
    {
        return IO_AERON_TYPE_ECHO_PAIR_PREFIX + correlationId;
    }
}
