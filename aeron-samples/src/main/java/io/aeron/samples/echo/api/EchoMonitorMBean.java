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
package io.aeron.samples.echo.api;

/**
 * MBean interface for interacting with a provisioned Echo sub/pub pair.
 */
public interface EchoMonitorMBean
{
    /**
     * Get the correlationId used to provision the pair.
     *
     * @return original caller supplied correlationId.
     */
    long getCorrelationId();

    /**
     * Get the measured count of back pressure events when trying to echo from the subscription to the publication.
     *
     * @return current back pressure count.
     */
    long getBackPressureCount();

    /**
     * Get the number of fragments echoed from the subscription to the publication.
     *
     * @return current fragment count.
     */
    long getFragmentCount();

    /**
     * Get the number of bytes echoed through the pair.
     *
     * @return number of bytes.
     */
    long getByteCount();
}
