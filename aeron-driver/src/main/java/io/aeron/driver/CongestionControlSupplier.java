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
package io.aeron.driver;

import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersManager;

import java.net.InetSocketAddress;

/**
 * Supplier of {@link CongestionControl} algorithm implementations to be used by receivers.
 */
@FunctionalInterface
public interface CongestionControlSupplier
{
    /**
     * Return a new {@link CongestionControl} instance.
     *
     * @param registrationId  for the publication image.
     * @param udpChannel      for the publication image.
     * @param streamId        for the publication image.
     * @param sessionId       for the publication image.
     * @param termLength      for the publication image.
     * @param senderMtuLength for the publication image.
     * @param controlAddress  for the publication image.
     * @param sourceAddress   for the publication image.
     * @param nanoClock       for the precise timing.
     * @param context         for configuration options applied in the driver.
     * @param countersManager for the driver.
     * @return congestion control instance ready for immediate usage.
     */
    CongestionControl newInstance(
        long registrationId,
        UdpChannel udpChannel,
        int streamId,
        int sessionId,
        int termLength,
        int senderMtuLength,
        InetSocketAddress controlAddress,
        InetSocketAddress sourceAddress,
        NanoClock nanoClock,
        MediaDriver.Context context,
        CountersManager countersManager);
}
