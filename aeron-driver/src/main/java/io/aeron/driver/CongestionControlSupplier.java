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
package io.aeron.driver;

import io.aeron.driver.media.UdpChannel;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.CountersManager;

public interface CongestionControlSupplier
{
    /**
     * Return a new {@link CongestionControl} instance.
     *
     * @param registrationId  for the instance.
     * @param udpChannel      for the instance.
     * @param streamId        for the instance.
     * @param sessionId       for the instance.
     * @param termLength      for the instance.
     * @param senderMtuLength for the instance.
     * @param clock           for the instance.
     * @param context         for the instance.
     * @param countersManager for the instance.
     * @return congestion control instance ready for immediate usage.
     */
    CongestionControl newInstance(
        long registrationId,
        UdpChannel udpChannel,
        int streamId,
        int sessionId,
        int termLength,
        int senderMtuLength,
        NanoClock clock,
        MediaDriver.Context context,
        CountersManager countersManager);
}
