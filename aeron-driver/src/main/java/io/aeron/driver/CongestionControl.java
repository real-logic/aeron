/*
 * Copyright 2016 Real Logic Ltd.
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

import java.net.InetSocketAddress;

/**
 * Strategy for applying congestion control to determine the receiverWindowLength of the Status Messages
 */
public interface CongestionControl
{
    /// polled by Receiver to determine when to initiate an RTT Measurement to the Sender
    boolean shouldMeasureRtt(long now);

    /// called by Receiver on reception of RTT
    void onRttMeasurement(long now, long rttInNanos, InetSocketAddress srcAddress);

    /// called by Conductor on trackRebuild. Returned value indicates forcing a new SM as well as receiverWindowLength
    long onTrackRebuild(
        long now,
        long newConsumptiopnPosition,
        long lastSmPosition,
        long hwmPosition,
        long startingRebuildPosition,
        long endingRebuildPosition);
}
