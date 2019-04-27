/*
 * Copyright 2014-2019 Real Logic Ltd.
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
 * Strategy for applying congestion control to determine the receiver window length of the Status Messages
 */
public interface CongestionControl extends AutoCloseable
{
    int FORCE_STATUS_MESSAGE_BIT = 0x1;

    /**
     * Pack values into a long so they can be returned on the stack without allocation.
     *
     * @param receiverWindowLength to go in the lower bits.
     * @param forceStatusMessage   to go in the higher bits.
     * @return the packed value.
     */
    static long packOutcome(final int receiverWindowLength, final boolean forceStatusMessage)
    {
        final int flags = forceStatusMessage ? FORCE_STATUS_MESSAGE_BIT : 0x0;

        return ((long)flags << 32) | receiverWindowLength;
    }

    /**
     * Extract the receiver window length from the packed value.
     *
     * @param outcome as the packed value.
     * @return the receiver window length.
     */
    static int receiverWindowLength(final long outcome)
    {
        return (int)outcome;
    }

    /**
     * Extract the boolean value for if a status message should be forced from the packed value.
     *
     * @param outcome which is packed containing the force status message flag.
     * @return true if the force status message bit has been set.
     */
    static boolean shouldForceStatusMessage(final long outcome)
    {
        return ((int)(outcome >>> 32) & FORCE_STATUS_MESSAGE_BIT) == FORCE_STATUS_MESSAGE_BIT;
    }

    /**
     * Threshold increment in a window after which a status message should be scheduled.
     *
     * @param windowLength to calculate the threshold from.
     * @return the threshold in the window after which a status message should be scheduled.
     */
    static int threshold(final int windowLength)
    {
        return windowLength / 4;
    }

    /**
     * Polled by {@link Receiver} to determine when to initiate an RTT measurement to a Sender.
     *
     * @param nowNs in nanoseconds
     * @return true for should measure RTT now or false for no measurement
     */
    boolean shouldMeasureRtt(long nowNs);

    /**
     * Called by {@link Receiver} to record that a measurement request has been sent.
     *
     * @param nowNs in nanoseconds.
     */
    void onRttMeasurementSent(long nowNs);

    /**
     * Called by {@link Receiver} on reception of an RTT Measurement.
     *
     * @param nowNs      in nanoseconds
     * @param rttNs      to the Sender in nanoseconds
     * @param srcAddress of the Sender
     */
    void onRttMeasurement(long nowNs, long rttNs, InetSocketAddress srcAddress);

    /**
     * Called by {@link DriverConductor} upon execution of {@link PublicationImage#trackRebuild(long, long)} to
     * pass on current status.
     * <p>
     * The return value must be packed using {@link CongestionControl#packOutcome(int, boolean)}.
     *
     * @param nowNs                   current time
     * @param newConsumptionPosition  of the subscribers
     * @param lastSmPosition          of the image
     * @param hwmPosition             of the image
     * @param startingRebuildPosition of the rebuild
     * @param endingRebuildPosition   of the rebuild
     * @param lossOccurred            during rebuild
     * @return outcome of congestion control calculation containing window length and whether to force sending an SM.
     */
    long onTrackRebuild(
        long nowNs,
        long newConsumptionPosition,
        long lastSmPosition,
        long hwmPosition,
        long startingRebuildPosition,
        long endingRebuildPosition,
        boolean lossOccurred);

    /**
     * Called by {@link DriverConductor} to initialise window length for a new {@link PublicationImage}.
     *
     * @return new image window length
     */
    int initialWindowLength();

    void close();
}
