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

/**
 * Timer service providing API to schedule timers in the Cluster.
 */
public interface TimerService
{
    /**
     * Called from the {@link #poll(long)}.
     */
    @FunctionalInterface
    interface TimerHandler
    {
        /**
         * Invoked for each expired timer.
         *
         * @param correlationId of the timer.
         * @return {@code true} if the timer event was processed or {@code false} otherwise, i.e. the timer should be
         * kept as non-expired.
         */
        boolean onTimerEvent(long correlationId);
    }

    /**
     * Called from the {@link #snapshot(TimerSnapshotTaker)}.
     */
    @FunctionalInterface
    interface TimerSnapshotTaker
    {
        /**
         * Take a snapshot of the timer.
         *
         * @param correlationId of the timer.
         * @param deadline      when the timer expires.
         */
        void snapshotTimer(long correlationId, long deadline);
    }

    /**
     * Max number of the timers to expire per single {@link #poll(long)}.
     */
    int POLL_LIMIT = 20;

    /**
     * Poll for expired timers. For each expired timer it invokes {@link TimerHandler#onTimerEvent(long)} on the
     * {@link TimerHandler} that was provided upon creation of this {@link TimerService} instance.
     * Poll can be terminated early if {@link #POLL_LIMIT} is reached or if {@link TimerHandler#onTimerEvent(long)}
     * returns {@code false}.
     *
     * @param now current time.
     * @return number of timers expired, never exceeds {@link #POLL_LIMIT}.
     */
    int poll(long now);

    /**
     * Schedule timer with the given {@code correlationId} and {@code deadline}.
     *
     * @param correlationId to associate with the timer.
     * @param deadline      when the timer expires.
     */
    void scheduleTimerForCorrelationId(long correlationId, long deadline);

    /**
     * Cancels timer by its {@code correlationId}.
     *
     * @param correlationId of the timer to cancel.
     * @return {@code true} if the timer was cancelled.
     */
    boolean cancelTimerByCorrelationId(long correlationId);

    /**
     * Takes a snapshot of the timer service state, i.e. serializes all non-expired timers.
     *
     * @param snapshotTaker to take a snapshot.
     */
    void snapshot(TimerSnapshotTaker snapshotTaker);

    /**
     * Set the current time from the Cluster in case the underlying implementation depends on it.
     *
     * @param now the current time.
     */
    void currentTime(long now);
}
