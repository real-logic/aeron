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

/**
 * Feedback delay used for NAKs as well as for some retransmission use cases.
 * <p>
 * Generates delay based on Optimal Multicast Feedback<br>
 * <a target="_blank" href="http://tools.ietf.org/html/rfc5401#page-13">http://tools.ietf.org/html/rfc5401#page-13</a>
 * <p>
 * {@code maxBackoffT} is max interval for delay
 * <p>
 * C version of the code:
 * <pre>{@code
 * double RandomBackoff(double maxBackoffT, double groupSize)
 * {
 *     double lambda = log(groupSize) + 1;
 *     double x = UniformRand(lambda / maxBackoffT) + lambda / (maxBackoffT * (exp(lambda) - 1));
 *     return ((maxBackoffT / lambda) * log(x * (exp(lambda) - 1) * (maxBackoffT / lambda)));
 * }
 * }</pre>
 * where {@code UniformRand(x)} is uniform distribution from {@code 0..max}
 * <p>
 * In this implementations calculation:
 * <ul>
 *     <li>the {@code groupSize} is a constant (could be configurable as system property)</li>
 *     <li>{@code maxBackoffT} is a constant (could be configurable as system property)</li>
 *     <li>{@code GRTT} is a constant (could be configurable as a system property)</li>
 * </ul>
 * <p>
 * {@code N} (the expected number of feedback messages per RTT) is:<br>
 * {@code N = exp(1.2 * L / (2 * maxBackoffT / GRTT))}
 * <p>
 * <b>Assumptions:</b>
 * <p>
 * {@code maxBackoffT = K * GRTT (K >= 1)}
 * <p>
 * Recommended {@code K}:
 * <ul>
 *     <li>K = 4 for situations where responses come from multiple places (i.e. for NAKs, multiple retransmitters)</li>
 *     <li>K = 6 for situations where responses come from single places (i.e. for NAKs, source only retransmit)</li>
 * </ul>
 */
public class OptimalMulticastDelayGenerator implements FeedbackDelayGenerator
{
    private final double calculatedN;
    private final double randMax;
    private final double baseX;
    private final double constantT;
    private final double factorT;

    /**
     * Create new feedback delay generator based on estimates. Pre-calculating some parameters upfront.
     * <p>
     * {@code maxBackoffT} and {@code gRtt} must be expressed in the same units.
     *
     * @param maxBackoffT of the delay interval
     * @param groupSize   estimate
     * @param gRtt        estimate
     */
    public OptimalMulticastDelayGenerator(final double maxBackoffT, final double groupSize, final double gRtt)
    {
        final double lambda = Math.log(groupSize) + 1;
        this.calculatedN = Math.exp(1.2 * lambda / (2 * maxBackoffT / gRtt));

        // constant pieces of the calculation
        this.randMax = lambda / maxBackoffT;
        this.baseX = lambda / (maxBackoffT * (Math.exp(lambda) - 1));
        this.constantT = maxBackoffT / lambda;
        this.factorT = (Math.exp(lambda) - 1) * (maxBackoffT / lambda);
    }

    /**
     * {@inheritDoc}
     */
    public long generateDelay()
    {
        return (long)generateNewOptimalDelay();
    }

    /**
     * Generate a new randomized delay value in the units of backoff and {@code gRtt}.
     *
     * @return delay in units of backoff and RTT
     */
    public double generateNewOptimalDelay()
    {
        final double x = uniformRandom(randMax) + baseX;

        return constantT * Math.log(x * factorT);
    }

    /**
     * Return the estimated number of feedback messages per RTT.
     *
     * @return number of estimated feedback messages in units of backoff and RTT
     */
    public double calculatedN()
    {
        return calculatedN;
    }

    /**
     * Return uniform random value in the range 0..max
     *
     * @param max of the random range
     * @return random value
     */
    public static double uniformRandom(final double max)
    {
        return Math.random() * max;
    }
}
