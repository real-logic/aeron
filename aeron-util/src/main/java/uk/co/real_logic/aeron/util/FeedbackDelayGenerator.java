/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron.util;

/**
 * Feedback delay used for NAKs as well as for some retransmission use cases.
 *
 * Generates delay based on Optimal Multicast Feedback
 * http://tools.ietf.org/html/rfc5401#page-13
 *
 * T_maxBackoff is max interval for delay
 *
 * C version:
 *
 * double RandomBackoff(double T_maxBackoff, double groupSize)
 * {
 *     double lambda = log(groupSize) + 1;
 *     double x = UniformRand(lambda/T_maxBackoff) + lambda / (T_maxBackoff*(exp(lambda)-1));
 *     return ((T_maxBackoff/lambda) * log(x*(exp(lambda)-1)*(T_maxBackoff/lambda)));
 * }
 *
 * where UniformRand(x) is uniform distribution from 0..max
 *
 * In this implementations calculation:
 * - the groupSize is a constant (could be configurable as system property)
 * - T_maxBackoff is a constant (could be configurable as system property)
 * - GRTT is a constant (could be configurable as a system property)
 *
 * N (the expected number of feedback messages per RTT) is
 *   N = exp(1.2 * L / (2*T_maxBackoff/GRTT))
 *
 * Assumptions:
 * T_maxBackoff = K * GRTT (K >= 1)
 *
 * Recommended K:
 * - K = 4 for situations where responses come from multiple places (i.e. for NAKs, multiple retransmitters)
 * - K = 6 for situations where responses come from single places (i.e. for NAKs, source only retransmit)
 */
public class FeedbackDelayGenerator
{
    private final double lambda;
    private final double groupSize;
    private final double T_maxBackoff;
    private final double gRtt;
    private final double calculatedN;

    private final double randMax;
    private final double baseX;
    private final double constantT;
    private final double factorT;

    /**
     * Create new feedback delay generator based on estimates. Pre-calculating some paramters upfront.
     *
     * {@code T_maxBackoff} and {@code gRtt} must be expressed in the same units.
     *
     * @param T_maxBackoff of the delay inetrval
     * @param groupSize estimate
     * @param gRtt estimate
     */
    public FeedbackDelayGenerator(final double T_maxBackoff, final double groupSize, final double gRtt)
    {
        this.lambda = Math.log(groupSize) + 1;
        this.groupSize = groupSize;
        this.T_maxBackoff = T_maxBackoff;
        this.gRtt = gRtt;
        this.calculatedN = Math.exp(1.2 * lambda / (2 * T_maxBackoff / gRtt));

        // constant pieces of the calculation
        this.randMax = lambda / T_maxBackoff;
        this.baseX = lambda / (T_maxBackoff * (Math.exp(lambda) - 1));
        this.constantT = T_maxBackoff / lambda;
        this.factorT = (Math.exp(lambda) - 1) * (T_maxBackoff / lambda);
    }

    /**
     * Generate a new randomized delay value in the units of backoff and {@code gRtt}.
     *
     * @return delay in units of backoff and RTT
     */
    public double generateNewDelay()
    {
        double x = uniformRandom(randMax) + baseX;

        return (constantT * Math.log(x * factorT));
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
