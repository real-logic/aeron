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
package uk.co.real_logic.aeron;

import java.util.stream.IntStream;

/**
 * Encapsulation of I/O driver
 *
 */
public final class Aeron
{

    // factory methods
    /**
     * Creates an I/O driver associated with this Aeron instance that can be used to create sources and receivers on
     * @param builder of the I/O driver and Aeron configuration or null for default configuration
     * @return Aeron instance
     */
    public static Aeron newSingleIoDriver(final Builder builder)
    {
        return new Aeron(builder);
    }

    /**
     * Creates multiple I/O drivers associated with multiple Aeron instances that can be used to create sources and receivers
     * @param builders of the I/O drivers
     * @return array of Aeron instances
     */
    public static Aeron[] newMultipleIoDrivers(final Builder[] builders)
    {
        final Aeron[] aerons = new Aeron[builders.length];

        IntStream.range(0, builders.length - 1).forEach((i) -> aerons[i] = new Aeron(builders[i]));
        return aerons;
    }

    /**
     * Create a new source that is to send to {@link uk.co.real_logic.aeron.Destination}.
     *
     * A unique, random, session ID will be generated for the source if the builder does not
     * set it. If the builder sets the Session ID, then it will be checked for conflicting with existing session Ids.
     *
     * @param builder for source options, etc.
     * @return new source
     */
    public Source newSource(final Source.Builder builder)
    {
        return new Source(this, builder);
    }

    /**
     * Create a new source that is to send to {@link Destination}
     * @param destination address to send all data to
     * @return new source
     */
    public Source newSource(final Destination destination)
    {
        return new Source(this, new Source.Builder().desintation(destination));
    }

    /**
     * Create a new source that will listen on {@link uk.co.real_logic.aeron.Destination}
     * @param builder builder for receiver options.
     * @return new receiver
     */
    public Receiver newReceiver(final Receiver.Builder builder)
    {
        return new Receiver(this, builder);
    }

    public static class Builder
    {
        Builder()
        {
        }
    }

    private Aeron(final Builder builder)
    {
    }
}
