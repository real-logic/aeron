/*
 * Copyright 2014-2022 Real Logic Limited.
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
package io.aeron.driver.media;

import java.net.InetSocketAddress;

/**
 * State tracking for a connection endpoint to an image from a transport.
 */
public final class ImageConnection
{
    /**
     * Time of the last observed activity on this connection for tracking liveness.
     */
    public long timeOfLastActivityNs;

    /**
     * Time of the last observed from the source.
     */
    public long timeOfLastFrameNs;

    /**
     * Is the end of the stream from source been observed.
     */
    public boolean isEos;

    /**
     * Control address for the source.
     */
    public final InetSocketAddress controlAddress;

    /**
     * Construct a representation of a connection to an image.
     *
     * @param timeOfLastActivityNs seen on this image.
     * @param controlAddress for the source of the image.
     */
    public ImageConnection(final long timeOfLastActivityNs, final InetSocketAddress controlAddress)
    {
        this.timeOfLastActivityNs = timeOfLastActivityNs;
        this.controlAddress = controlAddress;
    }
}
