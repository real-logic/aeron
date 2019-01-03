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
package io.aeron.driver.ext;

import org.agrona.concurrent.UnsafeBuffer;

import java.net.InetSocketAddress;

/**
 * Interface for loss generators.
 */
@FunctionalInterface
public interface LossGenerator
{
    /**
     * Should a frame be dropped?
     *
     * @param address The source address of the frame if inbound or the remote address if outbound.
     * @param buffer  The buffer containing the frame data.
     * @param length  The length of the frame.
     * @return true to drop, false to process
     */
    boolean shouldDropFrame(InetSocketAddress address, UnsafeBuffer buffer, int length);
}
