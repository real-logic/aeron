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
package io.aeron;

/**
 * Interface for delivery of End of Stream image notification to a {@link Subscription}.
 */
@Deprecated
@FunctionalInterface
public interface EndOfStreamHandler
{
    /**
     * Method called by Aeron to deliver notification that an {@link Image} has reached End of Stream.
     *
     * @param image that has reached End Of Stream.
     */
    void onEndOfStream(Image image);
}
