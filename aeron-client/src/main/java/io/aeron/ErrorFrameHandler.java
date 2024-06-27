/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron;

import io.aeron.status.PublicationErrorFrame;

/**
 * Interface for handling various error frame messages from different components in the client.
 */
public interface ErrorFrameHandler
{
    ErrorFrameHandler NO_OP = new ErrorFrameHandler()
    {
    };

    /**
     * Called when an error frame received by the local driver is propagated to the clients. E.g. when an image is
     * invalidated. This callback will reuse the {@link PublicationErrorFrame} instance, so data is only valid for the
     * lifetime of the callback. If the user needs to pass the data onto another thread or hold in another location for
     * use later, then the user needs to make use of the {@link PublicationErrorFrame#clone()} method to create a copy
     * for their own use.
     * <p>
     * This callback will be executed on the client conductor thread, similar to image availability notifications.
     *
     * @param errorFrame contain the data from the error frame received by the publication.
     */
    default void onPublicationError(final PublicationErrorFrame errorFrame)
    {
    }
}
