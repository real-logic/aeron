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

/**
 * Interface for handling various error frame messages from different components in the client.
 */
public interface ErrorFrameHandler
{
    ErrorFrameHandler NO_OP = new ErrorFrameHandler()
    {
    };

    /**
     * Called when an error frame is received by the local driver. E.g. when an image is invalidated.
     *
     * @param registrationId    for the publication.
     * @param errorCode         for the error received
     * @param errorText         description of the error that occurred.
     */
    default void onPublicationError(final long registrationId, final int errorCode, final String errorText)
    {
    }
}
