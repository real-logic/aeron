/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.archive.client;

import io.aeron.archive.codecs.ControlResponseCode;

/**
 * Listener for responses to requests made on the archive control channel and async notification of errors which may
 * happen later.
 */
@FunctionalInterface
public interface ControlEventListener
{
    /**
     * An event has been received from the Archive in response to a request with a given correlation id.
     *
     * @param controlSessionId of the originating session.
     * @param correlationId    of the associated request.
     * @param relevantId       of the object to which the response applies.
     * @param code             for the response status.
     * @param errorMessage     when is set if the response code is not OK.
     */
    void onResponse(
        long controlSessionId,
        long correlationId,
        long relevantId,
        ControlResponseCode code,
        String errorMessage);
}
