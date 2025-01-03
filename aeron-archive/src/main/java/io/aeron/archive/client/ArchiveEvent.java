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
package io.aeron.archive.client;

import io.aeron.exceptions.AeronEvent;
import io.aeron.exceptions.AeronException;

/**
 * A means to capture an Archive event of significance that does not require a stack trace, so it can be lighter-weight
 * and take up less space in a {@link org.agrona.concurrent.errors.DistinctErrorLog}.
 */
public class ArchiveEvent extends AeronEvent
{
    private static final long serialVersionUID = 6814756736866124884L;

    /**
     * Archive event with provided message and {@link io.aeron.exceptions.AeronException.Category#WARN}.
     *
     * @param message to detail the event.
     */
    public ArchiveEvent(final String message)
    {
        super(message, AeronException.Category.WARN);
    }

    /**
     * Archive event with provided message and {@link io.aeron.exceptions.AeronException.Category}.
     *
     * @param message  to detail the event.
     * @param category of the event.
     */
    public ArchiveEvent(final String message, final AeronException.Category category)
    {
        super(message, category);
    }
}
