/*
 * Copyright 2014-2020 Real Logic Limited.
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

import io.aeron.Aeron;
import io.aeron.exceptions.AeronException;

/**
 * Exception raised when communicating with the {@link AeronArchive}.
 */
public class ArchiveException extends AeronException
{
    /**
     * Generic archive error with detail likely in the message.
     */
    public static final int GENERIC = 0;

    /**
     * An active listing of recordings is currently in operation on the session.
     */
    public static final int ACTIVE_LISTING = 1;

    /**
     * The recording is currently active so the requested operation is not valid.
     */
    public static final int ACTIVE_RECORDING = 2;

    /**
     * A subscription is currently active for the requested channel and stream id which would clash.
     */
    public static final int ACTIVE_SUBSCRIPTION = 3;

    /**
     * The subscription for the requested operation is not known to the archive.
     */
    public static final int UNKNOWN_SUBSCRIPTION = 4;

    /**
     * The recording identity for the operation is not know to the archive.
     */
    public static final int UNKNOWN_RECORDING = 5;

    /**
     * The replay identity for the operation is not know to the archive.
     */
    public static final int UNKNOWN_REPLAY = 6;

    /**
     * The archive has reached its maximum concurrent replay sessions.
     */
    public static final int MAX_REPLAYS = 7;

    /**
     * The archive has reached its maximum concurrent recording sessions.
     */
    public static final int MAX_RECORDINGS = 8;

    /**
     * The extend recording operation is not valid for the existing recording.
     */
    public static final int INVALID_EXTENSION = 9;

    /**
     * The archive is rejecting the session because of failed authentication.
     */
    public static final int AUTHENTICATION_REJECTED = 10;

    private final int errorCode;
    private final long correlationId;

    public ArchiveException()
    {
        super();
        errorCode = GENERIC;
        correlationId = Aeron.NULL_VALUE;
    }

    public ArchiveException(final String message)
    {
        super(message);
        errorCode = GENERIC;
        correlationId = Aeron.NULL_VALUE;
    }

    public ArchiveException(final String message, final int errorCode)
    {
        super(message);
        this.errorCode = errorCode;
        correlationId = Aeron.NULL_VALUE;
    }

    public ArchiveException(final String message, final Throwable cause, final int errorCode)
    {
        super(message, cause);
        this.errorCode = errorCode;
        correlationId = Aeron.NULL_VALUE;
    }

    public ArchiveException(final String message, final int errorCode, final long correlationId)
    {
        super(message);
        this.errorCode = errorCode;
        this.correlationId = correlationId;
    }

    public ArchiveException(final String message, final Category category)
    {
        super(message, category);
        this.errorCode = GENERIC;
        this.correlationId = Aeron.NULL_VALUE;
    }

    public ArchiveException(final String message, final long correlationId, final Category category)
    {
        super(message, category);
        this.errorCode = GENERIC;
        this.correlationId = correlationId;
    }

    public ArchiveException(
        final String message, final int errorCode, final long correlationId, final Category category)
    {
        super(message, category);
        this.errorCode = errorCode;
        this.correlationId = correlationId;
    }

    /**
     * Error code providing more detail into what went wrong.
     *
     * @return code providing more detail into what went wrong.
     */
    public int errorCode()
    {
        return errorCode;
    }

    /**
     * Optional correlation-id associated with a control protocol request. Will be {@link Aeron#NULL_VALUE} if
     * not set.
     *
     * @return correlation-id associated with a control protocol request.
     */
    public long correlationId()
    {
        return correlationId;
    }
}
