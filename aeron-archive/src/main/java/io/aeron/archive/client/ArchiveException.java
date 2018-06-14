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
package io.aeron.archive.client;

import io.aeron.exceptions.AeronException;

/**
 * Exception raised when communicating with the {@link AeronArchive}.
 */
public class ArchiveException extends AeronException
{
    public static final int GENERIC = 0;
    public static final int ACTIVE_LISTING = 1;
    public static final int ACTIVE_RECORDING = 2;
    public static final int ACTIVE_SUBSCRIPTION = 3;
    public static final int UNKNOWN_SUBSCRIPTION = 4;
    public static final int UNKNOWN_RECORDING = 5;
    public static final int UNKNOWN_REPLAY = 6;
    public static final int MAX_REPLAYS = 7;
    public static final int MAX_RECORDINGS = 8;

    private final int errorCode;

    public ArchiveException()
    {
        super();
        errorCode = GENERIC;
    }

    public ArchiveException(final String message)
    {
        super(message);
        errorCode = GENERIC;
    }

    public ArchiveException(final String message, final int errorCode)
    {
        super(message);
        this.errorCode = errorCode;
    }

    public ArchiveException(final String message, final Throwable cause, final int errorCode)
    {
        super(message, cause);
        this.errorCode = errorCode;
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
}
