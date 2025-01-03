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
#ifndef AERON_ARCHIVE_ARCHIVE_EXCEPTION_H
#define AERON_ARCHIVE_ARCHIVE_EXCEPTION_H

#include "Aeron.h"

namespace aeron { namespace archive { namespace client
{

constexpr const std::int32_t ARCHIVE_ERROR_CODE_GENERIC = 0;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_ACTIVE_LISTING = 1;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_ACTIVE_RECORDING = 2;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_ACTIVE_SUBSCRIPTION = 3;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_UNKNOWN_SUBSCRIPTION = 4;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING = 5;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_UNKNOWN_REPLAY = 6;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_MAX_REPLAYS = 7;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_MAX_RECORDINGS = 8;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_INVALID_EXTENSION = 9;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_AUTHENTICATION_REJECTED = 10;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_STORAGE_SPACE = 11;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_UNKNOWN_REPLICATION = 12;
constexpr const std::int32_t ARCHIVE_ERROR_CODE_UNAUTHORISED_ACTION = 13;

/**
 * Exception raised when communicating with the AeronArchive.
 */
class ArchiveException : public SourcedException
{
private:
    std::int32_t m_errorCode = ARCHIVE_ERROR_CODE_GENERIC;
    std::int64_t m_correlationId = NULL_VALUE;

public:
    ArchiveException(
        const std::string &what,
        const std::string &function,
        const std::string &file,
        const int line) :
        SourcedException(what, function, file, line)
    {
    }

    ArchiveException(
        std::int32_t errorCode,
        const std::string &what,
        const std::string &function,
        const std::string &file,
        const int line) :
        SourcedException(what, function, file, line),
        m_errorCode(errorCode)
    {
    }

    ArchiveException(
        std::int32_t errorCode,
        std::int64_t correlationId,
        const std::string &what,
        const std::string &function,
        const std::string &file,
        const int line) :
        SourcedException(what, function, file, line),
        m_errorCode(errorCode),
        m_correlationId(correlationId)
    {
    }

    /**
     * Error code providing more detail into what went wrong.
     *
     * @return code providing more detail into what went wrong.
     */
    std::int32_t errorCode() const
    {
        return m_errorCode;
    }

    /**
     * Correlation id of request that triggered the exception.
     *
     * @return correlation id of request that triggered the exception.
     */
    std::int64_t correlationId() const
    {
        return m_correlationId;
    }
};

}}}

#endif //AERON_ARCHIVE_ARCHIVE_EXCEPTION_H
