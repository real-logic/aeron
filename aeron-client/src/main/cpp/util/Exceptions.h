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
#ifndef AERON_UTIL_EXCEPTIONS_FILE_H
#define AERON_UTIL_EXCEPTIONS_FILE_H

#include <cstdint>
#include <string>
#include <stdexcept>
#include "MacroUtil.h"

namespace aeron { namespace util {


static constexpr const char* past_prefix(const char * const prefix, const char * const filename)
{
    return *prefix == *filename ?
        past_prefix(prefix + 1, filename + 1) : *filename == '/' ? filename + 1 : filename;
}

#ifdef __PROJECT_SOURCE_DIR__
    #define __SHORT_FILE__ aeron::util::past_prefix(__PROJECT_SOURCE_DIR__,__FILE__)
#else
    #define __SHORT_FILE__ __FILE__
#endif

#ifdef _MSC_VER
    #define SOURCEINFO __FUNCTION__,  __SHORT_FILE__, __LINE__
    #if _MSC_VER >= 1900
        #define AERON_NOEXCEPT noexcept
    #else
        #define AERON_NOEXCEPT throw()
    #endif
#else
    #define SOURCEINFO  __PRETTY_FUNCTION__,  __SHORT_FILE__, __LINE__
    #define AERON_NOEXCEPT noexcept
#endif

class SourcedException : public std::exception
{
private:
    std::string m_where;
    std::string m_what;

public:
    SourcedException(const std::string &what, const std::string& function, const std::string& file, const int line) :
        m_where(function + " : " + file + " : " + std::to_string(line)),
        m_what(what)
    {
    }

    virtual const char * what() const AERON_NOEXCEPT
    {
        return m_what.c_str();
    }

    const char * where() const AERON_NOEXCEPT
    {
        return m_where.c_str();
    }
};

#define DECLARE_SOURCED_EXCEPTION(exceptionName)                \
    class exceptionName : public aeron::util::SourcedException  \
    {                                                           \
    public:                                                     \
        exceptionName(                                          \
            const std::string& what,                            \
            const std::string& function,                        \
            const std::string& file,                            \
            const int line) :                                   \
            SourcedException(what, function, file, line)        \
            {                                                   \
            }                                                   \
    }                                                           \

DECLARE_SOURCED_EXCEPTION(IOException);
DECLARE_SOURCED_EXCEPTION(FormatException);
DECLARE_SOURCED_EXCEPTION(OutOfBoundsException);
DECLARE_SOURCED_EXCEPTION(ParseException);
DECLARE_SOURCED_EXCEPTION(ElementNotFound);
DECLARE_SOURCED_EXCEPTION(IllegalArgumentException);
DECLARE_SOURCED_EXCEPTION(IllegalStateException);
DECLARE_SOURCED_EXCEPTION(DriverTimeoutException);
DECLARE_SOURCED_EXCEPTION(ConductorServiceTimeoutException);
DECLARE_SOURCED_EXCEPTION(ClientTimeoutException);
DECLARE_SOURCED_EXCEPTION(AeronException);
DECLARE_SOURCED_EXCEPTION(UnknownSubscriptionException);

class RegistrationException : public SourcedException
{
private:
    std::int32_t m_errorCode;

public:
    RegistrationException(
        std::int32_t errorCode,
        const std::string& what,
        const std::string& function,
        const std::string& file,
        const int line) :
        SourcedException(what, function, file, line),
        m_errorCode(errorCode)
    {
    }

    std::int32_t errorCode() const
    {
        return m_errorCode;
    }
};

}}
#endif
