/*
 * Copyright 2014-2017 Real Logic Ltd.
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
#ifndef INCLUDED_AERON_UTIL_EXCEPTIONS_FILE__
#define INCLUDED_AERON_UTIL_EXCEPTIONS_FILE__

#include <cstdint>
#include <string>
#include <stdexcept>
#include "MacroUtil.h"

// ==========================================================================================================================
// macro to create a const char* with the details of current source line in the format:
//    [Class::Method : d:\projects\file.cpp : 127]
// ==========================================================================================================================

namespace aeron { namespace util {

#ifdef _MSC_VER
    #define SOURCEINFO __FUNCTION__,  " : "  __FILE__  " : " TOSTRING(__LINE__)
    #if _MSC_VER >= 1900
        #define AERON_NOEXCEPT noexcept
    #else
        #define AERON_NOEXCEPT throw()
    #endif
#else
    #define SOURCEINFO  __PRETTY_FUNCTION__,  " : "  __FILE__  " : " TOSTRING(__LINE__)
    #define AERON_NOEXCEPT noexcept
#endif

class SourcedException : public std::exception
{
private:
    std::string m_where;
    std::string m_what;

public:
    SourcedException(const std::string &what, const std::string& function, const std::string& where)
            : m_where(function + where), m_what(what)
    {
    }

    virtual const char *what() const AERON_NOEXCEPT
    {
        return m_what.c_str();
    }

    const char *where() const AERON_NOEXCEPT
    {
        return m_where.c_str();
    }
};

#define DECLARE_SOURCED_EXCEPTION(exceptionName)                                            \
            class exceptionName : public aeron::util::SourcedException                      \
            {                                                                               \
                public:                                                                     \
                    exceptionName (const std::string &what, const std::string& function, const std::string& where)   \
                            : SourcedException (what, function, where)                      \
                        {}                                                                  \
            }                                                                               \

DECLARE_SOURCED_EXCEPTION (IOException);
DECLARE_SOURCED_EXCEPTION (FormatException);
DECLARE_SOURCED_EXCEPTION (OutOfBoundsException);
DECLARE_SOURCED_EXCEPTION (ParseException);
DECLARE_SOURCED_EXCEPTION (ElementNotFound);
DECLARE_SOURCED_EXCEPTION (IllegalArgumentException);
DECLARE_SOURCED_EXCEPTION (IllegalStateException);
DECLARE_SOURCED_EXCEPTION (DriverTimeoutException);
DECLARE_SOURCED_EXCEPTION (ConductorServiceTimeoutException);
DECLARE_SOURCED_EXCEPTION (UnknownSubscriptionException);

class RegistrationException : public SourcedException
{
private:
    std::int32_t m_errorCode;

public:
    RegistrationException(
        std::int32_t errorCode, const std::string &what, const std::string& function, const std::string& where) :
        SourcedException(what, function, where), m_errorCode(errorCode)
    {
    }

    std::int32_t errorCode() const
    {
        return m_errorCode;
    }
};

}}
#endif
