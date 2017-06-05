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
#ifndef INCLUDED_AERON_UTIL_STRING_UTIL_FILE__
#define INCLUDED_AERON_UTIL_STRING_UTIL_FILE__

#include <string>
#include <sstream>
#include <iostream>
#include <type_traits>
#include <iomanip>
#include <locale>
#include <stdarg.h>
#include "Exceptions.h"

namespace aeron { namespace util {

inline std::string trimWSLeft (std::string str, const char* wschars = " \t")
{
    str.erase(0,str.find_first_not_of(wschars));
    return str;
}

inline std::string trimWSRight (std::string str, const char* wschars = " \t")
{
    str.erase(str.find_last_not_of(wschars)+1);
    return str;
}

inline std::string trimWSBoth (std::string str, const char* wschars = " \t")
{
    return trimWSLeft(trimWSRight(str, wschars), wschars);
}

template<class valueType>
valueType parse (const std::string& input)
{
    std::string str = trimWSBoth(input);

    std::istringstream stream (str);
    valueType value;

    if (std::is_integral<valueType>::value && input.length() > 2 && input[0] == '0' && (input[1] == 'x' || input[1] == 'X'))
        stream >> std::hex >> value;
    else
        stream >> value;

    // if we failed extract an valid value or we didnt use up all the chars then throw an error
    if (stream.fail() || !stream.eof())
    {
        throw ParseException(std::string("Failed to parse: ") + input, SOURCEINFO);
    }

    return value;
}

template <typename value_t>
inline std::string toString (const value_t& value)
{
    std::stringstream stream;
    stream << value;
    return stream.str();
}

template <typename value_t>
inline std::string toStringWithCommas(const value_t& value)
{
    std::stringstream stream;

    stream.imbue(std::locale(""));
    stream << std::fixed << value;
    return stream.str();
}

inline std::string strPrintf (const char* format, ...)
{
    const int BUFFER_SIZE = 128;
    char buffer[BUFFER_SIZE];

    va_list argp;
    va_start(argp, format);
    int len = vsnprintf(buffer, BUFFER_SIZE, format, argp);
    va_end(argp);

    if (len >= BUFFER_SIZE)
    {
        len++;
        std::string output (len, ' ');

        va_start(argp, format);
        vsnprintf(&output[0], len, format, argp);
        va_end(argp);

        output.pop_back(); // remove trailing 0 char
        return output;
    }
    else
    {
        return std::string(buffer);
    }
}

namespace private_impl
{
    template <typename T>
    void concat (std::stringstream& s, T v)
    {
        s << v;
    }

    template <typename T, typename... Ts>
    void concat (std::stringstream& s, T v, Ts... vs)
    {
        s << v;
        concat(s, vs...);
    }
}

template <typename... Ts>
std::string strconcat (Ts... vs)
{
    std::stringstream s;
    private_impl::concat(s, vs...);
    return s.str();
}

inline bool continuationBarrier(const std::string& label)
{
    bool result = false;
    char response;
    std::cout << std::endl << label << " (y/n): ";
    std::cin >> response;

    if ('y' == response || 'Y' == response)
    {
        result = true;
    }

    return result;
}

template <typename T>
static T fromString (const std::string &str)
{
    std::istringstream is(str);
    T t;
    is >> t;
    return t;
}


}}

#endif
