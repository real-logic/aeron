/*
 * Copyright 2014 - 2015 Real Logic Ltd.
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
#include <stdio.h>
#include <stdarg.h>
#include "StringUtil.h"

namespace aeron { namespace common { namespace util {

std::string strPrintf (const char* format, ...)
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

}}}
