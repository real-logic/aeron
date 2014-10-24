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
