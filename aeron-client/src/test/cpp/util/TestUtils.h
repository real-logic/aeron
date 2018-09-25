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

#ifndef AERON_TESTUTILS_H
#define AERON_TESTUTILS_H

#if !defined(_MSC_VER)
#include <unistd.h>
#include <stdlib.h>
#endif

#include <util/Exceptions.h>

namespace aeron { namespace test {

std::string makeTempFileName()
{
#if !defined(_MSC_VER)
    char rawname[] = "/tmp/aeron-c.XXXXXXX";
    int fd = ::mkstemp(rawname);
    ::close(fd);
    ::unlink(rawname);

    return std::string(rawname);

#else
    char tmpdir[MAX_PATH+1];
    char tmpfile[MAX_PATH];

    if (::GetTempPath(MAX_PATH, &tmpdir[0]) > 0)
    {
        if (::GetTempFileName(tmpdir, TEXT("aeron-c"), 0, &tmpfile[0]) != 0)
        {
            return std::string(tmpfile);
        }
    }

    throw util::IllegalStateException("could not make unique temp filename", SOURCEINFO);
#endif
}

void throwIllegalArgumentException()
{
    throw util::IllegalArgumentException("Intentional IllegalArgumentException", SOURCEINFO);
}

}}

#endif //AERON_TESTUTILS_H
