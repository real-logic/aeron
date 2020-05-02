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

#ifndef AERON_CLIENT_TEST_UTILS_H
#define AERON_CLIENT_TEST_UTILS_H

#include <string>

extern "C"
{
#include "util/aeron_fileutil.h"
#include "util/aeron_error.h"
#include "aeron_log_buffer.h"
}

#define FILE_PAGE_SIZE (4 * 1024)

namespace aeron
{
namespace test
{

std::string tempFileName()
{
    char filename[AERON_MAX_PATH];

    aeron_temp_filename(filename, sizeof(filename));
    return std::string(filename);
}

void createLogFile(std::string &filename)
{
    aeron_mapped_file_t mappedFile = {
        nullptr,
        AERON_LOGBUFFER_TERM_MIN_LENGTH * 3 + AERON_LOGBUFFER_META_DATA_LENGTH };

    if (aeron_map_new_file(&mappedFile, filename.c_str(), false) < 0)
    {
        throw std::runtime_error("could not create log file: " + std::string(aeron_errmsg()));
    }

    auto metadata = reinterpret_cast<aeron_logbuffer_metadata_t *>((uint8_t *)mappedFile.addr +
        (mappedFile.length - AERON_LOGBUFFER_META_DATA_LENGTH));

    metadata->term_length = AERON_LOGBUFFER_TERM_MIN_LENGTH;
    metadata->page_size = FILE_PAGE_SIZE;

    aeron_unmap(&mappedFile);
}

}
}

#endif //AERON_CLIENT_TEST_UTILS_H
