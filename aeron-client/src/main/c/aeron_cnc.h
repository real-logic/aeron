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

#ifndef AERON_AERON_CNC_H
#define AERON_AERON_CNC_H

#include "aeron_common.h"
#include "util/aeron_fileutil.h"
#include "concurrent/aeron_mpsc_rb.h"
#include "concurrent/aeron_counters_manager.h"
#include "aeron_cnc_file_descriptor.h"

typedef struct aeron_cnc_stct
{
    aeron_mapped_file_t cnc_mmap;
    aeron_cnc_metadata_t *metadata;
    aeron_counters_reader_t counters_reader;
    char base_path[AERON_MAX_PATH];
    char filename[AERON_MAX_PATH];
}
aeron_cnc_t;

#endif //AERON_AERON_CNC_H
