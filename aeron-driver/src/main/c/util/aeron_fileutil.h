/*
 * Copyright 2014 - 2017 Real Logic Ltd.
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

#ifndef AERON_AERON_FILEUTIL_H
#define AERON_AERON_FILEUTIL_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>

typedef struct aeron_mapped_file_stct
{
    void *addr;
    size_t length;
}
aeron_mapped_file_t;

typedef struct aeron_mapped_buffer_stct
{
    uint8_t *addr;
    size_t length;
}
aeron_mapped_buffer_t;

int aeron_map_new_file(aeron_mapped_file_t *mapped_file, const char *path, bool fill_with_zeroes);
int aeron_map_existing_file(aeron_mapped_file_t *mapped_file, const char *path);
int aeron_unmap(aeron_mapped_file_t *mapped_file);

#define AERON_PARTITION_COUNT (3)

/* TODO: replace with actual value */
#define AERON_LOG_META_DATA_LENGTH (1024)
#define AERON_LOG_META_DATA_SECTION_INDEX (AERON_PARTITION_COUNT)
#define AERON_LOG_COMPUTE_LOG_LENGTH(term_length) (AERON_PARTITION_COUNT * term_length + AERON_LOG_META_DATA_LENGTH)

typedef struct aeron_mapped_raw_log_stct
{
    aeron_mapped_buffer_t term_buffers[AERON_PARTITION_COUNT];
    aeron_mapped_buffer_t log_meta_data;
    aeron_mapped_file_t mapped_files[AERON_PARTITION_COUNT + 1];
    size_t num_mapped_files;
}
aeron_mapped_raw_log_t;

int aeron_map_raw_log(
    aeron_mapped_raw_log_t *mapped_raw_log, const char *path, bool use_sparse_files, uint64_t term_length);
int aeron_map_raw_log_close(aeron_mapped_raw_log_t *mapped_raw_log);

#endif //AERON_AERON_FILEUTIL_H
