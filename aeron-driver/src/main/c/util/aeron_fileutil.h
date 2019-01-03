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

#ifndef AERON_FILEUTIL_H
#define AERON_FILEUTIL_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include "concurrent/aeron_logbuffer_descriptor.h"

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

typedef uint64_t (*aeron_usable_fs_space_func_t)(const char *path);

uint64_t aeron_usable_fs_space(const char *path);
uint64_t aeron_usable_fs_space_disabled(const char *path);

#define AERON_LOG_META_DATA_SECTION_INDEX (AERON_LOGBUFFER_PARTITION_COUNT)

typedef struct aeron_mapped_raw_log_stct
{
    aeron_mapped_buffer_t term_buffers[AERON_LOGBUFFER_PARTITION_COUNT];
    aeron_mapped_buffer_t log_meta_data;
    aeron_mapped_file_t mapped_file;
    size_t term_length;
}
aeron_mapped_raw_log_t;

#define AERON_PUBLICATIONS_DIR "publications"
#define AERON_IMAGES_DIR "images"

int aeron_ipc_publication_location(
    char *dst,
    size_t length,
    const char *aeron_dir,
    int32_t session_id,
    int32_t stream_id,
    int64_t correlation_id);

int aeron_network_publication_location(
    char *dst,
    size_t length,
    const char *aeron_dir,
    const char *channel_canonical_form,
    int32_t session_id,
    int32_t stream_id,
    int64_t correlation_id);

int aeron_publication_image_location(
    char *dst,
    size_t length,
    const char *aeron_dir,
    const char *channel_canonical_form,
    int32_t session_id,
    int32_t stream_id,
    int64_t correlation_id);

typedef int (*aeron_map_raw_log_func_t)(aeron_mapped_raw_log_t *, const char *, bool, uint64_t, uint64_t);
typedef int (*aeron_map_raw_log_close_func_t)(aeron_mapped_raw_log_t *, const char *filename);

int aeron_map_raw_log(
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool use_sparse_files,
    uint64_t term_length,
    uint64_t page_size);

int aeron_map_raw_log_close(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename);

#endif //AERON_FILEUTIL_H
