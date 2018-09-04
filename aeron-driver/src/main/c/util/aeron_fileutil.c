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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>
#include <sys/statvfs.h>
#include <stdio.h>
#include <inttypes.h>
#include <errno.h>
#include "util/aeron_fileutil.h"
#include "aeron_error.h"

#define AERON_BLOCK_SIZE (4 * 1024)

inline static int aeron_mmap(aeron_mapped_file_t *mapping, int fd, off_t offset)
{
    mapping->addr = mmap(NULL, mapping->length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);

    return (MAP_FAILED == mapping->addr) ? -1 : 0;
}

int aeron_unmap(aeron_mapped_file_t *mapped_file)
{
    int result = 0;

    if (NULL != mapped_file->addr)
    {
        result = munmap(mapped_file->addr, mapped_file->length);
    }

    return result;
}

inline static void aeron_touch_pages(uint8_t *base, size_t length, size_t page_size)
{
    for (size_t i = 0; i < length; i += page_size)
    {
        *(base + i) = 0;
    }
}

int aeron_fallocate(int fd, off_t length, bool fill_with_zeroes)
{
#if defined(HAVE_FALLOCATE)
    int mode = 0;

#if defined(FALLOC_FL_ZERO_RANGE)
    mode = (fill_with_zeroes ? FALLOC_FL_ZERO_RANGE : 0);
#endif
    if (fallocate(fd, mode, 0, length) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
        return -1;
    }
#else
    if (ftruncate(fd, length) < 0)
    {
        int errcode = errno;

        aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
        return -1;
    }
#endif
    if (fill_with_zeroes)
    {
        // TODO: finish
    }

    return 0;
}

int aeron_map_new_file(aeron_mapped_file_t *mapped_file, const char *path, bool fill_with_zeroes)
{
    int fd, result = -1;

    if ((fd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR)) >= 0)
    {
        if (ftruncate(fd, (off_t )mapped_file->length) >= 0)
        {
            void *file_mmap = mmap(NULL, mapped_file->length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
            close(fd);

            if (MAP_FAILED != file_mmap)
            {
                if (fill_with_zeroes)
                {
                    aeron_touch_pages(file_mmap, mapped_file->length, AERON_BLOCK_SIZE);
                }

                mapped_file->addr = file_mmap;
                result = 0;
            }
            else
            {
                int errcode = errno;

                aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
            }
        }
        else
        {
            int errcode = errno;

            aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
        }
    }
    else
    {
        int errcode = errno;

        aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
    }

    return result;
}

int aeron_map_existing_file(aeron_mapped_file_t *mapped_file, const char *path)
{
    struct stat sb;
    int fd, result = -1;

    if ((fd = open(path, O_RDWR)) >= 0)
    {
        if (fstat(fd, &sb) == 0)
        {
            mapped_file->length = (size_t)sb.st_size;

            void *file_mmap = mmap(NULL, mapped_file->length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

            if (MAP_FAILED != file_mmap)
            {
                mapped_file->addr = file_mmap;
                result = 0;
            }
            else
            {
                int errcode = errno;

                aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
            }
        }
        else
        {
            int errcode = errno;

            aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
        }

        close(fd);
    }
    else
    {
        int errcode = errno;

        aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
    }

    return result;
}

uint64_t aeron_usable_fs_space(const char *path)
{
    struct statvfs vfs;
    uint64_t result = 0;

    if (statvfs(path, &vfs) == 0)
    {
        result = vfs.f_bsize * vfs.f_bavail;
    }

    return result;
}

uint64_t aeron_usable_fs_space_disabled(const char *path)
{
    return UINT64_MAX;
}

/*
 * stream location:
 * dir/channel-sessionId(hex)-streamId(hex)-correlationId(hex).logbuffer
 */
int aeron_ipc_publication_location(
    char *dst,
    size_t length,
    const char *aeron_dir,
    int32_t session_id,
    int32_t stream_id,
    int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_PUBLICATIONS_DIR "/ipc-%" PRIx32 "-%" PRIx32 "-%" PRIx64 ".logbuffer",
        aeron_dir, session_id, stream_id, correlation_id);
}

int aeron_network_publication_location(
    char *dst,
    size_t length,
    const char *aeron_dir,
    const char *channel_canonical_form,
    int32_t session_id,
    int32_t stream_id,
    int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_PUBLICATIONS_DIR "/%s-%" PRIx32 "-%" PRIx32 "-%" PRIx64 ".logbuffer",
        aeron_dir, channel_canonical_form, session_id, stream_id, correlation_id);
}

int aeron_publication_image_location(
    char *dst,
    size_t length,
    const char *aeron_dir,
    const char *channel_canonical_form,
    int32_t session_id,
    int32_t stream_id,
    int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_IMAGES_DIR "/%s-%" PRIx32 "-%" PRIx32 "-%" PRIx64 ".logbuffer",
        aeron_dir, channel_canonical_form, session_id, stream_id, correlation_id);
}

int aeron_map_raw_log(
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool use_sparse_files,
    uint64_t term_length,
    uint64_t page_size)
{
    int fd, result = -1;
    uint64_t log_length = aeron_logbuffer_compute_log_length(term_length, page_size);

    if ((fd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR)) >= 0)
    {
        if (ftruncate(fd, (off_t)log_length) >= 0)
        {
            mapped_raw_log->mapped_file.length = log_length;
            mapped_raw_log->mapped_file.addr = NULL;

            int mmap_result = aeron_mmap(&mapped_raw_log->mapped_file, fd, 0);
            close(fd);

            if (mmap_result < 0)
            {
                int errcode = errno;

                aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
                return -1;
            }

            if (!use_sparse_files)
            {
                aeron_touch_pages(mapped_raw_log->mapped_file.addr, log_length, page_size);
            }

            for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
            {
                mapped_raw_log->term_buffers[i].addr =
                    (uint8_t *) mapped_raw_log->mapped_file.addr + (i * term_length);
                mapped_raw_log->term_buffers[i].length = term_length;
            }

            mapped_raw_log->log_meta_data.addr =
                (uint8_t *) mapped_raw_log->mapped_file.addr +
                    (log_length - AERON_LOGBUFFER_META_DATA_LENGTH);
            mapped_raw_log->log_meta_data.length = AERON_LOGBUFFER_META_DATA_LENGTH;

            mapped_raw_log->term_length = term_length;

            result = 0;
        }
        else
        {
            int errcode = errno;

            aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
            close(fd);
        }
    }
    else
    {
        int errcode = errno;

        aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
    }

    return result;
}

int aeron_map_raw_log_close(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename)
{
    int result = 0;

    if (mapped_raw_log->mapped_file.addr != NULL)
    {
        if ((result = munmap(mapped_raw_log->mapped_file.addr, mapped_raw_log->mapped_file.length)) < 0)
        {
            return -1;
        }

        if (NULL != filename && remove(filename) < 0)
        {
            int errcode = errno;

            aeron_set_err(errcode, "%s:%d: %s", __FILE__, __LINE__, strerror(errcode));
            return -1;
        }

        mapped_raw_log->mapped_file.addr = NULL;
    }

    return result;
}
