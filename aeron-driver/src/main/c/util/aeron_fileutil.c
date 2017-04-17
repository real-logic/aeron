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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>
#include "util/aeron_fileutil.h"

#define BLOCK_SIZE (4 * 1024)

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

int aeron_map_new_file(aeron_mapped_file_t *mapped_file, const char *path, bool fill_with_zeroes)
{
    int fd, result = -1;

    if ((fd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR)) >= 0)
    {
        if (lseek(fd, (off_t)(mapped_file->length - 1), SEEK_SET) >= 0)
        {
            if (write(fd, "", 1) > 0)
            {
                if (fill_with_zeroes)
                {
                    char block[BLOCK_SIZE];

                    memset(block, 0, sizeof(block));
                    const size_t blocks = mapped_file->length / sizeof(block);
                    const size_t block_remainder = mapped_file->length % sizeof(block);

                    for (size_t i = 0; i < blocks; i++)
                    {
                        if (write(fd, block, sizeof(block)) < (ssize_t)sizeof(block))
                        {
                            /* TODO: error */
                            close(fd);
                            return -1;
                        }
                    }

                    if (block_remainder > 0)
                    {
                        if (write(fd, block, block_remainder) < (ssize_t)block_remainder)
                        {
                            /* TODO: error */
                            close(fd);
                            return -1;
                        }
                    }
                }

                void *file_mmap = mmap(NULL, mapped_file->length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                close(fd);

                if (MAP_FAILED != file_mmap)
                {
                    mapped_file->addr = file_mmap;
                    result = 0;
                }
                else
                {
                    /* TODO: error */
                }
            }
            else
            {
                close(fd);
            }
        }
        else
        {
            close(fd);
        }
    }
    else
    {
        /* TODO: grab error */
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
                /* TODO: error */
            }
        }

        close(fd);
    }
    else
    {
        /* TODO: grab error */
    }

    return result;
}

inline static void aeron_allocate_pages(uint8_t *base, size_t length)
{
    for (size_t i = 0; i < length; i += BLOCK_SIZE)
    {
        *(base + i) = 0;
    }
}

int aeron_map_raw_log(
    aeron_mapped_raw_log_t *mapped_raw_log, const char *path, bool use_sparse_files, uint64_t term_length)
{
    int fd, result = -1;
    uint64_t log_length = AERON_LOG_COMPUTE_LOG_LENGTH(term_length);

    if ((fd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR)) >= 0)
    {
        if (log_length <= INT32_MAX)
        {
            mapped_raw_log->num_mapped_files = 1;
            mapped_raw_log->mapped_files[0].length = log_length;
            mapped_raw_log->mapped_files[0].addr = NULL;

            int mmap_result = aeron_mmap(&mapped_raw_log->mapped_files[0], fd, 0);
            close(fd);

            if (mmap_result < 0)
            {
                return -1;
            }

            if (!use_sparse_files)
            {
                aeron_allocate_pages(mapped_raw_log->mapped_files[0].addr, log_length);
            }

            for (size_t i = 0; i < AERON_PARTITION_COUNT; i++)
            {
                mapped_raw_log->term_buffers[i].addr =
                    (uint8_t *)mapped_raw_log->mapped_files[0].addr + (i * term_length);
                mapped_raw_log->term_buffers[i].length = term_length;
            }

            mapped_raw_log->log_meta_data.addr =
                (uint8_t *)mapped_raw_log->mapped_files[0].addr + (log_length - AERON_LOG_META_DATA_LENGTH);
            mapped_raw_log->log_meta_data.length = AERON_LOG_META_DATA_LENGTH;

            result = 0;
        }
        else
        {
            mapped_raw_log->num_mapped_files = 0;
            int mmap_result = -1;

            for (size_t i = 0; i < AERON_PARTITION_COUNT; i++)
            {
                mapped_raw_log->mapped_files[i].length = term_length;
                mapped_raw_log->mapped_files[i].addr = NULL;
                mmap_result = aeron_mmap(&mapped_raw_log->mapped_files[i], fd, (off_t)(i * term_length));

                if (mmap_result < 0)
                {
                    break;
                }

                mapped_raw_log->num_mapped_files++;

                if (!use_sparse_files)
                {
                    aeron_allocate_pages(mapped_raw_log->mapped_files[i].addr, log_length);
                }

                mapped_raw_log->term_buffers[i].addr = (uint8_t *)mapped_raw_log->mapped_files[1].addr;
                mapped_raw_log->term_buffers[i].length = term_length;
            }

            mapped_raw_log->mapped_files[AERON_LOG_META_DATA_SECTION_INDEX].length = AERON_LOG_META_DATA_LENGTH;
            mapped_raw_log->mapped_files[AERON_LOG_META_DATA_SECTION_INDEX].addr = NULL;

            mmap_result = (mmap_result < 0) ? -1 :
                aeron_mmap(
                    &mapped_raw_log->mapped_files[AERON_LOG_META_DATA_SECTION_INDEX],
                    fd,
                    AERON_PARTITION_COUNT * term_length);

            close(fd);

            if (mmap_result < 0)
            {
                for (size_t i = 0; i < mapped_raw_log->num_mapped_files; i++)
                {
                    if (NULL != mapped_raw_log->mapped_files[i].addr)
                    {
                        munmap(mapped_raw_log->mapped_files[i].addr, mapped_raw_log->mapped_files[i].length);
                        mapped_raw_log->mapped_files[i].addr = NULL;
                    }
                }

                return -1;
            }

            mapped_raw_log->num_mapped_files++;

            mapped_raw_log->log_meta_data.addr =
                (uint8_t *)mapped_raw_log->mapped_files[AERON_LOG_META_DATA_SECTION_INDEX].addr;
            mapped_raw_log->log_meta_data.length = AERON_LOG_META_DATA_LENGTH;

            result = 0;
        }
    }
    else
    {
        /* TODO: grab error */
    }

    return result;
}

int aeron_map_raw_log_close(aeron_mapped_raw_log_t *mapped_raw_log)
{
    int result = 0;

    for (size_t i = 0; i < mapped_raw_log->num_mapped_files; i++)
    {
        if (mapped_raw_log->mapped_files[i].addr != NULL)
        {
            if ((result = munmap(mapped_raw_log->mapped_files[i].addr, mapped_raw_log->mapped_files[i].length)) < 0)
            {
                break;
            }

            mapped_raw_log->mapped_files[i].addr = NULL;
        }
    }

    return result;
}
