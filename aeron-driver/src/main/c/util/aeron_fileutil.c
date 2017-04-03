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

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <string.h>
#include "util/aeron_fileutil.h"

#define BLOCK_SIZE (4 * 1024)

int aeron_map_new_file(void **addr, const char *path, size_t length, bool fill_with_zeroes)
{
    int fd, result = -1;

    if ((fd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR)) >= 0)
    {
        if (lseek(fd, (off_t)(length - 1), SEEK_SET) >= 0)
        {
            if (write(fd, "", 1) > 0)
            {
                if (fill_with_zeroes)
                {
                    char block[BLOCK_SIZE];

                    memset(block, 0, sizeof(block));
                    const size_t blocks = length / sizeof(block);
                    const size_t block_remainder = length % sizeof(block);

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

                void *file_mmap = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                close(fd);

                if (MAP_FAILED != file_mmap)
                {
                    *addr = file_mmap;
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

