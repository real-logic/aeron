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

#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ftw.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <concurrent/aeron_mpsc_rb.h>
#include <inttypes.h>
#include "aeronmd.h"
#include "aeron_alloc.h"

inline static const char *tmp_dir()
{
#if defined(_MSC_VER)
    static char buff[MAX_PATH+1];

    if (::GetTempPath(MAX_PATH, &buff[0]) > 0)
    {
        dir = buff;
    }

    return buff;
#else
    const char *dir = "/tmp";

    if (getenv("TMPDIR"))
    {
        dir = getenv("TMPDIR");
    }

    return dir;
#endif
}

inline static const char *username()
{
    const char *username = getenv("USER");
#if (_MSC_VER)
    if (NULL == username)
    {
        username = getenv("USERNAME");
        if (NULL == username)
        {
             username = "default";
        }
    }
#else
    if (NULL == username)
    {
        username = "default";
    }
#endif
    return username;
}

int aeron_driver_context_init(aeron_driver_context_t **context)
{
    aeron_driver_context_t *_context = NULL;

    if (NULL == context)
    {
        /* TODO: EINVAL */
        return -1;
    }

    if (aeron_alloc((void **)&_context, sizeof(aeron_driver_context_t)) < 0)
    {
        return -1;
    }

    if (aeron_alloc((void **)&_context->aeron_dir, AERON_MAX_PATH) < 0)
    {
        return -1;
    }

#if defined(__linux__)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "/dev/shm/aeron-%s", username());
#elif (_MSC_VER)
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s/aeron-%s", tmp_dir(), username());
#else
    snprintf(_context->aeron_dir, AERON_MAX_PATH - 1, "%s/aeron-%s", tmp_dir(), username());
#endif

    _context->threading_mode = AERON_THREADING_MODE_DEDICATED;
    _context->dirs_delete_on_start = false;

    *context = _context;
    return 0;
}

int aeron_driver_context_close(aeron_driver_context_t *context)
{
    if (NULL == context)
    {
        /* TODO: EINVAL */
        return -1;
    }

    aeron_free((void *)context->aeron_dir);
    aeron_free(context);
    return 0;
}

static int unlink_func(const char *path, const struct stat *sb, int type_flag, struct FTW *ftw)
{
    if (remove(path) != 0)
    {
        /* TODO: change to normal error handling */
        perror(path);
    }

    return 0; /* just continue */
}

int aeron_dir_delete(const char *dirname)
{
    return nftw(dirname, unlink_func, 64, FTW_DEPTH | FTW_PHYS);
}

bool aeron_is_driver_active(const char *dirname, int64_t timeout, int64_t now, aeron_log_func_t log_func)
{
    struct stat sb;
    char buffer[AERON_MAX_PATH];
    int fd;

    if (stat(dirname, &sb) == 0 && (S_ISDIR(sb.st_mode)))
    {
        snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron directory %s exists", dirname);
        log_func(buffer);

        snprintf(buffer, sizeof(buffer) - 1, "%s/%s", dirname, AERON_CNC_FILE);
        if ((fd = open(buffer, O_RDONLY)) >= 0)
        {
            snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron CnC file %s/%s exists", dirname, AERON_CNC_FILE);
            log_func(buffer);

            if (fstat(fd, &sb) == 0)
            {
                void *cnc_mmap = mmap(NULL, (size_t)sb.st_size, PROT_READ, MAP_FILE, fd, 0);

                if (MAP_FAILED != cnc_mmap)
                {
                    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)cnc_mmap;

                    if (AERON_CNC_VERSION != metadata->cnc_version)
                    {
                        snprintf(
                            buffer,
                            sizeof(buffer) - 1,
                            "ERROR: aeron cnc file version not understood: version=%d",
                            metadata->cnc_version);
                        log_func(buffer);
                    }
                    else
                    {
                        aeron_mpsc_rb_t rb;

                        if (aeron_mpsc_rb_init(
                            &rb, aeron_cnc_to_driver_buffer(metadata), (size_t)metadata->to_driver_buffer_length) != 0)
                        {
                            snprintf(
                                buffer, sizeof(buffer) - 1, "ERROR: aeron cnc file could not init to-driver buffer");
                            log_func(buffer);
                        }
                        else
                        {
                            int64_t timestamp = aeron_mpsc_rb_consumer_heartbeat_time_value(&rb);

                            int64_t diff = now - timestamp;

                            snprintf(
                                buffer, sizeof(buffer) - 1, "INFO: Aeron toDriver consumer heartbeat is %" PRId64 " ms old", diff);
                            log_func(buffer);

                            if (diff <= timeout)
                            {
                                return true;
                            }
                        }
                    }

                    munmap(cnc_mmap, (size_t) sb.st_size);
                }
                else
                {
                    /* TODO: add error info */
                    snprintf(buffer, sizeof(buffer) - 1, "INFO: failed to mmap CnC file");
                    log_func(buffer);
                }
            }

            close(fd);
        }
    }

    return false;
}

extern uint8_t *aeron_cnc_to_driver_buffer(aeron_cnc_metadata_t *metadata);
