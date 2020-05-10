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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

#include "aeron_platform.h"
#include "aeron_error.h"
#include "aeron_fileutil.h"

#if defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)
#include <WinSock2.h>
#include <windows.h>
#include <stdint.h>
#include <stdio.h>
#include <io.h>
#include <direct.h>
#include <process.h>

#define PROT_READ  1
#define PROT_WRITE 2
#define MAP_FAILED ((void*)-1)

#define MAP_SHARED 0x01
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#define S_IRGRP 0
#define S_IWGRP 0
#define S_IROTH 0
#define S_IWOTH 0

static int aeron_mmap(aeron_mapped_file_t *mapping, int fd, off_t offset)
{
    HANDLE hmap = CreateFileMapping((HANDLE)_get_osfhandle(fd), 0, PAGE_READWRITE, 0, 0, 0);

    if (!hmap)
    {
        aeron_set_err_from_last_err_code("CreateFileMapping");
        close(fd);
        return -1;
    }

    mapping->addr = MapViewOfFileEx(hmap, FILE_MAP_WRITE, 0, offset, mapping->length, NULL);

    if (!CloseHandle(hmap))
    {
        fprintf(stderr, "unable to close file mapping handle\n");
    }

    if (!mapping->addr)
    {
        mapping->addr = MAP_FAILED;
    }

    close(fd);

    return MAP_FAILED == mapping->addr ? -1 : 0;
}

int aeron_unmap(aeron_mapped_file_t *mapped_file)
{
    if (NULL != mapped_file->addr)
    {
        return UnmapViewOfFile(mapped_file->addr) ? 0 : -1;
    }

    return 0;
}

int aeron_ftruncate(int fd, off_t length)
{
    int error = _chsize_s(fd, length);
    if (error != 0)
    {
        return -1;
    }

    return 0;
}

int aeron_mkdir(const char *path, int permission)
{
    return _mkdir(path);
}

int64_t aeron_file_length(const char *path)
{
    WIN32_FILE_ATTRIBUTE_DATA info;

    if (GetFileAttributesEx(path, GetFileExInfoStandard, &info) == 0)
    {
        return -1;
    }

    return ((int64_t)info.nFileSizeHigh << 32) | (info.nFileSizeLow);
}

uint64_t aeron_usable_fs_space(const char *path)
{
    ULARGE_INTEGER lpAvailableToCaller, lpTotalNumberOfBytes, lpTotalNumberOfFreeBytes;

    if (!GetDiskFreeSpaceExA(
        path,
        &lpAvailableToCaller,
        &lpTotalNumberOfBytes,
        &lpTotalNumberOfFreeBytes))
    {
        return 0;
    }

    return (uint64_t)lpAvailableToCaller.QuadPart;
}

int aeron_create_file(const char* path)
{
    int fd;
    int error = _sopen_s(&fd, path, _O_RDWR | _O_CREAT | _O_EXCL, _SH_DENYNO, _S_IREAD | _S_IWRITE);

    if (error != NO_ERROR)
    {
        return -1;
    }

    return fd;
}

BOOL IsDots(const WCHAR *str)
{
    if (wcscmp(str, L".") && wcscmp(str, L".."))
        return FALSE;
    return TRUE;
}

static BOOL DeleteDirectory(const WCHAR *sPath)
{
    HANDLE hFind; // file handle
    WIN32_FIND_DATAW FindFileData;

    WCHAR DirPath[AERON_MAX_PATH];
    WCHAR FileName[AERON_MAX_PATH];

    wcscpy(DirPath, sPath);
    wcscat(DirPath, L"\\*"); // searching all files
    wcscpy(FileName, sPath);
    wcscat(FileName, L"\\");

    hFind = FindFirstFileW(DirPath, &FindFileData); // find the first file
    if (hFind == INVALID_HANDLE_VALUE)
        return FALSE;
    wcscpy(DirPath, FileName);

    bool bSearch = true;
    while (bSearch)
    { // until we finds an entry
        if (FindNextFileW(hFind, &FindFileData))
        {
            if (IsDots(FindFileData.cFileName))
                continue;
            wcscat(FileName, FindFileData.cFileName);
            if ((FindFileData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY))
            {

                // we have found a directory, recurse
                if (!DeleteDirectory(FileName))
                {
                    FindClose(hFind);
                    return FALSE; // directory couldn't be deleted
                }
                RemoveDirectoryW(FileName); // remove the empty directory
                wcscpy(FileName, DirPath);
            }
            else
            {
                if (FindFileData.dwFileAttributes & FILE_ATTRIBUTE_READONLY)
                    _wchmod(FileName, _S_IWRITE); // change read-only file mode
                if (!DeleteFileW(FileName))
                { // delete the file
                    FindClose(hFind);
                    return FALSE;
                }
                wcscpy(FileName, DirPath);
            }
        }
        else
        {
            if (GetLastError() == ERROR_NO_MORE_FILES) // no more files there
                bSearch = false;
            else
            {
                // some error occured, close the handle and return FALSE
                FindClose(hFind);
                return FALSE;
            }
        }
    }
    FindClose(hFind); // closing file handle

    return RemoveDirectoryW(sPath); // remove the empty directory
}

int aeron_delete_directory(const char *dir)
{
    wchar_t ws[AERON_MAX_PATH];
    swprintf(ws, AERON_MAX_PATH, L"%hs", dir);
    if (DeleteDirectory(ws) == FALSE)
    {
        return -1;
    }
    return 0;
}

int aeron_is_directory(const char* path)
{
    const DWORD attributes = GetFileAttributes(path);
    return attributes != INVALID_FILE_ATTRIBUTES && (attributes & FILE_ATTRIBUTE_DIRECTORY);
}

#else
#include <unistd.h>
#include <sys/mman.h>
#include <sys/statvfs.h>
#include <ftw.h>
#include <stdio.h>

static int aeron_mmap(aeron_mapped_file_t *mapping, int fd, off_t offset)
{
    mapping->addr = mmap(NULL, mapping->length, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
    close(fd);

    return MAP_FAILED == mapping->addr ? -1 : 0;
}

int aeron_unmap(aeron_mapped_file_t *mapped_file)
{
    if (NULL != mapped_file->addr)
    {
        return munmap(mapped_file->addr, mapped_file->length);
    }

    return 0;
}

static int unlink_func(const char *path, const struct stat *sb, int type_flag, struct FTW *ftw)
{
    if (remove(path) != 0)
    {
        aeron_set_err_from_last_err_code("could not remove %s", path);
    }

    return 0;
}

int aeron_delete_directory(const char *dirname)
{
    return nftw(dirname, unlink_func, 64, FTW_DEPTH | FTW_PHYS);
}

int aeron_is_directory(const char* dirname)
{
    struct stat sb;
    return stat(dirname, &sb) == 0 && S_ISDIR(sb.st_mode);
}

int64_t aeron_file_length(const char *path)
{
    struct stat sb;
    return stat(path, &sb) == 0 ? sb.st_size : -1;
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

int aeron_create_file(const char* path)
{
    return open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
}
#endif

#include <inttypes.h>

#define AERON_BLOCK_SIZE (4 * 1024)

static void aeron_touch_pages(volatile uint8_t *base, size_t length, size_t page_size)
{
    for (size_t i = 0; i < length; i += page_size)
    {
        volatile uint8_t *first_page_byte = base + i;
        *first_page_byte = 0;
    }
}

int aeron_map_new_file(aeron_mapped_file_t *mapped_file, const char *path, bool fill_with_zeroes)
{
    int fd, result = -1;

    if ((fd = aeron_create_file(path)) >= 0)
    {
        if (aeron_ftruncate(fd, (off_t)mapped_file->length) >= 0)
        {
            if (aeron_mmap(mapped_file, fd, 0) == 0)
            {
                if (fill_with_zeroes)
                {
                    aeron_touch_pages(mapped_file->addr, mapped_file->length, AERON_BLOCK_SIZE);
                }

                result = 0;
            }
            else
            {
                aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
            }
        }
        else
        {
            aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        }
    }
    else
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
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

            if (aeron_mmap(mapped_file, fd, 0) == 0)
            {
                result = 0;
            }
            else
            {
                aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
            }
        }
        else
        {
            aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        }
    }
    else
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
    }

    return result;
}

uint64_t aeron_usable_fs_space_disabled(const char *path)
{
    return UINT64_MAX;
}

int aeron_ipc_publication_location(
    char *dst,
    size_t length,
    const char *aeron_dir,
    int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_PUBLICATIONS_DIR "/%" PRId64 ".logbuffer",
        aeron_dir, correlation_id);
}

int aeron_network_publication_location(
    char *dst,
    size_t length,
    const char *aeron_dir,
    int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_PUBLICATIONS_DIR "/%" PRId64 ".logbuffer",
        aeron_dir, correlation_id);
}

int aeron_publication_image_location(
    char *dst,
    size_t length,
    const char *aeron_dir,
    int64_t correlation_id)
{
    return snprintf(
        dst, length,
        "%s/" AERON_IMAGES_DIR "/%" PRId64 ".logbuffer",
        aeron_dir, correlation_id);
}

size_t aeron_temp_filename(char *filename, size_t length)
{
#if !defined(_MSC_VER)
    char rawname[] = "/tmp/aeron-c.XXXXXXX";
    int fd = mkstemp(rawname);
    close(fd);
    unlink(rawname);

    strncpy(filename, rawname, length);

    return strlen(filename);
#else
    char tmpdir[MAX_PATH+1];
    char tmpfile[MAX_PATH];

    if (GetTempPath(MAX_PATH, &tmpdir[0]) > 0)
    {
        if (GetTempFileName(tmpdir, TEXT("aeron-c"), 101, &tmpfile[0]) != 0)
        {
            strncpy(filename, tmpfile, length);
            return strlen(filename);
        }
    }

    return 0;
#endif
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

    if ((fd = open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) >= 0)
    {
        if (aeron_ftruncate(fd, (off_t)log_length) >= 0)
        {
            mapped_raw_log->mapped_file.length = log_length;
            mapped_raw_log->mapped_file.addr = NULL;

            if (aeron_mmap(&mapped_raw_log->mapped_file, fd, 0) < 0)
            {
                aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
                return -1;
            }

            if (!use_sparse_files)
            {
                aeron_touch_pages(mapped_raw_log->mapped_file.addr, log_length, page_size);
            }

            for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
            {
                mapped_raw_log->term_buffers[i].addr = (uint8_t *)mapped_raw_log->mapped_file.addr + (i * term_length);
                mapped_raw_log->term_buffers[i].length = term_length;
            }

            mapped_raw_log->log_meta_data.addr =
                (uint8_t *)mapped_raw_log->mapped_file.addr + (log_length - AERON_LOGBUFFER_META_DATA_LENGTH);
            mapped_raw_log->log_meta_data.length = AERON_LOGBUFFER_META_DATA_LENGTH;
            mapped_raw_log->term_length = term_length;

            result = 0;
        }
        else
        {
            aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        }
    }
    else
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
    }

    return result;
}

int aeron_map_existing_log(
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool pre_touch)
{
    struct stat sb;
    int fd, result = -1;

    if ((fd = open(path, O_RDWR)) >= 0)
    {
        if (fstat(fd, &sb) == 0)
        {
            mapped_raw_log->mapped_file.length = (size_t)sb.st_size;

            if (aeron_mmap(&mapped_raw_log->mapped_file, fd, 0) < 0)
            {
                aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
            }
        }
        else
        {
            aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
        }

        mapped_raw_log->log_meta_data.addr =
            (uint8_t *)mapped_raw_log->mapped_file.addr +
            (mapped_raw_log->mapped_file.length - AERON_LOGBUFFER_META_DATA_LENGTH);
        mapped_raw_log->log_meta_data.length = AERON_LOGBUFFER_META_DATA_LENGTH;

        aeron_logbuffer_metadata_t *log_meta_data = (aeron_logbuffer_metadata_t *)mapped_raw_log->log_meta_data.addr;
        size_t term_length = (size_t)log_meta_data->term_length;
        size_t page_size = (size_t)log_meta_data->page_size;

        if (aeron_logbuffer_check_term_length(term_length) < 0 ||
            aeron_logbuffer_check_page_size(page_size) < 0)
        {
            aeron_unmap(&mapped_raw_log->mapped_file);
            return -1;
        }

        mapped_raw_log->term_length = term_length;

        for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
        {
            mapped_raw_log->term_buffers[i].addr = (uint8_t *)mapped_raw_log->mapped_file.addr + (i * term_length);
            mapped_raw_log->term_buffers[i].length = term_length;
        }

        if (pre_touch)
        {
            volatile int32_t value = 0;

            for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
            {
                uint8_t *base_addr = mapped_raw_log->term_buffers[i].addr;

                for (size_t offset = 0; offset < term_length; offset += page_size)
                {
                    aeron_cmpxchg32((volatile int32_t *)(base_addr + offset), value, value);
                }
            }
        }

        result = 0;
    }
    else
    {
        aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
    }

    return result;
}

int aeron_map_raw_log_close(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename)
{
    int result = 0;

    if (mapped_raw_log->mapped_file.addr != NULL)
    {
        if ((result = aeron_unmap(&mapped_raw_log->mapped_file)) < 0)
        {
            return -1;
        }

        if (NULL != filename && remove(filename) < 0)
        {
            aeron_set_err_from_last_err_code("%s:%d", __FILE__, __LINE__);
            return -1;
        }

        mapped_raw_log->mapped_file.addr = NULL;
    }

    return result;
}
