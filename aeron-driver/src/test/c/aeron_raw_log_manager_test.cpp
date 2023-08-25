/*
 * Copyright 2014-2023 Real Logic Limited.
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

#include <array>
#include <exception>
#include <unordered_map>

#include <fcntl.h>
#include <gtest/gtest.h>

extern "C"
{
#include "aeron_driver_context.h"
#include "aeron_raw_log_manager.h"
}

#define PAGE_SIZE (4 * 1024)
#define TERM_LENGTH (1024 * 1024)



static thread_local int mmapCount = 0;
static thread_local int munmapCount = 0;

int dummy_raw_log_map(
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool use_sparse_files,
    uint64_t term_length,
    uint64_t page_size)
{
    mmapCount++;

    return aeron_raw_log_map(mapped_raw_log, path, use_sparse_files, term_length, page_size);
}

bool dummy_raw_log_free(aeron_mapped_raw_log_t *mapped_raw_log, const char *filename)
{
    munmapCount++;
    return aeron_raw_log_free(mapped_raw_log, filename);
}

class RawLogManagerTest : public testing::Test
{
public:
    RawLogManagerTest()
    {
        mmapCount = 0;
        munmapCount = 0;
        // Setup tmp folder
        aeron_alloc((void **)&context.aeron_dir, AERON_MAX_PATH);
        aeron_temp_filename(context.aeron_dir, AERON_MAX_PATH);

        char filename[AERON_MAX_PATH] = { 0 };
        aeron_mkdir(context.aeron_dir, S_IRWXU | S_IRWXG | S_IRWXO);
        aeron_file_resolve(context.aeron_dir, AERON_TMP_BUFFER_DIR, filename, sizeof(filename));
        aeron_mkdir(filename, S_IRWXU | S_IRWXG | S_IRWXO);

        context.file_page_size = PAGE_SIZE;
        context.num_raw_log_pools = 0;
        context.usable_fs_space_func = aeron_usable_fs_space;
    }

    void initLogManager()
    {
        aeron_raw_log_manager_init(&log_manager, &context, dummy_raw_log_map, dummy_raw_log_free);
    }

    ~RawLogManagerTest() override
    {
    }

    aeron_driver_context_t context;
    aeron_raw_log_manager_t log_manager;
};

TEST_F(RawLogManagerTest, unconfiguredMaintainsOldBehaviour)
{
    initLogManager();
    EXPECT_EQ(log_manager.num_pools, 0);
    EXPECT_EQ(log_manager.file_page_size, PAGE_SIZE);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&log_manager.log_return_queue), 0);

    EXPECT_EQ(mmapCount, 0);
    EXPECT_EQ(munmapCount, 0);

    aeron_mapped_raw_log_t log;
    char filename[AERON_MAX_PATH] = { 0 };
    aeron_temp_filename(filename, sizeof(filename));

    // Unconfigured raw log managers fall through to the normal raw_log_map raw_log_free immediately
    aeron_raw_log_manager_map_log(&log_manager, &log, filename, false, TERM_LENGTH, PAGE_SIZE);
    EXPECT_EQ(mmapCount, 1);
    EXPECT_EQ(munmapCount, 0);

    aeron_raw_log_manager_return_log(&log_manager, &log, filename);
    EXPECT_EQ(mmapCount, 1);
    EXPECT_EQ(munmapCount, 1);

}

class RawLogManagerWithPoolTest : public RawLogManagerTest
{
public:

    size_t const initialCapacity = 8;
    size_t const minimumReserve = 2;

    RawLogManagerWithPoolTest()
    {
        aeron_driver_context_add_raw_log_pool_config(&context, {TERM_LENGTH, initialCapacity, minimumReserve});
        initLogManager();

        // Initialisation should populate pool
        EXPECT_EQ(context.num_raw_log_pools, 1);
        EXPECT_EQ(mmapCount, initialCapacity);
        EXPECT_EQ(munmapCount, 0);

        mmapCount = 0;
    }
};

TEST_F(RawLogManagerWithPoolTest, doesntMmapWhenUnderCapacity)
{
    aeron_mapped_raw_log_t log;
    char filename[AERON_MAX_PATH] = { 0 };
    aeron_temp_filename(filename, sizeof(filename));

    // If usage doesnt exceed the premapped capacity, we dont mmap or munmap
    for (auto i = 0; i < 1; i++)
    {
        aeron_raw_log_manager_map_log(&log_manager, &log, filename, false, TERM_LENGTH, PAGE_SIZE);
        aeron_raw_log_manager_return_log(&log_manager, &log, filename);
        EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&log_manager.log_return_queue), 1);

        aeron_raw_log_manager_do_work(&log_manager);
        EXPECT_EQ(mmapCount, 0);
        EXPECT_EQ(munmapCount, 0);
    }
}

TEST_F(RawLogManagerWithPoolTest, willMmapToMaintainMinimumReserve)
{
    char filename[AERON_MAX_PATH] = { 0 };
    std::unordered_map<std::string, aeron_mapped_raw_log_t> mappedLogs;
    
    for (auto i = 0; i < 6; i++)
    {
        aeron_temp_filename(filename, sizeof(filename));
        auto& log = mappedLogs[filename];
        aeron_raw_log_manager_map_log(&log_manager, &log, filename, false, TERM_LENGTH, PAGE_SIZE);
        aeron_raw_log_manager_do_work(&log_manager);
    }
    
    // If usage doesnt exceed the premapped capacity - minimumReserve, we dont mmap or munmap
    EXPECT_EQ(mmapCount, 0);
    EXPECT_EQ(munmapCount, 0);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&log_manager.log_return_queue), 0);

    for (auto i = 0; i < 2; i++)
    {
        aeron_temp_filename(filename, sizeof(filename));
        auto& log = mappedLogs[filename];
        aeron_raw_log_manager_map_log(&log_manager, &log, filename, false, TERM_LENGTH, PAGE_SIZE);
        aeron_raw_log_manager_do_work(&log_manager);
    }

    // Allocate to maintain minimum reserve
    EXPECT_EQ(mmapCount, minimumReserve); 
    EXPECT_EQ(munmapCount, 0);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&log_manager.log_return_queue), 0);


    for (auto& log : mappedLogs)
    {
        aeron_raw_log_manager_return_log(&log_manager, &log.second, log.first.c_str());
    }

    // Logs are queued up to return
    EXPECT_EQ(mmapCount, minimumReserve);
    EXPECT_EQ(munmapCount, 0);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&log_manager.log_return_queue), 8);

    aeron_raw_log_manager_do_work(&log_manager);
    // We've gone back over capacity, so free up 2 extra logbuffers
    EXPECT_EQ(mmapCount, minimumReserve);
    EXPECT_EQ(munmapCount, 2);

    // End up with 8 tmp files back in reserve again
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_size(&log_manager.pools[0].raw_log_queue), 8);
}
