#ifndef AERON_RAW_LOG_MANAGER_H
#define AERON_RAW_LOG_MANAGER_H

#include "concurrent/aeron_spsc_concurrent_array_queue.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
#include "util/aeron_raw_log_pool_config.h"

#include <errno.h>
#include <stdio.h>

typedef struct aeron_driver_context_stct aeron_driver_context_t;

typedef int (*aeron_raw_log_map_func_t)(aeron_mapped_raw_log_t *, const char *, bool, uint64_t, uint64_t);
typedef bool (*aeron_raw_log_free_func_t)(aeron_mapped_raw_log_t *, const char *);

typedef struct aeron_mapped_tmp_raw_log_stct
{
    aeron_mapped_raw_log_t raw_log;
    char* path;
    aeron_raw_log_free_func_t free_func;
}
aeron_mapped_tmp_raw_log_t;

typedef struct aeron_raw_log_size_pool_stct
{
    aeron_raw_log_pool_config_t cfg;
    // queue of aeron_mapped_raw_log_t, log_manager -> conductor
    aeron_spsc_concurrent_array_queue_t raw_log_queue;
    bool disabled;
}
aeron_raw_log_pool_t;

typedef struct aeron_raw_log_manager_stct
{
    aeron_raw_log_pool_t* pools;
    size_t num_pools;
    char const * aeron_dir;
    size_t file_page_size;
    // queue of aeron_mapped_raw_log_t, conductor -> log_manager
    aeron_spsc_concurrent_array_queue_t log_return_queue; 
    volatile int64_t next_id;

    aeron_raw_log_map_func_t map_func;
    aeron_raw_log_free_func_t free_func;
    aeron_usable_fs_space_func_t usable_space_func;
}
aeron_raw_log_manager_t;

int aeron_raw_log_manager_init(aeron_raw_log_manager_t* raw_log_manager, aeron_driver_context_t* context, 
                               aeron_raw_log_map_func_t map_func, aeron_raw_log_free_func_t free_func);
void aeron_raw_log_manager_on_close(void *clientd);

int aeron_raw_log_manager_do_work(void *clientd);

typedef int (*aeron_raw_log_mngr_map_func_t)(aeron_raw_log_manager_t*, aeron_mapped_raw_log_t *, const char *, bool, uint64_t, uint64_t);
typedef bool (*aeron_raw_log_mngr_free_func_t)(aeron_raw_log_manager_t*, aeron_mapped_raw_log_t *, const char *);

int aeron_raw_log_manager_map_log(
    aeron_raw_log_manager_t* raw_log_manager,
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool use_sparse_files,
    uint64_t term_length,
    uint64_t page_size);


bool aeron_raw_log_manager_return_log(
    aeron_raw_log_manager_t* log_manager,
    aeron_mapped_raw_log_t* raw_log,
    const char* filename);

#endif //AERON_RAW_LOG_MANAGER_H
