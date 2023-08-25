#include "aeron_raw_log_manager.h"
#include "aeron_driver_context.h"

#include <inttypes.h>

void aeron_mapped_tmp_raw_log_free(aeron_mapped_tmp_raw_log_t* tmp_raw_log)
{
    tmp_raw_log->free_func(&tmp_raw_log->raw_log, tmp_raw_log->path);
    aeron_free(tmp_raw_log->path);
    aeron_free(tmp_raw_log);
}

// Called both upon conductor returning logs, and log manager creating new logs
char* aeron_raw_log_manager_next_tmp_file(aeron_raw_log_manager_t* log_manager)
{
    char buffer[AERON_MAX_PATH];
    int64_t id;
    AERON_GET_AND_ADD_INT64(id, log_manager->next_id, 1);
    int path_length = aeron_tmp_logbuffer_location(buffer, sizeof(buffer), log_manager->aeron_dir, log_manager->next_id++);

    char* path;
    aeron_alloc((void**)&path, path_length+1);
    strncpy(path, buffer, path_length+1);

    return path;
}

void aeron_mapped_tmp_raw_log_free_from_queue(void* clientd, void* item)
{
    (void)clientd;
    aeron_mapped_tmp_raw_log_free((aeron_mapped_tmp_raw_log_t*)item);
}

aeron_mapped_tmp_raw_log_t* aeron_mapped_tmp_raw_log_create(aeron_raw_log_manager_t* log_manager, uint64_t term_length)
{
    aeron_mapped_tmp_raw_log_t* tmp_raw_log;
    aeron_alloc((void**)&tmp_raw_log, sizeof(aeron_mapped_tmp_raw_log_t));
    tmp_raw_log->free_func = log_manager->free_func;

    const uint64_t log_length = aeron_logbuffer_compute_log_length(term_length, log_manager->file_page_size);
    const uint64_t usable_space = log_manager->usable_space_func(log_manager->aeron_dir);
    if (usable_space < log_length)
    {
        fprintf(stderr, "usable space (%ld) is less than log length (%ld), for dir %s.\n", usable_space, log_length, log_manager->aeron_dir);
        aeron_free(tmp_raw_log);
        return NULL;
    }
    
    tmp_raw_log->path = aeron_raw_log_manager_next_tmp_file(log_manager);
    if (log_manager->map_func(&tmp_raw_log->raw_log, tmp_raw_log->path, false, term_length, log_manager->file_page_size))
    {
        aeron_free(tmp_raw_log->path);
        aeron_free(tmp_raw_log);
        return NULL;
    }

    return tmp_raw_log;
}


void aeron_raw_log_pool_close(aeron_raw_log_pool_t* pool)
{
    aeron_spsc_concurrent_array_queue_drain_all(&pool->raw_log_queue, aeron_mapped_tmp_raw_log_free_from_queue, NULL);
    aeron_spsc_concurrent_array_queue_close(&pool->raw_log_queue);
}

int aeron_raw_log_pool_init(aeron_raw_log_manager_t* raw_log_manager, aeron_raw_log_pool_t* pool, aeron_raw_log_pool_config_t* pool_config)
{
    pool->cfg = *pool_config;
    pool->disabled = false;

    if (-1 == aeron_spsc_concurrent_array_queue_init(&pool->raw_log_queue, pool->cfg.initial_capacity))
    {
        fprintf(stderr, "failed to allocate log return queue.\n");
        return -1;
    }

    for (size_t i = 0; i < pool->cfg.initial_capacity; i++)
    {
        aeron_mapped_tmp_raw_log_t* raw_log = aeron_mapped_tmp_raw_log_create(raw_log_manager, pool->cfg.term_length);
        if (NULL == raw_log)
        {
            fprintf(stderr, "failed to allocate log for initial pool. i: %ld, size: %ld\n", i, pool->cfg.term_length);
            aeron_raw_log_pool_close(pool);
            return -1;
        }

        if (AERON_OFFER_SUCCESS != aeron_spsc_concurrent_array_queue_offer(&pool->raw_log_queue, (void*)raw_log))
        {
            fprintf(stderr, "failed to offer log for initial pool.\n");
            aeron_raw_log_pool_close(pool);
            return -1;
        }
    }

    return pool->cfg.initial_capacity;
}

int aeron_raw_log_manager_init(aeron_raw_log_manager_t* raw_log_manager, aeron_driver_context_t* context,
                               aeron_raw_log_map_func_t map_func, aeron_raw_log_free_func_t free_func)
{
    raw_log_manager->aeron_dir = context->aeron_dir;
    raw_log_manager->file_page_size = context->file_page_size;
    raw_log_manager->num_pools = context->num_raw_log_pools;
    raw_log_manager->map_func = map_func;
    raw_log_manager->free_func = free_func;
    raw_log_manager->usable_space_func = context->usable_fs_space_func;
    AERON_PUT_VOLATILE(raw_log_manager->next_id, 0);

    if (aeron_reallocf((void**)&raw_log_manager->pools, sizeof(aeron_raw_log_pool_t)*raw_log_manager->num_pools))
    {
        fprintf(stderr, "failed to allocate log manager pools.\n");
        return -1;
    }

    size_t total_capacity = 0;
    for (size_t i = 0; i < raw_log_manager->num_pools; i++)
    {
        int result = aeron_raw_log_pool_init(
            raw_log_manager, 
            &raw_log_manager->pools[i], 
            &context->raw_log_pools[i]);
        
        if (-1 == result)
        {
            fprintf(stderr, "failed to initialise log pool, len: %ld, term_length: %ld.\n",
                context->raw_log_pools[i].initial_capacity, context->raw_log_pools[i].term_length);
            return -1;
        }

        total_capacity += result;
    }

    if (-1 == aeron_spsc_concurrent_array_queue_init(&raw_log_manager->log_return_queue, total_capacity))
    {
        fprintf(stderr, "failed to allocate log return queue.\n");
        return -1;
    }

    return 0;
}

void aeron_raw_log_manager_on_close(void* clientd)
{
    aeron_raw_log_manager_t* raw_log_manager = (aeron_raw_log_manager_t*)clientd;

    for (size_t i = 0; i < raw_log_manager->num_pools; i++)
    {
        aeron_raw_log_pool_close(&raw_log_manager->pools[i]);
    }
    
    aeron_spsc_concurrent_array_queue_drain_all(&raw_log_manager->log_return_queue, aeron_mapped_tmp_raw_log_free_from_queue, NULL);
    aeron_spsc_concurrent_array_queue_close(&raw_log_manager->log_return_queue);

    aeron_free(raw_log_manager->pools);
}

void aeron_raw_log_manager_process_returned_log(void *clientd, void *item)
{
    aeron_raw_log_manager_t* raw_log_manager = (aeron_raw_log_manager_t*)clientd;
    aeron_mapped_tmp_raw_log_t* tmp_raw_log = (aeron_mapped_tmp_raw_log_t*)item;
    aeron_mapped_raw_log_t* raw_log = &tmp_raw_log->raw_log;

    memset(raw_log->mapped_file.addr, 0, raw_log->mapped_file.length);

    for (size_t i = 0; i < raw_log_manager->num_pools; i++)
    {
        aeron_raw_log_pool_t* pool = &raw_log_manager->pools[i];
        if (pool->cfg.term_length == tmp_raw_log->raw_log.term_length)
        {
            if (aeron_spsc_concurrent_array_queue_size(&pool->raw_log_queue) > pool->cfg.initial_capacity ||
                AERON_OFFER_SUCCESS != aeron_spsc_concurrent_array_queue_offer(&pool->raw_log_queue, (void*)tmp_raw_log))
            {
                aeron_mapped_tmp_raw_log_free(tmp_raw_log);
            }
            break;
        }
    }
}

int aeron_raw_log_manager_refill_pool(aeron_raw_log_manager_t* raw_log_manager, aeron_raw_log_pool_t* pool)
{
    int work = 0;
    if (pool->disabled)
        return work;

    while (aeron_spsc_concurrent_array_queue_size(&pool->raw_log_queue) < pool->cfg.minimum_reserve)
    {
        aeron_mapped_tmp_raw_log_t* raw_log = aeron_mapped_tmp_raw_log_create(raw_log_manager, pool->cfg.term_length);
        if (NULL == raw_log)
        {
            const uint64_t log_length = aeron_logbuffer_compute_log_length(pool->cfg.term_length, raw_log_manager->file_page_size);
            pool->disabled = true;
            AERON_SET_ERR(ENOSPC, "Insufficient usable storage for new log of length=%" PRId64 " in %s", log_length, raw_log_manager->aeron_dir);
            return work;
        }

        if (AERON_OFFER_SUCCESS != aeron_spsc_concurrent_array_queue_offer(&pool->raw_log_queue, (void*)raw_log))
        {
            aeron_mapped_tmp_raw_log_free(raw_log);
            return work;
        }
        work++;
    }

    return work;
}

int aeron_raw_log_manager_do_work(void* clientd)
{
    aeron_raw_log_manager_t* raw_log_manager = (aeron_raw_log_manager_t*)clientd;
    int work = 0;

    work += (int) aeron_spsc_concurrent_array_queue_drain_all(&raw_log_manager->log_return_queue, aeron_raw_log_manager_process_returned_log, raw_log_manager);

    for (size_t i = 0; i < raw_log_manager->num_pools; i++)
    {
        work += aeron_raw_log_manager_refill_pool(raw_log_manager, &raw_log_manager->pools[i]);
    }
    return work;
}

// All functions from here onwards called from conductor only
aeron_mapped_tmp_raw_log_t* aeron_raw_log_manager_take_log_from_pool(
    aeron_raw_log_manager_t* raw_log_manager,
    uint64_t term_length,
    uint64_t page_size)
{
    if (raw_log_manager->file_page_size != page_size)
    {
        return NULL;
    }

    for (size_t i = 0; i < raw_log_manager->num_pools; i++)
    {
        if (raw_log_manager->pools[i].cfg.term_length == term_length)
        {
            return (aeron_mapped_tmp_raw_log_t*)aeron_spsc_concurrent_array_queue_poll(&raw_log_manager->pools[i].raw_log_queue);
        }
    }

    return NULL;
}

int aeron_raw_log_manager_map_log(
    aeron_raw_log_manager_t* raw_log_manager,
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *path,
    bool use_sparse_files,
    uint64_t term_length,
    uint64_t page_size)
{
    aeron_mapped_tmp_raw_log_t* pool_log = aeron_raw_log_manager_take_log_from_pool(raw_log_manager, term_length, page_size);
    if (NULL != pool_log)
    {   
        if (rename(pool_log->path, path) != 0)
        {
            AERON_SET_ERR(errno, "Failed to rename raw log, from: %s, to: %s", pool_log->path, path);
            aeron_mapped_tmp_raw_log_free(pool_log);
            return -1;
        }

        *mapped_raw_log = pool_log->raw_log;
        aeron_free(pool_log->path);
        aeron_free(pool_log);

        mapped_raw_log->from_pool = true;

        return 0;
    }

    // Fallback to mapping on conductor thread if nothing available in pool
    return raw_log_manager->map_func(mapped_raw_log, path, use_sparse_files, term_length, page_size);
}

bool aeron_raw_log_manager_return_log(aeron_raw_log_manager_t* log_manager, aeron_mapped_raw_log_t* raw_log, const char* filename)
{
    if (!raw_log->from_pool)
    {
        return log_manager->free_func(raw_log, filename);
    }

    aeron_mapped_tmp_raw_log_t* tmp_raw_log;
    aeron_alloc((void**)&tmp_raw_log, sizeof(aeron_mapped_tmp_raw_log_t));
    
    tmp_raw_log->raw_log = *raw_log;
    tmp_raw_log->path = aeron_raw_log_manager_next_tmp_file(log_manager);
    tmp_raw_log->free_func = log_manager->free_func;

    if (rename(filename, tmp_raw_log->path) != 0)
    {
        AERON_SET_ERR(errno, "Failed to reclaim raw log, from: %s, to: %s", filename, tmp_raw_log->path);
        aeron_mapped_tmp_raw_log_free(tmp_raw_log);
        return false;
    }

    if (AERON_OFFER_SUCCESS != aeron_spsc_concurrent_array_queue_offer(&log_manager->log_return_queue, (void*)tmp_raw_log))
    {
        aeron_mapped_tmp_raw_log_free(tmp_raw_log);
    }

    return true;
}
