/*
 * Copyright 2014-2021 Real Logic Limited.
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

#include <string.h>
#include <inttypes.h>

#include "util/aeron_error.h"
#include "util/aeron_strutil.h"
#include "util/aeron_arrayutil.h"
#include "aeron_csv_table_name_resolver.h"

#define AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE (1024)
#define AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS (3)
#define AERON_CSV_TABLE_NAME_RESOLVER_ENTRY_COUNTER_TYPE_ID (2001)

typedef struct aeron_csv_table_name_resolver_row_stct
{
    const char *name;
    const char *initial_resolution_host;
    const char *re_resolution_host;
    int32_t counter_id;
}
aeron_csv_table_name_resolver_row_t;

typedef struct aeron_csv_table_name_resolver_stct
{
    aeron_csv_table_name_resolver_row_t *array;
    size_t length;
    size_t capacity;
    char *saved_config_csv;
}
aeron_csv_table_name_resolver_t;

int aeron_csv_table_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    const char *hostname = name;
    if (NULL != resolver->state)
    {
        aeron_csv_table_name_resolver_t *table = (aeron_csv_table_name_resolver_t *)resolver->state;

        for (size_t i = 0; i < table->length; i++)
        {
            if (strncmp(name, table->array[i].name, strlen(table->array[i].name) + 1) == 0)
            {
                hostname = is_re_resolution ?
                    table->array[i].re_resolution_host : table->array[i].initial_resolution_host;
            }
        }
    }

    return aeron_default_name_resolver_resolve(NULL, hostname, uri_param_name, is_re_resolution, address);
}

int aeron_csv_table_name_resolver_close(aeron_name_resolver_t *resolver)
{
    aeron_csv_table_name_resolver_t *resolver_state = (aeron_csv_table_name_resolver_t *)resolver->state;

    if (NULL != resolver_state)
    {
        aeron_free(resolver_state->saved_config_csv);
        aeron_free(resolver_state->array);
        aeron_free(resolver_state);
    }
    return 0;
}

int aeron_csv_table_name_resolver_supplier(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context)
{
    resolver->lookup_func = aeron_default_name_resolver_lookup;
    resolver->close_func = aeron_csv_table_name_resolver_close;

    resolver->resolve_func = aeron_csv_table_name_resolver_resolve;
    resolver->do_work_func = aeron_default_name_resolver_do_work;

    char *rows[AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE];
    char *columns[AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];
    aeron_csv_table_name_resolver_t *lookup_table = NULL;

    if (NULL == args)
    {
        AERON_SET_ERR(EINVAL, "No CSV configuration, please specify: %s", AERON_NAME_RESOLVER_INIT_ARGS_ENV_VAR);
        goto error;
    }

    if (aeron_alloc((void **)&lookup_table, sizeof(aeron_csv_table_name_resolver_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Allocating lookup table");
        goto error;
    }
    resolver->state = lookup_table;

    lookup_table->saved_config_csv = strdup(args);
    if (NULL == lookup_table->saved_config_csv)
    {
        AERON_SET_ERR(errno, "%s", "Duplicating config string");
        goto error;
    }

    int num_rows = aeron_tokenise(lookup_table->saved_config_csv, '|', AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE, rows);
    if (num_rows < 0)
    {
        AERON_SET_ERR(num_rows, "%s", "Failed to parse rows for lookup table");
        goto error;
    }

    for (int i = num_rows; -1 < --i;)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, (*lookup_table), aeron_csv_table_name_resolver_row_t)
        if (ensure_capacity_result < 0)
        {
            AERON_APPEND_ERR(
                "Failed to allocate rows for lookup table (%" PRIu64 ",%" PRIu64 ")",
                (uint64_t)lookup_table->length,
                (uint64_t)lookup_table->capacity);
            goto error;
        }

        int num_columns = aeron_tokenise(rows[i], ',', AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS, columns);
        if (AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS == num_columns)
        {
            // Fields are in reverse order.
            lookup_table->array[lookup_table->length].re_resolution_host = columns[0];
            lookup_table->array[lookup_table->length].initial_resolution_host = columns[1];
            lookup_table->array[lookup_table->length].name = columns[2];

            uint8_t key_buffer[512] = { 0 };
            uint32_t name_str_length = (uint32_t)strlen(lookup_table->array[lookup_table->length].name);
            uint32_t name_length = name_str_length < sizeof(key_buffer) - sizeof(name_str_length) ?
                name_str_length : sizeof(key_buffer) - sizeof(name_str_length);

            memcpy(key_buffer, &name_length, sizeof(name_length));
            memcpy(&key_buffer[sizeof(name_length)], lookup_table->array[lookup_table->length].name, name_length);

            char value_buffer[512] = { 0 };
            size_t value_buffer_maxlen = sizeof(value_buffer) - 1;

            int value_buffer_result = snprintf(
                value_buffer,
                value_buffer_maxlen,
                "NameEntry{name='%s', initialResolutionHost='%s', reResolutionHost='%s'}",
                lookup_table->array[lookup_table->length].name,
                lookup_table->array[lookup_table->length].initial_resolution_host,
                lookup_table->array[lookup_table->length].re_resolution_host);
            
            if (value_buffer_result < 0)
            {
                AERON_SET_ERR(EINVAL, "%s", "Failed to create csv resolver counter label");
                goto error;
            }

            size_t key_length = sizeof(name_length) + name_length;
            size_t value_length = (size_t)value_buffer_result < value_buffer_maxlen ?
                (size_t)value_buffer_result : value_buffer_maxlen;

            lookup_table->array[lookup_table->length].counter_id = aeron_counters_manager_allocate(
                context->counters_manager, 
                AERON_CSV_TABLE_NAME_RESOLVER_ENTRY_COUNTER_TYPE_ID,
                key_buffer,
                key_length,
                value_buffer,
                value_length);

            if (lookup_table->array[lookup_table->length].counter_id < 0)
            {
                AERON_APPEND_ERR("%s", "Failed to allocate csv resolver counter");
                goto error;
            }

            lookup_table->length++;
        }
    }

    resolver->state = lookup_table;

    return 0;

error:
    aeron_csv_table_name_resolver_close(resolver);
    return -1;
}
