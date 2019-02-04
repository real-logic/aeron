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

#include <stdlib.h>
#include <inttypes.h>
#include "uri/aeron_uri.h"
#include "util/aeron_arrayutil.h"
#include "util/aeron_parse_util.h"
#include "aeron_driver_context.h"
#include "aeron_uri.h"
#include "aeron_alloc.h"

typedef enum aeron_uri_parser_state_enum
{
    PARAM_KEY, PARAM_VALUE
}
aeron_uri_parser_state_t;

int aeron_uri_parse_params(char *uri, aeron_uri_parse_callback_t param_func, void *clientd)
{
    aeron_uri_parser_state_t state = PARAM_KEY;
    char *param_key = NULL, *param_value = NULL;

    for (size_t i = 0; uri[i] != '\0'; i++)
    {
        char c = uri[i];

        switch (state)
        {
            case PARAM_KEY:
                switch (c)
                {
                    case '=':
                        uri[i] = '\0';
                        param_value = NULL;
                        state = PARAM_VALUE;
                        break;

                    default:
                        if (NULL == param_key)
                        {
                            param_key = &uri[i];
                        }
                        break;
                }
                break;

            case PARAM_VALUE:
                switch (c)
                {
                    case '|':
                        uri[i] = '\0';
                        if (param_func(clientd, param_key, param_value) < 0)
                        {
                            return -1;
                        }

                        param_key = NULL;
                        state = PARAM_KEY;
                        break;

                    default:
                        if (NULL == param_value)
                        {
                            param_value = &uri[i];
                        }
                        break;
                }
                break;
        }
    }

    if (state == PARAM_VALUE)
    {
        if (param_func(clientd, param_key, param_value) < 0)
        {
            return -1;
        }
    }

    return 0;
}

void aeron_uri_close(aeron_uri_t *params)
{
    if (params != NULL)
    {
        if (params->type == AERON_URI_UDP)
        {
            aeron_free(params->params.udp.additional_params.array);
            params->params.udp.additional_params.array = NULL;
        }
        else if (params->type == AERON_URI_IPC)
        {
            aeron_free(params->params.ipc.additional_params.array);
            params->params.udp.additional_params.array = NULL;
        }
    }
}

int aeron_uri_params_ensure_capacity(aeron_uri_params_t *params)
{
    if (aeron_array_ensure_capacity(
        (uint8_t **)&params->array, sizeof(aeron_uri_param_t), params->length, params->length + 1) >= 0)
    {
        params->length++;
        return 0;
    }

    return -1;
}

static int aeron_udp_uri_params_func(void *clientd, const char *key, const char *value)
{
    aeron_udp_channel_params_t *params = (aeron_udp_channel_params_t *)clientd;

    if (strcmp(key, AERON_UDP_CHANNEL_ENDPOINT_KEY) == 0)
    {
        params->endpoint_key = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_INTERFACE_KEY) == 0)
    {
        params->interface_key = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_TTL_KEY) == 0)
    {
        params->ttl_key = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_CONTROL_KEY) == 0)
    {
        params->control_key = value;
    }
    else
    {
        size_t index = params->additional_params.length;
        if (aeron_uri_params_ensure_capacity(&params->additional_params) < 0)
        {
            return -1;
        }

        aeron_uri_param_t *param = &params->additional_params.array[index];

        param->key = key;
        param->value = value;
    }

    return 0;
}

int aeron_udp_uri_parse(char *uri, aeron_udp_channel_params_t *params)
{
    params->additional_params.length = 0;
    params->additional_params.array = NULL;
    params->endpoint_key = NULL;
    params->interface_key = NULL;
    params->ttl_key = NULL;
    params->control_key = NULL;

    return aeron_uri_parse_params(uri, aeron_udp_uri_params_func, params);
}

static int aeron_ipc_uri_params_func(void *clientd, const char *key, const char *value)
{
    aeron_ipc_channel_params_t *params = (aeron_ipc_channel_params_t *)clientd;

    size_t index = params->additional_params.length;
    if (aeron_uri_params_ensure_capacity(&params->additional_params) < 0)
    {
        return -1;
    }

    aeron_uri_param_t *param = &params->additional_params.array[index];

    param->key = key;
    param->value = value;

    return 0;
}

int aeron_ipc_uri_parse(char *uri, aeron_ipc_channel_params_t *params)
{
    params->additional_params.length = 0;
    params->additional_params.array = NULL;

    return aeron_uri_parse_params(uri, aeron_ipc_uri_params_func, params);
}

#define AERON_URI_SCHEME "aeron:"
#define AERON_URI_UDP_TRANSPORT "udp"
#define AERON_URI_IPC_TRANSPORT "ipc"

int aeron_uri_parse(size_t uri_length, const char *uri, aeron_uri_t *params)
{
    size_t copy_length = sizeof(params->mutable_uri) - 1;
    copy_length = uri_length < copy_length ? uri_length : copy_length;

    memcpy(params->mutable_uri, uri, copy_length);
    params->mutable_uri[copy_length] = '\0';

    char *ptr = params->mutable_uri;
    params->type = AERON_URI_UNKNOWN;

    if (strncmp(ptr, AERON_URI_SCHEME, strlen(AERON_URI_SCHEME)) == 0)
    {
        ptr += strlen(AERON_URI_SCHEME);

        if (strncmp(ptr, AERON_URI_UDP_TRANSPORT, strlen(AERON_URI_UDP_TRANSPORT)) == 0)
        {
            ptr += strlen(AERON_URI_UDP_TRANSPORT);

            if (*ptr++ == '?')
            {
                params->type = AERON_URI_UDP;
                return aeron_udp_uri_parse(ptr, &params->params.udp);
            }
        }
        else if (strncmp(ptr, AERON_URI_IPC_TRANSPORT, strlen(AERON_URI_IPC_TRANSPORT)) == 0)
        {
            ptr += strlen(AERON_URI_IPC_TRANSPORT);

            if (*ptr == '?')
            {
                ptr++;
            }

            params->type = AERON_URI_IPC;
            return aeron_ipc_uri_parse(ptr, &params->params.ipc);
        }
    }

    aeron_set_err(EINVAL, "invalid URI scheme or transport: %s", uri);

    return -1;
}

uint8_t aeron_uri_multicast_ttl(aeron_uri_t *uri)
{
    uint8_t result = 0;

    if (AERON_URI_UDP == uri->type && NULL != uri->params.udp.ttl_key)
    {
        uint64_t value = strtoull(uri->params.udp.ttl_key, NULL, 0);
        result = value > 255u ? (uint8_t)255 : (uint8_t)value;
    }

    return result;
}

const char *aeron_uri_find_param_value(aeron_uri_params_t *uri_params, const char *key)
{
    size_t key_len = strlen(key);

    for (size_t i = 0, length = uri_params->length; i < length; i++)
    {
        aeron_uri_param_t *param = &uri_params->array[i];

        if (strncmp(key, param->key, key_len) == 0)
        {
            return param->value;
        }
    }

    return NULL;
}

int aeron_uri_get_term_length_param(aeron_uri_params_t *uri_params, aeron_uri_publication_params_t *params)
{
    const char *value_str;

    if ((value_str = aeron_uri_find_param_value(uri_params, AERON_URI_TERM_LENGTH_KEY)) != NULL)
    {
        uint64_t value;

        if (-1 == aeron_parse_size64(value_str, &value))
        {
            aeron_set_err(EINVAL, "could not parse %s in URI", AERON_URI_TERM_LENGTH_KEY);
            return -1;
        }

        if (aeron_logbuffer_check_term_length(value) < 0)
        {
            return -1;
        }

        params->term_length = value;
    }

    return 0;
}

int aeron_uri_get_mtu_length_param(aeron_uri_params_t *uri_params, aeron_uri_publication_params_t *params)
{
    const char *value_str;

    if ((value_str = aeron_uri_find_param_value(uri_params, AERON_URI_MTU_LENGTH_KEY)) != NULL)
    {
        uint64_t value;

        if (-1 == aeron_parse_size64(value_str, &value))
        {
            aeron_set_err(EINVAL, "could not parse %s in URI", AERON_URI_MTU_LENGTH_KEY);
            return -1;
        }

        if (aeron_driver_context_validate_mtu_length(value) < 0)
        {
            return -1;
        }

        params->mtu_length = value;
    }

    return 0;
}

int aeron_uri_linger_timeout_param(aeron_uri_params_t *uri_params, aeron_uri_publication_params_t *params)
{
    const char *value_str;

    if ((value_str = aeron_uri_find_param_value(uri_params, AERON_URI_LINGER_TIMEOUT_KEY)) != NULL)
    {
        uint64_t value;

        if (-1 == aeron_parse_duration_ns(value_str, &value))
        {
            aeron_set_err(EINVAL, "could not parse %s in URI", AERON_URI_LINGER_TIMEOUT_KEY);
            return -1;
        }

        params->linger_timeout_ns = value;
    }

    return 0;
}

int aeron_uri_publication_params(
    aeron_uri_t *uri,
    aeron_uri_publication_params_t *params,
    aeron_driver_context_t *context,
    bool is_exclusive)
{
    params->linger_timeout_ns = context->publication_linger_timeout_ns;
    params->term_length = AERON_URI_IPC == uri->type ? context->ipc_term_buffer_length : context->term_buffer_length;
    params->mtu_length = AERON_URI_IPC == uri->type ? context->ipc_mtu_length : context->mtu_length;
    params->initial_term_id = 0;
    params->term_offset = 0;
    params->term_id = 0;
    params->is_replay = false;
    params->is_sparse = context->term_buffer_sparse_file;
    aeron_uri_params_t *uri_params = AERON_URI_IPC == uri->type ?
        &uri->params.ipc.additional_params : &uri->params.udp.additional_params;

    if (aeron_uri_linger_timeout_param(uri_params, params) < 0)
    {
        return -1;
    }

    if (aeron_uri_get_term_length_param(uri_params, params) < 0)
    {
        return -1;
    }

    if (aeron_uri_get_mtu_length_param(uri_params, params) < 0)
    {
        return -1;
    }

    if (is_exclusive)
    {
        const char *initial_term_id_str = NULL;
        const char *term_id_str = NULL;
        const char *term_offset_str = NULL;
        int count = 0;

        initial_term_id_str = aeron_uri_find_param_value(uri_params, AERON_URI_INITIAL_TERM_ID_KEY);
        count += initial_term_id_str ? 1 : 0;

        term_id_str = aeron_uri_find_param_value(uri_params, AERON_URI_TERM_ID_KEY);
        count += term_id_str ? 1 : 0;

        term_offset_str = aeron_uri_find_param_value(uri_params, AERON_URI_TERM_OFFSET_KEY);
        count += term_offset_str ? 1 : 0;

        if (count > 0)
        {
            if (count < 3)
            {
                aeron_set_err(EINVAL, "params must be used as a complete set: %s %s %s",
                    AERON_URI_INITIAL_TERM_ID_KEY, AERON_URI_TERM_ID_KEY, AERON_URI_TERM_OFFSET_KEY);
                return -1;
            }

            errno = 0;
            if ((params->initial_term_id = strtoll(initial_term_id_str, NULL, 0)) == 0 && 0 != errno)
            {
                aeron_set_err(EINVAL, "could not parse %s in URI", AERON_URI_INITIAL_TERM_ID_KEY);
                return -1;
            }

            errno = 0;
            if ((params->term_id = strtoll(term_id_str, NULL, 0)) == 0 && 0 != errno)
            {
                aeron_set_err(EINVAL, "could not parse %s in URI", AERON_URI_TERM_ID_KEY);
                return -1;
            }

            errno = 0;
            if ((params->term_offset = strtoull(term_offset_str, NULL, 0)) == 0 && 0 != errno)
            {
                aeron_set_err(EINVAL, "could not parse %s in URI", AERON_URI_TERM_OFFSET_KEY);
                return -1;
            }

            if (params->initial_term_id < INT32_MIN || params->initial_term_id > INT32_MAX)
            {
                aeron_set_err(
                    EINVAL,
                    "Params %s=%" PRId64 " out of range", AERON_URI_INITIAL_TERM_ID_KEY, params->initial_term_id);
                return -1;
            }

            if (params->term_id < INT32_MIN || params->term_id > INT32_MAX)
            {
                aeron_set_err(EINVAL, "Params %s=%" PRId64 " out of range", AERON_URI_TERM_ID_KEY, params->term_id);
                return -1;
            }

            if (((int32_t)params->term_id - (int32_t)params->initial_term_id) < 0)
            {
                aeron_set_err(
                    EINVAL,
                    "Param difference greater than 2^31 - 1: %s=%" PRId64 " %s=%" PRId64,
                    AERON_URI_INITIAL_TERM_ID_KEY,
                    params->initial_term_id,
                    AERON_URI_TERM_OFFSET_KEY,
                    params->term_id);
                return -1;
            }

            if (params->term_offset > params->term_length)
            {
                aeron_set_err(
                    EINVAL,
                    "Param %s=%" PRIu64 " > %s=%" PRIu64,
                    AERON_URI_TERM_OFFSET_KEY,
                    params->term_offset,
                    AERON_URI_TERM_LENGTH_KEY,
                    params->term_length);
                return -1;
            }

            if ((params->term_offset & (AERON_LOGBUFFER_FRAME_ALIGNMENT - 1)) != 0)
            {
                aeron_set_err(
                    EINVAL,
                    "Param %s=%" PRIu64 " must be multiple of FRAME_ALIGNMENT",
                    AERON_URI_TERM_OFFSET_KEY,
                    params->term_offset);
                return -1;
            }

            params->is_replay = true;
        }
    }

    const char *value_str;

    if ((value_str = aeron_uri_find_param_value(uri_params, AERON_URI_SPARSE_TERM_KEY)) != NULL)
    {
        if (strncmp("true", value_str, strlen("true")) == 0)
        {
            params->is_sparse = true;
        }
    }

    return 0;
}

int aeron_uri_subscription_params(
    aeron_uri_t *uri,
    aeron_uri_subscription_params_t *params,
    aeron_driver_context_t *context)
{
    params->is_reliable = true;
    params->is_sparse = context->term_buffer_sparse_file;

    const char *value_str;
    aeron_uri_params_t *uri_params = AERON_URI_IPC == uri->type ?
        &uri->params.ipc.additional_params : &uri->params.udp.additional_params;

    if ((value_str = aeron_uri_find_param_value(uri_params, AERON_UDP_CHANNEL_RELIABLE_KEY)) != NULL)
    {
        if (strncmp("false", value_str, strlen("false")) == 0)
        {
            params->is_reliable = false;
        }
    }

    if ((value_str = aeron_uri_find_param_value(uri_params, AERON_URI_SPARSE_TERM_KEY)) != NULL)
    {
        if (strncmp("true", value_str, strlen("true")) == 0)
        {
            params->is_sparse = true;
        }
    }

    return 0;
}
