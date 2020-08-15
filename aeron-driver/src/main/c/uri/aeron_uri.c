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

#include <inttypes.h>
#include "uri/aeron_uri.h"
#include "util/aeron_arrayutil.h"
#include "util/aeron_math.h"
#include "util/aeron_parse_util.h"
#include "aeron_driver_context.h"
#include "aeron_driver_conductor.h"

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
                        if (NULL == param_key)
                        {
                            aeron_set_err(-AERON_ERROR_CODE_INVALID_CHANNEL, "empty key not allowed");
                            return -1;
                        }
                        uri[i] = '\0';
                        param_value = NULL;
                        state = PARAM_VALUE;
                        break;

                    case '|':
                        aeron_set_err(-AERON_ERROR_CODE_INVALID_CHANNEL, "invalid end of key");
                        return -1;

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
                        if (NULL == param_value)
                        {
                            aeron_set_err(-AERON_ERROR_CODE_INVALID_CHANNEL, "empty value not allowed");
                            return -1;
                        }
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
        params->endpoint = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_INTERFACE_KEY) == 0)
    {
        params->bind_interface = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_TTL_KEY) == 0)
    {
        params->ttl = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_CONTROL_KEY) == 0)
    {
        params->control = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_CONTROL_MODE_KEY) == 0)
    {
        params->control_mode = value;
    }
    else if (strcmp(key, AERON_URI_TAGS_KEY) == 0)
    {
        char *ptr = strchr(value, ',');

        if (NULL != ptr)
        {
            *ptr++ = '\0';
            params->entity_tag = '\0' == *ptr ? NULL : ptr;
        }

        params->channel_tag = '\0' == *value ? NULL : value;
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
    params->endpoint = NULL;
    params->bind_interface = NULL;
    params->ttl = NULL;
    params->control = NULL;
    params->channel_tag = NULL;
    params->entity_tag = NULL;

    return aeron_uri_parse_params(uri, aeron_udp_uri_params_func, params);
}

static int aeron_ipc_uri_params_func(void *clientd, const char *key, const char *value)
{
    aeron_ipc_channel_params_t *params = (aeron_ipc_channel_params_t *)clientd;

    if (strcmp(key, AERON_URI_TAGS_KEY) == 0)
    {
        char *ptr = strchr(value, ',');

        if (NULL != ptr)
        {
            *ptr++ = '\0';
            params->entity_tag = '\0' == *ptr ? NULL : ptr;
        }

        params->channel_tag = '\0' == *value ? NULL : value;
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

int aeron_ipc_uri_parse(char *uri, aeron_ipc_channel_params_t *params)
{
    params->additional_params.length = 0;
    params->additional_params.array = NULL;
    params->channel_tag = NULL;
    params->entity_tag = NULL;

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
                int result = aeron_udp_uri_parse(ptr, &params->params.udp);
                if (result < 0)
                {
                    aeron_set_err(EINVAL, "%s: %.*s", aeron_errmsg(), (int)uri_length, uri);
                }
                return result;
            }
        }
        else if (strncmp(ptr, AERON_URI_IPC_TRANSPORT, strlen(AERON_URI_IPC_TRANSPORT)) == 0)
        {
            ptr += strlen(AERON_URI_IPC_TRANSPORT);

            if (*ptr == '\0' || *ptr++ == '?')
            {
                params->type = AERON_URI_IPC;
                int result = aeron_ipc_uri_parse(ptr, &params->params.ipc);
                if (result < 0)
                {
                    aeron_set_err(EINVAL, "%s: %.*s", aeron_errmsg(), (int)uri_length, uri);
                }
                return result;
            }
        }
    }

    aeron_set_err(-AERON_ERROR_CODE_INVALID_CHANNEL, "invalid URI scheme or transport: %s", uri);

    return -1;
}

uint8_t aeron_uri_multicast_ttl(aeron_uri_t *uri)
{
    uint8_t result = 0;

    if (AERON_URI_UDP == uri->type && NULL != uri->params.udp.ttl)
    {
        uint64_t value = strtoull(uri->params.udp.ttl, NULL, 0);
        result = value > 255u ? (uint8_t)255 : (uint8_t)value;
    }

    return result;
}

const char *aeron_uri_find_param_value(const aeron_uri_params_t *uri_params, const char *key)
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
            aeron_set_err(EINVAL, "could not parse %s=%s in URI", AERON_URI_TERM_LENGTH_KEY, value_str);
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
            aeron_set_err(EINVAL, "could not parse %s=%s in URI", AERON_URI_MTU_LENGTH_KEY, value_str);
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
            aeron_set_err(EINVAL, "could not parse %s=%s in URI", AERON_URI_LINGER_TIMEOUT_KEY, value_str);
            return -1;
        }

        params->linger_timeout_ns = value;
    }

    return 0;
}

int aeron_uri_get_int32(aeron_uri_params_t *uri_params, const char *key, int32_t *retval)
{
    const char *value_str;
    if ((value_str = aeron_uri_find_param_value(uri_params, key)) == NULL)
    {
        *retval = 0;
        return 0;
    }

    char *end_ptr = "";
    errno = 0;
    const long value = strtol(value_str, &end_ptr, 0);

    if (0 != errno || '\0' != *end_ptr)
    {
        aeron_set_err(EINVAL, "could not parse %s=%s as int32_t in URI: %s", key, value_str, strerror(errno));
        return -1;
    }
    else if (value < INT32_MIN || INT32_MAX < value)
    {
        aeron_set_err(
            EINVAL,
            "could not parse %s=%s as int32_t in URI: Numerical result out of range", key, value_str);
        return -1;
    }

    *retval = (int32_t)value;

    return 1;
}

int aeron_uri_get_int64(aeron_uri_params_t *uri_params, const char *key, int64_t *retval)
{
    const char *value_str;
    if ((value_str = aeron_uri_find_param_value(uri_params, key)) == NULL)
    {
        *retval = 0;
        return 0;
    }

    char *end_ptr;
    int64_t value;

    errno = 0;
    value = strtoll(value_str, &end_ptr, 0);
    if (0 != errno || '\0' != *end_ptr)
    {
        aeron_set_err(EINVAL, "could not parse %s=%s as int64_t in URI: ", key, value_str, strerror(errno));
        return -1;
    }

    *retval = value;

    return 1;
}

int aeron_uri_get_bool(aeron_uri_params_t *uri_params, const char *key, bool *retval)
{
    const char *value_str = aeron_uri_find_param_value(uri_params, key);
    if (value_str != NULL)
    {
        if (strncmp("true", value_str, strlen("true")) == 0)
        {
            *retval = true;
        }
        else if (strncmp("false", value_str, strlen("false")) == 0)
        {
            *retval = false;
        }
        else
        {
            aeron_set_err(EINVAL, "could not parse %s=%s as bool from URI", key, value_str);
            return -1;
        }
    }

    return 1;
}

int aeron_uri_get_ats(aeron_uri_params_t *uri_params, aeron_uri_ats_status_t *uri_ats_status)
{
    const char *value_str = aeron_uri_find_param_value(uri_params, AERON_URI_ATS_KEY);
    *uri_ats_status = AERON_URI_ATS_STATUS_DEFAULT;
    if (value_str != NULL)
    {
        if (strncmp("true", value_str, strlen("true")) == 0)
        {
            *uri_ats_status = AERON_URI_ATS_STATUS_ENABLED;
        }
        else if (strncmp("false", value_str, strlen("false")) == 0)
        {
            *uri_ats_status = AERON_URI_ATS_STATUS_DISABLED;
        }
        else
        {
            aeron_set_err(EINVAL, "could not parse %s=%s as bool from URI", AERON_URI_ATS_KEY, value_str);
            return -1;
        }
    }

    return 1;
}

int aeron_uri_publication_session_id_param(
    aeron_uri_params_t *uri_params, aeron_driver_conductor_t *conductor, aeron_uri_publication_params_t *params)
{
    const char *session_id_str = aeron_uri_find_param_value(uri_params, AERON_URI_SESSION_ID_KEY);
    if (NULL != session_id_str)
    {
        if (0 == strncmp("tag:", session_id_str, strlen("tag:")))
        {
            char *end_ptr;
            errno = 0;

            long long tag = strtoll(&session_id_str[4], &end_ptr, 0);
            if (0 != errno || '\0' != *end_ptr)
            {
                aeron_set_err(
                    EINVAL,
                    "could not parse %s=%s as int64_t in URI: ",
                    AERON_URI_SESSION_ID_KEY, session_id_str, strerror(errno));
                return -1;
            }

            aeron_network_publication_t *publication = aeron_driver_conductor_find_network_publication_by_tag(
                conductor, (int64_t)tag);

            if (NULL == publication)
            {
                aeron_set_err(
                    EINVAL, "%s=%s must reference a network publication", AERON_URI_SESSION_ID_KEY, session_id_str);
                return -1;
            }

            params->has_session_id = true;
            params->session_id = publication->session_id;
            params->mtu_length = publication->mtu_length;
            params->term_length = publication->term_buffer_length;
        }
        else
        {
            int result = aeron_uri_get_int32(uri_params, AERON_URI_SESSION_ID_KEY, &params->session_id);
            params->has_session_id = 1 == result;

            return result < 0 ? -1 : 0;
        }
    }

    return 0;
}

int aeron_uri_subscription_session_id_param(aeron_uri_params_t *uri_params, aeron_uri_subscription_params_t *params)
{
    int result = aeron_uri_get_int32(uri_params, AERON_URI_SESSION_ID_KEY, &params->session_id);
    params->has_session_id = 1 == result;

    return result < 0 ? -1 : 0;
}

int aeron_uri_publication_params(
    aeron_uri_t *uri,
    aeron_uri_publication_params_t *params,
    aeron_driver_conductor_t *conductor,
    bool is_exclusive)
{
    aeron_driver_context_t *context = conductor->context;

    params->linger_timeout_ns = context->publication_linger_timeout_ns;
    params->term_length = AERON_URI_IPC == uri->type ? context->ipc_term_buffer_length : context->term_buffer_length;
    params->mtu_length = AERON_URI_IPC == uri->type ? context->ipc_mtu_length : context->mtu_length;
    params->initial_term_id = 0;
    params->term_offset = 0;
    params->term_id = 0;
    params->has_position = false;
    params->is_sparse = context->term_buffer_sparse_file;
    params->signal_eos = true;
    params->spies_simulate_connection = context->spies_simulate_connection;
    params->has_session_id = false;
    params->session_id = 0;
    params->entity_tag = AERON_URI_INVALID_TAG;

    aeron_uri_params_t *uri_params = AERON_URI_IPC == uri->type ?
        &uri->params.ipc.additional_params : &uri->params.udp.additional_params;

    if (aeron_uri_publication_session_id_param(uri_params, conductor, params) < 0)
    {
        return -1;
    }

    const char *entity_tag_str = AERON_URI_IPC == uri->type ? uri->params.ipc.entity_tag : uri->params.udp.entity_tag;
    if (NULL != entity_tag_str)
    {
        errno = 0;
        char *end_ptr;
        long long entity_tag = strtoll(entity_tag_str, &end_ptr, 10);
        if (0 != errno || *end_ptr != '\0')
        {
            aeron_set_err(EINVAL, "Entity tag invalid");
            return -1;
        }

        params->entity_tag = (int64_t)entity_tag;
    }

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

    int count = 0;

    int32_t initial_term_id;
    int32_t term_id;
    int parse_result;

    parse_result = aeron_uri_get_int32(uri_params, AERON_URI_INITIAL_TERM_ID_KEY, &initial_term_id);
    if (parse_result < 0)
    {
        return -1;
    }
    count += parse_result > 0 ? 1 : 0;

    parse_result = aeron_uri_get_int32(uri_params, AERON_URI_TERM_ID_KEY, &term_id);
    if (parse_result < 0)
    {
        return -1;
    }
    count += parse_result > 0 ? 1 : 0;

    const char *term_offset_str = aeron_uri_find_param_value(uri_params, AERON_URI_TERM_OFFSET_KEY);
    count += term_offset_str ? 1 : 0;

    if (count > 0)
    {
        char *end_ptr = NULL;

        if (!is_exclusive)
        {
            aeron_set_err(
                EINVAL, "params: %s %s %s are not supported for concurrent publications",
                AERON_URI_INITIAL_TERM_ID_KEY, AERON_URI_TERM_ID_KEY, AERON_URI_TERM_OFFSET_KEY);
            return -1;
        }
        if (count < 3)
        {
            aeron_set_err(
                EINVAL, "params must be used as a complete set: %s %s %s",
                AERON_URI_INITIAL_TERM_ID_KEY, AERON_URI_TERM_ID_KEY, AERON_URI_TERM_OFFSET_KEY);
            return -1;
        }

        errno = 0;
        end_ptr = NULL;
        uint64_t term_offset = strtoull(term_offset_str, &end_ptr, 0);
        if ((term_offset == 0 && 0 != errno) || end_ptr == term_offset_str)
        {
            aeron_set_err(
                EINVAL,
                "could not parse %s=%s in URI: %s", AERON_URI_TERM_OFFSET_KEY, term_offset_str, strerror(errno));
            return -1;
        }

        if (aeron_sub_wrap_i32(term_id, initial_term_id) < 0)
        {
            aeron_set_err(
                EINVAL,
                "Param difference greater than 2^31 - 1: %s=%" PRId32 " %s=%" PRId32,
                AERON_URI_INITIAL_TERM_ID_KEY,
                initial_term_id,
                AERON_URI_TERM_OFFSET_KEY,
                term_id);
            return -1;
        }

        if (term_offset > params->term_length)
        {
            aeron_set_err(
                EINVAL,
                "Param %s=%" PRIu64 " > %s=%" PRIu64,
                AERON_URI_TERM_OFFSET_KEY,
                term_offset,
                AERON_URI_TERM_LENGTH_KEY,
                params->term_length);
            return -1;
        }

        if ((term_offset & (AERON_LOGBUFFER_FRAME_ALIGNMENT - 1u)) != 0)
        {
            aeron_set_err(
                EINVAL,
                "Param %s=%" PRIu64 " must be multiple of FRAME_ALIGNMENT",
                AERON_URI_TERM_OFFSET_KEY,
                params->term_offset);
            return -1;
        }

        params->term_offset = term_offset;
        params->initial_term_id = initial_term_id;
        params->term_id = term_id;
        params->has_position = true;
    }

    if (aeron_uri_get_bool(uri_params, AERON_URI_SPARSE_TERM_KEY, &params->is_sparse) < 0)
    {
        return -1;
    }

    if (aeron_uri_get_bool(uri_params, AERON_URI_EOS_KEY, &params->signal_eos) < 0)
    {
        return -1;
    }

    if (aeron_uri_get_bool(uri_params, AERON_URI_SPIES_SIMULATE_CONNECTION_KEY, &params->spies_simulate_connection) < 0)
    {
        return -1;
    }

    return 0;
}

int aeron_uri_subscription_params(
    aeron_uri_t *uri, aeron_uri_subscription_params_t *params, aeron_driver_conductor_t *conductor)
{
    aeron_driver_context_t *context = conductor->context;

    params->is_reliable = context->reliable_stream;
    params->is_sparse = context->term_buffer_sparse_file;
    params->is_tether = context->tether_subscriptions;
    params->is_rejoin = context->rejoin_stream;

    aeron_uri_params_t *uri_params = AERON_URI_IPC == uri->type ?
        &uri->params.ipc.additional_params : &uri->params.udp.additional_params;

    if (aeron_uri_get_bool(uri_params, AERON_UDP_CHANNEL_RELIABLE_KEY, &params->is_reliable) < 0)
    {
        return -1;
    }

    if (aeron_uri_get_bool(uri_params, AERON_URI_SPARSE_TERM_KEY, &params->is_sparse) < 0)
    {
        return -1;
    }

    if (aeron_uri_get_bool(uri_params, AERON_URI_TETHER_KEY, &params->is_tether) < 0)
    {
        return -1;
    }

    if (aeron_uri_get_bool(uri_params, AERON_URI_REJOIN_KEY, &params->is_rejoin) < 0)
    {
        return -1;
    }

    params->group = aeron_config_parse_inferable_boolean(
        aeron_uri_find_param_value(uri_params, AERON_URI_GROUP_KEY), context->receiver_group_consideration);

    if (aeron_uri_subscription_session_id_param(uri_params, params) < 0)
    {
        return -1;
    }

    return 0;
}

int64_t aeron_uri_parse_tag(const char *tag_str)
{
    errno = 0;
    char *end_ptr = NULL;
    unsigned long value = strtoul(tag_str, &end_ptr, 10);

    if ((0 == value && 0 != errno) || end_ptr == tag_str)
    {
        return AERON_URI_INVALID_TAG;
    }

    return (int64_t)value;
}
