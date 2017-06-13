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

#include "uri/aeron_uri.h"

typedef enum aeron_uri_parser_state_enum
{
    PARAM_KEY, PARAM_VALUE
}
aeron_uri_parser_state_t;

int aeron_uri_parse(char *uri, aeron_uri_parse_callback_t param_func, void *clientd)
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
        if (aeron_uri_params_ensure_capacity(&params->additional_params) < 0)
        {
            return -1;
        }

        aeron_uri_param_t *param = &params->additional_params.array[params->additional_params.length - 1];

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

    return aeron_uri_parse(uri, aeron_udp_uri_params_func, params);
}

extern int aeron_uri_params_ensure_capacity(aeron_uri_params_t *params);
