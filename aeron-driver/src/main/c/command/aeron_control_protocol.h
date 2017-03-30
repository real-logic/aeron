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

#ifndef AERON_AERON_CONTROL_PROTOCOL_H
#define AERON_AERON_CONTROL_PROTOCOL_H

#include <stdint.h>

#define AERON_COMMAND_ADD_PUBLICATION (0x01)
#define AERON_COMMAND_REMOVE_PUBLICATION (0x02)
#define AERON_COMMAND_ADD_SUBSCRIPTION (0x04)
#define AERON_COMMAND_REMOVE_SUBSCRIPTION (0x05)
#define AERON_COMMAND_CLIENT_KEEPALIVE (0x06)
#define AERON_COMMAND_ADD_DESTINATION (0x07)
#define AERON_COMMAND_REMOVE_DESTINATION (0x08)

#define AERON_RESPONSE_ON_ERROR (0x0F01)
#define AERON_RESPONSE_ON_AVAILABLE_IMAGE (0x0F02)
#define AERON_RESPONSE_ON_PUBLICATION_READY (0x0F03)
#define AERON_RESPONSE_ON_OPERATION_SUCCESS (0x0F04)
#define AERON_RESPONSE_ON_UNAVAILABLE_IMAGE (0x0F05)

/* error codes */
#define AERON_ERROR_CODE_GENERIC_ERROR (0)
#define AERON_ERROR_CODE_INVALID_CHANNEL (1)
#define AERON_ERROR_CODE_UNKNOWN_SUBSCRIPTION (2)
#define AERON_ERROR_CODE_UNKNOWN_PUBLICAITON (3)
#define AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID (4)
#define AERON_ERROR_CODE_MALFORMED_COMMAND (5)

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_correlated_command_stct
{
    int64_t client_id;
    int64_t correlation_id;
}
aeron_correlated_command_t;

typedef struct aeron_publication_command_stct
{
    aeron_correlated_command_t correlated;
    int32_t stream_id;
    int32_t channel_length;
    int8_t  channel_data[1];
}
aeron_publication_command_t;

typedef struct aeron_publication_buffers_ready_stct
{
    int64_t correlation_id;
    int32_t session_id;
    int32_t stream_id;
    int32_t position_imit_counter_id;
    int32_t log_file_length;
    int8_t  log_file_data[1];
}
aeron_publication_buffers_ready_t;

typedef struct aeron_subscription_command_stct
{
    aeron_correlated_command_t correlated;
    int64_t registration_correlation_id;
    int32_t stream_id;
    int32_t channel_length;
    int8_t  channel_data[1];
}
aeron_subscription_command_t;

typedef struct aeron_image_buffers_ready_stct
{
    int64_t correlation_id;
    int32_t session_id;
    int32_t stream_id;
    int32_t subscriber_position_block_length;
    int32_t subscriber_position_count;
}
aeron_image_buffers_ready_t;

typedef struct aeron_image_buffers_ready_subscriber_position_stct
{
    int32_t indicator_id;
    int64_t registration_id;
}
aeron_image_buffers_ready_subscriber_position_t;

typedef struct aeron_error_response_stct
{
    int64_t offending_command_correlation_id;
    int32_t error_code;
    int32_t error_message_length;
    int8_t  error_message_data[1];
}
aeron_error_response_t;

typedef struct aeron_remove_command_stct
{
    aeron_correlated_command_t correlated;
    int64_t registration_id;
}
aeron_remove_command_t;
#pragma pack(pop)


#endif //AERON_AERON_CONTROL_PROTOCOL_H
