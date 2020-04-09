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


#include <vector>
#include "aeron_receiver_test.h"

extern "C"
{
#include <util/aeron_fileutil.h>
#include <concurrent/aeron_atomic.h>
#include <concurrent/aeron_distinct_error_log.h>
#include <aeron_publication_image.h>
#include <aeron_data_packet_dispatcher.h>
#include <aeron_driver_receiver.h>
#include <aeron_position.h>
#include <aeron_publication_image.h>
#include <media/aeron_receive_channel_endpoint.h>
#include <media/aeron_receive_destination.h>
#include "aeron_test_udp_bindings.h"

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context);
}

#define CAPACITY (32 * 1024)
#define TERM_BUFFER_SIZE (64 * 1024)
#define MTU (4096)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 2 * CAPACITY> buffer_2x_t;

class PublicationImageTest : public ReceiverTestBase
{
};

TEST_F(PublicationImageTest, shouldAddAndRemoveDestination)
{
    const char *uri_1 = "aeron:udp?endpoint=localhost:9090";
    const char *uri_2 = "aeron:udp?endpoint=localhost:9091";
    const char *uri_3 = "aeron:udp?endpoint=localhost:9093";
    aeron_receive_channel_endpoint_t *endpoint = createMdsEndpoint();
    int32_t stream_id = 1001;
    int32_t session_id = 1000001;

    aeron_udp_channel_t *channel_1;
    aeron_receive_destination_t *dest_1;

    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &channel_1);

    ASSERT_LE(0, aeron_receive_destination_create(&dest_1, channel_1, m_context));
    ASSERT_EQ(1, aeron_receive_channel_endpoint_add_destination(endpoint, dest_1));

    aeron_publication_image_t *image = createImage(endpoint, dest_1, stream_id, session_id);

    aeron_udp_channel_t *channel_2;
    aeron_receive_destination_t *dest_2;

    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &channel_2);

    ASSERT_LE(0, aeron_receive_destination_create(&dest_2, channel_2, m_context));
    ASSERT_EQ(2, aeron_receive_channel_endpoint_add_destination(endpoint, dest_2));

    ASSERT_EQ(2, aeron_publication_image_add_destination(image, dest_2));

    aeron_udp_channel_t *remove_channel_1;
    aeron_udp_channel_parse(strlen(uri_1), uri_1, &m_resolver, &remove_channel_1);

    ASSERT_EQ(1, aeron_receive_channel_endpoint_remove_destination(endpoint, remove_channel_1));
    ASSERT_EQ(1u, endpoint->destinations.length);
    ASSERT_EQ(1, aeron_publication_image_remove_destination(image, remove_channel_1));
    ASSERT_EQ(1u, image->connections.length);

    aeron_udp_channel_t *channel_not_added;
    aeron_udp_channel_parse(strlen(uri_3), uri_3, &m_resolver, &channel_not_added);

    ASSERT_EQ(0, aeron_receive_channel_endpoint_remove_destination(endpoint, channel_not_added));
    ASSERT_EQ(1u, endpoint->destinations.length);
    ASSERT_EQ(0, aeron_publication_image_remove_destination(image, channel_not_added));
    ASSERT_EQ(1u, image->connections.length);

    aeron_udp_channel_t *remove_channel_2;
    aeron_udp_channel_parse(strlen(uri_2), uri_2, &m_resolver, &remove_channel_2);

    ASSERT_EQ(1, aeron_receive_channel_endpoint_remove_destination(endpoint, remove_channel_2));
    ASSERT_EQ(0u, endpoint->destinations.length);
    ASSERT_EQ(1, aeron_publication_image_remove_destination(image, remove_channel_2));
    ASSERT_EQ(0u, image->connections.length);
}

