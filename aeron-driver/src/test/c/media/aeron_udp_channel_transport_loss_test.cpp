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

#include <gtest/gtest.h>
#include <media/aeron_udp_channel_transport.h>

extern "C"
{
#include <media/aeron_udp_channel_transport_loss.h>
#include <uri/aeron_uri.h>
#include <protocol/aeron_udp_protocol.h>

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif
}

class UdpChannelTransportLossTest : public testing::Test
{
public:
    UdpChannelTransportLossTest()
    {
    }
};

void test_recv_callback(
    void *clientd,
    void *transport_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
}

static int delegate_return_packets_recvmmsg(
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen,
    int64_t *bytes_rcved,
    aeron_udp_transport_recv_func_t recv_func,
    void *clientd)
{
    const int16_t *msg_type = static_cast<int16_t *>(transport->dispatch_clientd);
    for (size_t i = 0; i < vlen; i++)
    {
        iovec *iovec = msgvec[i].msg_hdr.msg_iov;
        aeron_frame_header_t *frame_header = static_cast<aeron_frame_header_t *>(iovec[0].iov_base);
        frame_header->type = *msg_type;
        msgvec[i].msg_len = static_cast<unsigned int>(iovec[0].iov_len);

        recv_func(clientd, NULL, static_cast<uint8_t *>(iovec[0].iov_base), msgvec[i].msg_len, NULL);

        if (NULL != bytes_rcved)
        {
            *bytes_rcved += msgvec[i].msg_len;
        }
    }

    return static_cast<int>(vlen);
}

TEST_F(UdpChannelTransportLossTest, shouldDiscardAllPacketsWithRateOfOne)
{
    uint16_t msg_type = AERON_HDR_TYPE_DATA;
    aeron_udp_channel_transport_t transport;
    aeron_udp_channel_transport_bindings_t bindings;
    aeron_udp_channel_transport_loss_params_t params;
    transport.dispatch_clientd = reinterpret_cast<void *>(&msg_type);

    struct mmsghdr msgvec[2];
    const size_t vlen = 2;
    uint8_t data_0[1024];
    uint8_t data_1[1024];
    struct iovec vec[2];

    vec[0].iov_base = data_0;
    vec[0].iov_len = 1024;
    vec[1].iov_base = data_1;
    vec[1].iov_len = 1024;

    msgvec[0].msg_hdr.msg_iov = &vec[0];
    msgvec[1].msg_hdr.msg_iov = &vec[1];

    params.rate = 1.0;
    params.recv_msg_type_mask = 1U << msg_type;
    params.seed = 0;

    bindings.recvmmsg_func = delegate_return_packets_recvmmsg;

    aeron_udp_channel_transport_loss_load(&bindings);
    aeron_udp_channel_transport_loss_configure(&params);

    int messages_received = aeron_udp_channel_transport_loss_recvmmsg(
        &transport, msgvec, vlen, NULL, test_recv_callback, NULL);

    EXPECT_EQ(messages_received, 0);
}

TEST_F(UdpChannelTransportLossTest, shouldNotDiscardAllPacketsWithRateOfOneWithDifferentMessageType)
{
    uint16_t loss_msg_type = AERON_HDR_TYPE_DATA;
    uint16_t data_msg_type = AERON_HDR_TYPE_SETUP;
    aeron_udp_channel_transport_bindings_t bindings;
    aeron_udp_channel_transport_loss_params_t params;
    aeron_udp_channel_transport_t transport;
    transport.dispatch_clientd = reinterpret_cast<void *>(&data_msg_type);

    struct mmsghdr msgvec[2];
    const size_t vlen = 2;
    uint8_t data_0[1024];
    uint8_t data_1[1024];
    struct iovec vec[2];

    vec[0].iov_base = data_0;
    vec[0].iov_len = 1024;
    vec[1].iov_base = data_1;
    vec[1].iov_len = 1024;

    msgvec[0].msg_hdr.msg_iov = &vec[0];
    msgvec[1].msg_hdr.msg_iov = &vec[1];

    params.rate = 1.0;
    params.recv_msg_type_mask = 1U << loss_msg_type;
    params.seed = 0;

    bindings.recvmmsg_func = delegate_return_packets_recvmmsg;

    aeron_udp_channel_transport_loss_load(&bindings);
    aeron_udp_channel_transport_loss_configure(&params);

    int messages_received = aeron_udp_channel_transport_loss_recvmmsg(
        &transport, msgvec, vlen, NULL, test_recv_callback, NULL);

    EXPECT_EQ(messages_received, 2);
}

TEST_F(UdpChannelTransportLossTest, shouldNotDiscardAllPacketsWithRateOfZero)
{
    uint16_t loss_msg_type = AERON_HDR_TYPE_DATA;
    aeron_udp_channel_transport_bindings_t bindings;
    aeron_udp_channel_transport_loss_params_t params;
    aeron_udp_channel_transport_t transport;
    transport.dispatch_clientd = reinterpret_cast<void *>(&loss_msg_type);

    struct mmsghdr msgvec[2];
    const size_t vlen = 2;
    uint8_t data_0[1024];
    uint8_t data_1[1024];
    struct iovec vec[2];

    vec[0].iov_base = data_0;
    vec[0].iov_len = 1024;
    vec[1].iov_base = data_1;
    vec[1].iov_len = 1024;

    msgvec[0].msg_hdr.msg_iov = &vec[0];
    msgvec[1].msg_hdr.msg_iov = &vec[1];

    params.rate = 0.0;
    params.recv_msg_type_mask = 1U << loss_msg_type;
    params.seed = 0;

    bindings.recvmmsg_func = delegate_return_packets_recvmmsg;

    aeron_udp_channel_transport_loss_load(&bindings);
    aeron_udp_channel_transport_loss_configure(&params);

    int messages_received = aeron_udp_channel_transport_loss_recvmmsg(
        &transport, msgvec, vlen, NULL, test_recv_callback, NULL);

    EXPECT_EQ(messages_received, 2);
}

TEST_F(UdpChannelTransportLossTest, shouldDiscardRoughlyHalfTheMessages)
{
    uint16_t msg_type = AERON_HDR_TYPE_DATA;
    aeron_udp_channel_transport_bindings_t bindings;
    aeron_udp_channel_transport_loss_params_t params;
    aeron_udp_channel_transport_t transport;
    transport.dispatch_clientd = reinterpret_cast<void *>(&msg_type);
    int64_t bytes_received = 0;

    const size_t vlen = 10;
    struct mmsghdr msgvec[vlen];
    uint8_t data[vlen * 1024];
    struct iovec vec[vlen];

    for (size_t i = 0; i < vlen; i++)
    {
        vec[i].iov_base = &data[i * 1024];
        vec[i].iov_len = 1024;

        msgvec[i].msg_hdr.msg_iov = &vec[i];
    }

    params.rate = 0.5;
    params.recv_msg_type_mask = 1U << msg_type;
    params.seed = 23764;

    bindings.recvmmsg_func = delegate_return_packets_recvmmsg;

    aeron_udp_channel_transport_loss_load(&bindings);
    aeron_udp_channel_transport_loss_configure(&params);

    int messages_received = aeron_udp_channel_transport_loss_recvmmsg(
        &transport, msgvec, vlen, &bytes_received, test_recv_callback, NULL);

    EXPECT_LT(messages_received, static_cast<int>(vlen));
    EXPECT_GT(messages_received, 0);
    EXPECT_LT(bytes_received, static_cast<int64_t>(vlen * bytes_received));
    EXPECT_GT(bytes_received, 0);
    EXPECT_EQ(messages_received, 6);
}

TEST_F(UdpChannelTransportLossTest, shouldParseAllParams)
{
    aeron_udp_channel_transport_loss_params_t params;
    memset(&params, 0, sizeof(params));

    char *uri = strdup("rate=0.20|seed=10|recv-msg-mask=0xF");
    int i = aeron_udp_channel_transport_loss_parse_params(uri, &params);

    EXPECT_EQ(i, 0);
    EXPECT_EQ(params.rate, 0.2);
    EXPECT_EQ(params.seed, 10ull);
    EXPECT_EQ(params.recv_msg_type_mask, 0xFul);
}

TEST_F(UdpChannelTransportLossTest, shouldFailOnInvalidRate)
{
    aeron_udp_channel_transport_loss_params_t params;
    memset(&params, 0, sizeof(params));

    char *uri = strdup("rate=abc");
    int i = aeron_udp_channel_transport_loss_parse_params(uri, &params);

    EXPECT_EQ(i, -1);
}

TEST_F(UdpChannelTransportLossTest, shouldFailOnInvalidSeed)
{
    aeron_udp_channel_transport_loss_params_t params;
    memset(&params, 0, sizeof(params));

    char *uri = strdup("seed=abc");
    int i = aeron_udp_channel_transport_loss_parse_params(uri, &params);

    EXPECT_EQ(i, -1);
}

TEST_F(UdpChannelTransportLossTest, shouldFailOnInvalidRecvMsgMask)
{
    aeron_udp_channel_transport_loss_params_t params;
    memset(&params, 0, sizeof(params));

    char *uri = strdup("recv-msg-mask=zzz");
    int i = aeron_udp_channel_transport_loss_parse_params(uri, &params);

    EXPECT_EQ(i, -1);
}
