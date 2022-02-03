//
// Created by mike on 31/01/22.
//


#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <errno.h>
#include <inttypes.h>
#include <time.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <net/if.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <poll.h>
#include <unistd.h>
#include <fcntl.h>

#define E(x)    \
do              \
{               \
    if (x < 0)  \
    {           \
        fprintf(stderr, "Failed: %d, %s\n", errno, strerror(errno)); \
        abort();     \
    }            \
}               \
while (0)       \

int init_addr_in(struct sockaddr *addr, const char *ip, uint16_t port)
{
    struct sockaddr_in *addr_in = (struct sockaddr_in *)addr;
    addr_in->sin_family = AF_INET;
    inet_pton(AF_INET, ip, (void *)&addr_in->sin_addr);
    addr_in->sin_port = htons(port);

    return 0;
}

int main(int argc, char **argv)
{
    uint8_t buf1[128];
    uint8_t buf2[128];

    memset(buf1, 'x', sizeof(buf1));
    memset(buf2, 'y', sizeof(buf2));

    struct iovec iovec[] =
        {
            {
                .iov_base = buf1,
                .iov_len = sizeof(buf1)
            },
            {
                .iov_base = buf2,
                .iov_len = sizeof(buf2)
            }
        };

    int send_fd = socket(AF_INET, SOCK_DGRAM, 0);
    E(send_fd);

    struct sockaddr_storage addr;
    struct sockaddr_storage addr2;

    init_addr_in((struct sockaddr *)&addr, "127.0.0.1", 10000);
    init_addr_in((struct sockaddr *)&addr2, "192.168.178.21", 10001);

    struct msghdr msg;
    msg.msg_control = NULL;
    msg.msg_controllen = 0;
    msg.msg_name = &addr;
    msg.msg_namelen = sizeof(struct sockaddr_in);
    msg.msg_iov = iovec;
    msg.msg_iovlen = 2;
    msg.msg_flags = 0;

    ssize_t result = sendmsg(send_fd, &msg, 0);

    E(result);

    fprintf(stdout, "%ld\n", iovec[0].iov_len);

    struct mmsghdr msgs[2];
    msgs[0].msg_hdr.msg_control = NULL;
    msgs[0].msg_hdr.msg_controllen = 0;
    msgs[0].msg_hdr.msg_name = &addr;
    msgs[0].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
    msgs[0].msg_hdr.msg_iov = &iovec[0];
    msgs[0].msg_hdr.msg_iovlen = 1;
    msgs[0].msg_hdr.msg_flags = 0;

    msgs[1].msg_hdr.msg_control = NULL;
    msgs[1].msg_hdr.msg_controllen = 0;
    msgs[1].msg_hdr.msg_name = &addr2;
    msgs[1].msg_hdr.msg_namelen = sizeof(struct sockaddr_in);
    msgs[1].msg_hdr.msg_iov = &iovec[1];
    msgs[1].msg_hdr.msg_iovlen = 1;
    msgs[1].msg_hdr.msg_flags = 0;

    result = sendmmsg(send_fd, msgs, 2, 0);
}
