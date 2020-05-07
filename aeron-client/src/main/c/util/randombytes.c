// In the case that are compiling on linux, we need to define _GNU_SOURCE
// *before* randombytes.h is included. Otherwise SYS_getrandom will not be
// declared.
#if defined(__linux__)
# define _GNU_SOURCE
#endif /* defined(__linux__) */

#include "randombytes.h"

#if defined(_WIN32)
/* Windows */
# include <windows.h>
# include <wincrypt.h> /* CryptAcquireContext, CryptGenRandom */
#endif /* defined(_WIN32) */


#if defined(__linux__)
/* Linux */
// We would need to include <linux/random.h>, but not every target has access
// to the linux headers. We only need RNDGETENTCNT, so we instead inline it.
// RNDGETENTCNT is originally defined in `include/uapi/linux/random.h` in the
// linux repo.
# define RNDGETENTCNT 0x80045200

# include <assert.h>
# include <errno.h>
# include <fcntl.h>
# include <poll.h>
# include <stdint.h>
# include <stdio.h>
# include <sys/ioctl.h>
# include <sys/stat.h>
# include <sys/syscall.h>
# include <sys/types.h>
# include <unistd.h>

// We need SSIZE_MAX as the maximum read len from /dev/urandom
# if !defined(SSIZE_MAX)
#  define SSIZE_MAX (SIZE_MAX / 2 - 1)
# endif /* defined(SSIZE_MAX) */

#endif /* defined(__linux__) */


#if defined(__unix__) || (defined(__APPLE__) && defined(__MACH__))
/* Dragonfly, FreeBSD, NetBSD, OpenBSD (has arc4random) */
# include <sys/param.h>
# if defined(BSD)
#  include <stdlib.h>
# endif
#endif

#if defined(__EMSCRIPTEN__)
# include <assert.h>
# include <emscripten.h>
# include <errno.h>
# include <stdbool.h>
#endif /* defined(__EMSCRIPTEN__) */


#if defined(_WIN32)
static int randombytes_win32_randombytes(void* buf, const size_t n)
{
	HCRYPTPROV ctx;
	BOOL tmp;

	tmp = CryptAcquireContext(&ctx, NULL, NULL, PROV_RSA_FULL,
	                          CRYPT_VERIFYCONTEXT);
	if (tmp == FALSE) return -1;

	tmp = CryptGenRandom(ctx, n, (BYTE*) buf);
	if (tmp == FALSE) return -1;

	tmp = CryptReleaseContext(ctx, 0);
	if (tmp == FALSE) return -1;

	return 0;
}
#endif /* defined(_WIN32) */


#if defined(__linux__) && defined(SYS_getrandom)
static int randombytes_linux_randombytes_getrandom(void *buf, size_t n)
{
	/* I have thought about using a separate PRF, seeded by getrandom, but
	 * it turns out that the performance of getrandom is good enough
	 * (250 MB/s on my laptop).
	 */
	size_t offset = 0, chunk;
	int ret;
	while (n > 0) {
		/* getrandom does not allow chunks larger than 33554431 */
		chunk = n <= 33554431 ? n : 33554431;
		do {
			ret = syscall(SYS_getrandom, (char *)buf + offset, chunk, 0);
		} while (ret == -1 && errno == EINTR);
		if (ret < 0) return ret;
		offset += ret;
		n -= ret;
	}
	assert(n == 0);
	return 0;
}
#endif /* defined(__linux__) && defined(SYS_getrandom) */


#if defined(__linux__) && !defined(SYS_getrandom)
static int randombytes_linux_read_entropy_ioctl(int device, int *entropy)
{
	return ioctl(device, RNDGETENTCNT, entropy);
}

static int randombytes_linux_read_entropy_proc(FILE *stream, int *entropy)
{
	int retcode;
	do {
		rewind(stream);
		retcode = fscanf(stream, "%d", entropy);
	} while (retcode != 1 && errno == EINTR);
	if (retcode != 1) {
		return -1;
	}
	return 0;
}

static int randombytes_linux_wait_for_entropy(int device)
{
	/* We will block on /dev/random, because any increase in the OS' entropy
	 * level will unblock the request. I use poll here (as does libsodium),
	 * because we don't *actually* want to read from the device. */
	enum { IOCTL, PROC } strategy = IOCTL;
	const int bits = 128;
	struct pollfd pfd;
	int fd;
	FILE *proc_file;
	int retcode, retcode_error = 0; // Used as return codes throughout this function
	int entropy = 0;

	/* If the device has enough entropy already, we will want to return early */
	retcode = randombytes_linux_read_entropy_ioctl(device, &entropy);
	// printf("errno: %d (%s)\n", errno, strerror(errno));
	if (retcode != 0 && (errno == ENOTTY || errno == ENOSYS)) {
		// The ioctl call on /dev/urandom has failed due to a
		//   - ENOTTY (unsupported action), or
		//   - ENOSYS (invalid ioctl; this happens on MIPS, see #22).
		//
		// We will fall back to reading from
		// `/proc/sys/kernel/random/entropy_avail`.  This less ideal,
		// because it allocates a file descriptor, and it may not work
		// in a chroot.  But at this point it seems we have no better
		// options left.
		strategy = PROC;
		// Open the entropy count file
		proc_file = fopen("/proc/sys/kernel/random/entropy_avail", "r");
	} else if (retcode != 0) {
		// Unrecoverable ioctl error
		return -1;
	}
	if (entropy >= bits) {
		return 0;
	}

	do {
		fd = open("/dev/random", O_RDONLY);
	} while (fd == -1 && errno == EINTR); /* EAGAIN will not occur */
	if (fd == -1) {
		/* Unrecoverable IO error */
		return -1;
	}

	pfd.fd = fd;
	pfd.events = POLLIN;
	for (;;) {
		retcode = poll(&pfd, 1, -1);
		if (retcode == -1 && (errno == EINTR || errno == EAGAIN)) {
			continue;
		} else if (retcode == 1) {
			if (strategy == IOCTL) {
				retcode = randombytes_linux_read_entropy_ioctl(device, &entropy);
			} else if (strategy == PROC) {
				retcode = randombytes_linux_read_entropy_proc(proc_file, &entropy);
			} else {
				return -1; // Unreachable
			}

			if (retcode != 0) {
				// Unrecoverable I/O error
				retcode_error = retcode;
				break;
			}
			if (entropy >= bits) {
				break;
			}
		} else {
			// Unreachable: poll() should only return -1 or 1
			retcode_error = -1;
			break;
		}
	}
	do {
		retcode = close(fd);
	} while (retcode == -1 && errno == EINTR);
	if (strategy == PROC) {
		do {
			retcode = fclose(proc_file);
		} while (retcode == -1 && errno == EINTR);
	}
	if (retcode_error != 0) {
		return retcode_error;
	}
	return retcode;
}


static int randombytes_linux_randombytes_urandom(void *buf, size_t n)
{
	int fd;
	size_t offset = 0, count;
	ssize_t tmp;
	do {
		fd = open("/dev/urandom", O_RDONLY);
	} while (fd == -1 && errno == EINTR);
	if (fd == -1) return -1;
	if (randombytes_linux_wait_for_entropy(fd) == -1) return -1;

	while (n > 0) {
		count = n <= SSIZE_MAX ? n : SSIZE_MAX;
		tmp = read(fd, (char *)buf + offset, count);
		if (tmp == -1 && (errno == EAGAIN || errno == EINTR)) {
			continue;
		}
		if (tmp == -1) return -1; /* Unrecoverable IO error */
		offset += tmp;
		n -= tmp;
	}
	assert(n == 0);
	return 0;
}
#endif /* defined(__linux__) && !defined(SYS_getrandom) */


#if defined(BSD)
static int randombytes_bsd_randombytes(void *buf, size_t n)
{
	arc4random_buf(buf, n);
	return 0;
}
#endif /* defined(BSD) */


#if defined(__EMSCRIPTEN__)
static int randombytes_js_randombytes_nodejs(void *buf, size_t n) {
	const int ret = EM_ASM_INT({
		var crypto;
		try {
			crypto = require('crypto');
		} catch (error) {
			return -2;
		}
		try {
			writeArrayToMemory(crypto.randomBytes($1), $0);
			return 0;
		} catch (error) {
			return -1;
		}
	}, buf, n);
	switch (ret) {
	case 0:
		return 0;
	case -1:
		errno = EINVAL;
		return -1;
	case -2:
		errno = ENOSYS;
		return -1;
	}
	assert(false); // Unreachable
}
#endif /* defined(__EMSCRIPTEN__) */


int randombytes(void *buf, size_t n)
{
#if defined(__EMSCRIPTEN__)
# pragma message("Using crypto api from NodeJS")
	return randombytes_js_randombytes_nodejs(buf, n);
#elif defined(__linux__)
# if defined(SYS_getrandom)
#  pragma message("Using getrandom system call")
	/* Use getrandom system call */
	return randombytes_linux_randombytes_getrandom(buf, n);
# else
#  pragma message("Using /dev/urandom device")
	/* When we have enough entropy, we can read from /dev/urandom */
	return randombytes_linux_randombytes_urandom(buf, n);
# endif
#elif defined(BSD)
# pragma message("Using arc4random system call")
	/* Use arc4random system call */
	return randombytes_bsd_randombytes(buf, n);
#elif defined(_WIN32)
# pragma message("Using Windows cryptographic API")
	/* Use windows API */
	return randombytes_win32_randombytes(buf, n);
#else
# error "randombytes(...) is not supported on this platform"
#endif
}
