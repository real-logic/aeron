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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include "concurrent/aeron_thread.h"
#if !defined(_WIN32)
#include <unistd.h>
#endif

#define SECOND_AS_NANOSECONDS (1000l * 1000l * 1000l)

void aeron_nano_sleep(uint64_t nanoseconds)
{
#ifdef AERON_COMPILER_MSVC
    HANDLE timer = CreateWaitableTimer(NULL, TRUE, NULL);
    if (!timer)
    {
        return;
    }

    LARGE_INTEGER li;
    li.QuadPart = -(int64_t)(nanoseconds / 100);

    if (!SetWaitableTimer(timer, &li, 0, NULL, NULL, FALSE))
    {
        CloseHandle(timer);
        return;
    }

    WaitForSingleObject(timer, INFINITE);
    CloseHandle(timer);
#else
    time_t seconds = nanoseconds / SECOND_AS_NANOSECONDS;
    struct timespec ts =
    {
        .tv_sec = seconds,
        .tv_nsec = (long)nanoseconds - (seconds * SECOND_AS_NANOSECONDS)
    };

    nanosleep(&ts, NULL);
#endif
}

void aeron_micro_sleep(size_t microseconds)
{
#ifdef _WIN32
    aeron_nano_sleep(1000 * microseconds);
#else
    usleep(microseconds);
#endif
}

#if defined(AERON_COMPILER_GCC)

void aeron_thread_set_name(const char* role_name)
{
#if defined(Darwin)
    pthread_setname_np(role_name);
#else
    pthread_setname_np(pthread_self(), role_name);
#endif
}

#elif defined(AERON_COMPILER_MSVC)

static BOOL aeron_thread_once_callback(PINIT_ONCE init_once, void (*callback)(void), void** context)
{
    callback();
    return TRUE;
}

void aeron_thread_once(AERON_INIT_ONCE* s_init_once, void* callback)
{
    InitOnceExecuteOnce(s_init_once, aeron_thread_once_callback, callback, NULL);
}

int aeron_mutex_init(aeron_mutex_t* mutex, void* attr)
{
    *mutex = CreateMutexA(NULL, FALSE, NULL);
    return *mutex ? 0 : -1;
}

int aeron_mutex_lock(aeron_mutex_t* mutex)
{
    return WaitForSingleObject(*mutex, INFINITE) == WAIT_OBJECT_0 ? 0 : EINVAL;
}

int aeron_mutex_unlock(aeron_mutex_t* mutex)
{
    return ReleaseMutex(*mutex) ? 0 : EINVAL;
}

int aeron_mutex_destroy(aeron_mutex_t* mutex)
{
    if (*mutex)
    {
        CloseHandle(*mutex);
        *mutex = 0;
        return 0;
    }

    return EINVAL;
}

int aeron_thread_attr_init(pthread_attr_t* attr)
{
    return 0;
}

static DWORD WINAPI aeron_thread_proc(LPVOID parameter)
{
    aeron_thread_t* thread = (aeron_thread_t*)parameter;
    thread->result = thread->callback(thread->arg0);
    return 0;
}

int aeron_thread_create(aeron_thread_t* thread, void* attr, void*(*callback)(void*), void* arg0)
{
    thread->callback = callback;
    thread->arg0 = arg0;
	
    DWORD id;
    thread->handle = CreateThread(
        NULL,              // default security attributes
        0,                 // use default stack size
        aeron_thread_proc, // thread function name
        thread,            // argument to thread function
        0,                 // use default creation flags
        &id);              // returns the thread identifier

    return thread->handle ? 0 : -1;
}

void aeron_thread_set_name(const char* role_name)
{
    size_t wn = mbstowcs(NULL, role_name, 0);
    wchar_t * buf = malloc(sizeof(wchar_t) * (wn + 1));  // value-initialize to 0 (see below)
    if (!buf)
    {
        return;
    }
	
    mbstowcs(buf, role_name, wn + 1);
    SetThreadDescription(GetCurrentThread(), buf);

    free(buf);
}

int aeron_thread_join(aeron_thread_t thread, void **value_ptr)
{
    if (thread.handle)
    {
        WaitForSingleObject(thread.handle, INFINITE);
        CloseHandle(thread.handle);
    }
    else
    {
        return EINVAL;
    }

    if (value_ptr)
    {
        *value_ptr = thread.result;
    }

    return 0;
}

int aeron_thread_key_create(pthread_key_t *key, void(*destr_function) (void *))
{
    DWORD dkey = TlsAlloc();
    if (dkey != TLS_OUT_OF_INDEXES)
    {
        *key = dkey;
        return 0;
    }
    else
    {
        return EAGAIN;
    }
}

int aeron_thread_key_delete(pthread_key_t key)
{
    if (TlsFree(key))
    {
        return 0;
    }
    else
    {
        return EINVAL;
    }
}

int aeron_thread_set_specific(pthread_key_t key, const void *pointer)
{
    if (TlsSetValue(key, (LPVOID)pointer))
    {
        return 0;
    }
    else
    {
        return EINVAL;
    }
}

void * aeron_thread_get_specific(pthread_key_t key)
{
    return TlsGetValue(key);
}


#else
#error Unsupported platform!
#endif

 // sched

#if defined(AERON_COMPILER_GCC)

#include <sched.h>

void proc_yield()
{
    __asm__ volatile("pause\n": : : "memory");
}

#elif defined(AERON_COMPILER_MSVC)

#else
#error Unsupported platform!
#endif
