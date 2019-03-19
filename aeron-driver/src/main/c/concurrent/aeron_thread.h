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

#ifndef AERON_THREAD_H
#define AERON_THREAD_H

#include <util/aeron_platform.h>

#include <stdint.h>


#if defined(AERON_COMPILER_GCC)

    #include <pthread.h>
    #define AERON_MUTEX pthread_mutex_t
    #define AERON_INIT_ONCE pthread_once_t
    #define AERON_INIT_ONCE_VALUE PTHREAD_ONCE_INIT

    typedef pthread_t aeron_thread_t;
    #define aeron_mutex_init pthread_mutex_init
    #define aeron_mutex_lock pthread_mutex_lock
    #define aeron_mutex_unlock pthread_mutex_unlock
	#define aeron_thread_once pthread_once 
	#define aeron_thread_attr_init pthread_attr_init 
	#define aeron_thread_create pthread_create 
	#define aeron_thread_set_name pthread_setname_np 
	#define aeron_thread_self pthread_self 
	#define aeron_thread_join pthread_join 
	#define aeron_thread_key_create pthread_key_create 
	#define aeron_thread_key_delete pthread_key_delete 
	#define aeron_thread_get_specific pthread_getspecific 
	#define aeron_thread_set_specific pthread_setspecific 

#elif defined(AERON_COMPILER_MSVC)

    #include <WinSock2.h>
    #include <windows.h>
    #include <winnt.h>

    #define AERON_MUTEX HANDLE

    typedef HANDLE aeron_thread_t;

    typedef SSIZE_T ssize_t;
    typedef INIT_ONCE AERON_INIT_ONCE;

    typedef DWORD pthread_attr_t;
    typedef DWORD pthread_key_t;

    #define AERON_INIT_ONCE_VALUE INIT_ONCE_STATIC_INIT;

    void aeron_thread_once(AERON_INIT_ONCE* s_init_once, void* callback);

    void aeron_mutex_init(HANDLE* mutex, void* attr);

    void aeron_mutex_lock(HANDLE* mutex);

    void aeron_mutex_unlock(HANDLE* mutex);

    int aeron_thread_attr_init(pthread_attr_t* attr);

    int aeron_thread_create(aeron_thread_t* thread, void* attr, void*(*callback)(void*), void* arg0);

    void aeron_thread_set_name(aeron_thread_t self, const char* role_name);

    aeron_thread_t aeron_thread_self();

    DWORD aeron_thread_join(aeron_thread_t thread, void **value_ptr);

    int aeron_thread_key_create(pthread_key_t *key, void(*destr_function) (void *));

    int aeron_thread_key_delete(pthread_key_t key);

    int aeron_thread_set_specific(pthread_key_t key, const void *pointer);

    void* aeron_thread_get_specific(pthread_key_t key);


#else
#error Unsupported platform!
#endif

// sched

void aeron_nano_sleep(uint64_t nanoseconds);

#if defined(AERON_COMPILER_GCC)

#include <sched.h>

#define aeron_micro_sleep usleep

void proc_yield();

#elif defined(AERON_COMPILER_MSVC) && defined(AERON_CPU_X64)

#define sched_yield SwitchToThread
#define proc_yield _mm_pause

void aeron_micro_sleep(size_t microseconds);

#else
#error Unsupported platform!
#endif


#endif //AERON_THREAD_H
