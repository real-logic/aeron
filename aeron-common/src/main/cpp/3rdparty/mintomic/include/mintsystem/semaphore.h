#ifndef __MINTSYSTEM_SEMAPHORE_H__
#define __MINTSYSTEM_SEMAPHORE_H__

#include <mintomic/core.h>

#if MINT_COMPILER_MSVC
    #include "private/semaphore_msvc.h"
#elif MINT_COMPILER_GCC
    #include "private/semaphore_gcc.h"
#else
    #error Unsupported platform!
#endif

#endif // __MINTSYSTEM_SEMAPHORE_H__
