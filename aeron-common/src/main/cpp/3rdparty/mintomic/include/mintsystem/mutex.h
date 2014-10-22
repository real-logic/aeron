#ifndef __MINTSYSTEM_MUTEX_H__
#define __MINTSYSTEM_MUTEX_H__

#include <mintomic/core.h>

#if MINT_COMPILER_MSVC
    #include "private/mutex_msvc.h"
#elif MINT_COMPILER_GCC
    #include "private/mutex_gcc.h"
#else
    #error Unsupported platform!
#endif

#endif // __MINTSYSTEM_MUTEX_H__
