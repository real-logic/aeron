#ifndef __MINTSYSTEM_TID_H__
#define __MINTSYSTEM_TID_H__

#include <mintomic/core.h>

#if MINT_COMPILER_MSVC
    #include "private/tid_msvc.h"
#elif MINT_COMPILER_GCC
    #include "private/tid_gcc.h"
#else
    #error Unsupported platform!
#endif

#endif // __MINTSYSTEM_TID_H__
