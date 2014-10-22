#ifndef __MINTOMIC_CORE_H__
#define __MINTOMIC_CORE_H__

#include "platform_detect.h"


#if MINT_HAS_STDINT
    #include <stdint.h>
#else
    #include "private/stdint.h"
#endif

#if MINT_COMPILER_MSVC
    #include "private/core_msvc.h"
#elif MINT_COMPILER_GCC
    #include "private/core_gcc.h"
#else
    #error Unsupported platform!
#endif


#endif // __MINTOMIC_CORE_H__
