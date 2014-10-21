#ifndef __MINTOMIC_PRIVATE_CORE_MSVC_H__
#define __MINTOMIC_PRIVATE_CORE_MSVC_H__

#if MINT_TARGET_XBOX_360    // Xbox 360
    #include <xtl.h>
#else                       // Windows
    #ifndef WIN32_LEAN_AND_MEAN
        #define WIN32_LEAN_AND_MEAN
    #endif
    #include <windows.h>
    #undef WIN32_LEAN_AND_MEAN
    #include <intrin.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif


//-------------------------------------
//  Alignment
//-------------------------------------
#define MINT_DECL_ALIGNED(declaration, amt) __declspec(align(amt)) declaration


//-------------------------------------
//  Inlining
//-------------------------------------
#ifdef __cplusplus
#define MINT_C_INLINE inline
#else
#define MINT_C_INLINE __inline
#endif

#define MINT_NO_INLINE __declspec(noinline)


//-------------------------------------
//  Thread local
//-------------------------------------
#define MINT_THREAD_LOCAL __declspec(thread)


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTOMIC_PRIVATE_CORE_MSVC_H__
