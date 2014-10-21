#ifndef __MINTOMIC_PRIVATE_CORE_GCC_H__
#define __MINTOMIC_PRIVATE_CORE_GCC_H__

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif


//-------------------------------------
//  Alignment
//-------------------------------------
// Note: May not work on local variables.
// http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
#define MINT_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))


//-------------------------------------
//  Inlining
//-------------------------------------
#define MINT_C_INLINE static inline
#define MINT_NO_INLINE __attribute__((noinline))


//-------------------------------------
//  Thread local
//-------------------------------------
#define MINT_THREAD_LOCAL __thread


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTOMIC_PRIVATE_CORE_GCC_H__
