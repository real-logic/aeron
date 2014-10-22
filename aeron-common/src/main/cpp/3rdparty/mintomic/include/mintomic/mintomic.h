#ifndef __MINTOMIC_MINTOMIC_H__
#define __MINTOMIC_MINTOMIC_H__

#include "core.h"

#ifdef __cplusplus
extern "C" {
#endif


//--------------------------------------------------------------
//  Platform-specific fences and atomic RMW operations
//--------------------------------------------------------------
#if MINT_COMPILER_MSVC
    #include "private/mintomic_msvc.h"
#elif MINT_COMPILER_GCC && (MINT_CPU_X86 || MINT_CPU_X64)
    #include "private/mintomic_gcc_x86-64.h"
#elif MINT_COMPILER_GCC && MINT_CPU_ARM
    #include "private/mintomic_gcc_arm.h"
#else
    #error Unsupported platform!
#endif

//--------------------------------------------------------------
//  Pointer-sized atomic RMW operation wrappers
//--------------------------------------------------------------
#if MINT_PTR_SIZE == 4
    MINT_C_INLINE void *mint_load_ptr_relaxed(mint_atomicPtr_t *object)
    {
        return (void *) mint_load_32_relaxed((mint_atomic32_t *) object);
    }
    MINT_C_INLINE void mint_store_ptr_relaxed(mint_atomicPtr_t *object, void *desired)
    {
        mint_store_32_relaxed((mint_atomic32_t *) object, (size_t) desired);
    }
    MINT_C_INLINE void *mint_compare_exchange_strong_ptr_relaxed(mint_atomicPtr_t *object, void *expected, void *desired)
    {
        return (void *) mint_compare_exchange_strong_32_relaxed((mint_atomic32_t *) object, (size_t) expected, (size_t) desired);
    }
    MINT_C_INLINE void *mint_fetch_add_ptr_relaxed(mint_atomicPtr_t *object, ptrdiff_t operand)
    {
        return (void *) mint_fetch_add_32_relaxed((mint_atomic32_t *) object, operand);
    }
    MINT_C_INLINE void *mint_fetch_and_ptr_relaxed(mint_atomicPtr_t *object, size_t operand)
    {
        return (void *) mint_fetch_and_32_relaxed((mint_atomic32_t *) object, operand);
    }
    MINT_C_INLINE void *mint_fetch_or_ptr_relaxed(mint_atomicPtr_t *object, size_t operand)
    {
        return (void *) mint_fetch_or_32_relaxed((mint_atomic32_t *) object, operand);
    }
#elif MINT_PTR_SIZE == 8
    MINT_C_INLINE void *mint_load_ptr_relaxed(mint_atomicPtr_t *object)
    {
        return (void *) mint_load_64_relaxed((mint_atomic64_t *) object);
    }
    MINT_C_INLINE void mint_store_ptr_relaxed(mint_atomicPtr_t *object, void *desired)
    {
        mint_store_64_relaxed((mint_atomic64_t *) object, (size_t) desired);
    }
    MINT_C_INLINE void *mint_compare_exchange_strong_ptr_relaxed(mint_atomicPtr_t *object, void *expected, void *desired)
    {
        return (void *) mint_compare_exchange_strong_64_relaxed((mint_atomic64_t *) object, (size_t) expected, (size_t) desired);
    }
    MINT_C_INLINE void *mint_fetch_add_ptr_relaxed(mint_atomicPtr_t *object, ptrdiff_t operand)
    {
        return (void *) mint_fetch_add_64_relaxed((mint_atomic64_t *) object, operand);
    }
    MINT_C_INLINE void *mint_fetch_and_ptr_relaxed(mint_atomicPtr_t *object, size_t operand)
    {
        return (void *) mint_fetch_and_64_relaxed((mint_atomic64_t *) object, operand);
    }
    MINT_C_INLINE void *mint_fetch_or_ptr_relaxed(mint_atomicPtr_t *object, size_t operand)
    {
        return (void *) mint_fetch_or_64_relaxed((mint_atomic64_t *) object, operand);
    }
#else
    #error MINT_PTR_SIZE not set!
#endif


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTOMIC_MINTOMIC_H__
