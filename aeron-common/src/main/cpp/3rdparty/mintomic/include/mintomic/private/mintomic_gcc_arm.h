#ifndef __MINTOMIC_PRIVATE_MINTOMIC_GCC_ARM_H__
#define __MINTOMIC_PRIVATE_MINTOMIC_GCC_ARM_H__

#ifdef __cplusplus
extern "C" {
#endif


//-------------------------------------
//  Atomic types
//-------------------------------------
// In compilers targeting the "old" ABI, 64-bit values are not guaranteed 8-byte aligned.
// These wrappers enforce the correct alignment.
// Warning: Local variables on the stack may still not be aligned using some compilers:
// http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
// XCode 3.2.5 with iPhoneOS SDK 4.2 and GCC 4.2 demonstrates both problems.
#if MINT_HAS_C11_MEMORY_MODEL
    typedef struct { uint32_t _nonatomic; } __attribute__((aligned(4))) mint_atomic32_t;
    typedef struct { uint64_t _nonatomic; } __attribute__((aligned(8))) mint_atomic64_t;
    typedef struct { void *_nonatomic; } __attribute__((aligned(MINT_PTR_SIZE))) mint_atomicPtr_t;
#else
    typedef struct { volatile uint32_t _nonatomic; } __attribute__((aligned(4))) mint_atomic32_t;
    typedef struct { volatile uint64_t _nonatomic; } __attribute__((aligned(8))) mint_atomic64_t;
    typedef struct { void *volatile _nonatomic; } __attribute__((aligned(MINT_PTR_SIZE))) mint_atomicPtr_t;
#endif


//-------------------------------------
//  Fences
//-------------------------------------
#define mint_signal_fence_consume() (0)
#define mint_signal_fence_acquire() asm volatile("" ::: "memory")
#define mint_signal_fence_release() asm volatile("" ::: "memory")
#define mint_signal_fence_seq_cst() asm volatile("" ::: "memory")

#define mint_thread_fence_consume() (0)
#if MINT_CPU_ARM_VERSION == 7
    #define mint_thread_fence_acquire() asm volatile("dmb ish" ::: "memory")
    #define mint_thread_fence_release() asm volatile("dmb ish" ::: "memory")
    #define mint_thread_fence_seq_cst() asm volatile("dmb ish" ::: "memory")
#elif MINT_CPU_ARM_VERSION == 6
    // dmb instruction does not exist in ARMv6 and lower.
    // mcr instruction can accomplish the same thing.
    #if MINT_CPU_ARM_THUMB
        // When compiling for ARMv6 in Thumb mode, the mcr instruction is not available.
        // We need to switch to ARM mode, by calling standalone functions, to use this instruction.
        // As non-inlined function calls, they function as compiler barriers as well.
        void mint_thread_fence_acquire();
        void mint_thread_fence_release();
        void mint_thread_fence_seq_cst();
    #else
        // Do we really need to set the source register to 0?
        #define mint_thread_fence_acquire() asm volatile("mcr p15, 0, %0, c7, c10, 5" :: "r"(0) : "memory")
        #define mint_thread_fence_release() asm volatile("mcr p15, 0, %0, c7, c10, 5" :: "r"(0) : "memory")
        #define mint_thread_fence_seq_cst() asm volatile("mcr p15, 0, %0, c7, c10, 5" :: "r"(0) : "memory")
    #endif
#endif


//----------------------------------------------
//  32-bit atomic operations
//----------------------------------------------
MINT_C_INLINE uint32_t mint_load_32_relaxed(mint_atomic32_t *object)
{
    return object->_nonatomic;
}

MINT_C_INLINE void mint_store_32_relaxed(mint_atomic32_t *object, uint32_t desired)
{
    object->_nonatomic = desired;
}

#if (MINT_CPU_ARM_VERSION == 6) && MINT_CPU_ARM_THUMB
    // When compiling for ARMv6 in Thumb mode, the ldrex/strex instructions are not available.
    // We need to switch to ARM mode, by calling standalone functions, to use these instructions.
    uint32_t mint_compare_exchange_strong_32_relaxed(mint_atomic32_t *object, uint32_t expected, uint32_t desired);
    uint32_t mint_fetch_add_32_relaxed(mint_atomic32_t *object, int32_t operand);
    uint32_t mint_fetch_and_32_relaxed(mint_atomic32_t *object, uint32_t operand);
    uint32_t mint_fetch_or_32_relaxed(mint_atomic32_t *object, uint32_t operand);

#else
    MINT_C_INLINE uint32_t mint_compare_exchange_strong_32_relaxed(mint_atomic32_t *object, uint32_t expected, uint32_t desired)
    {
        uint32_t status;
        uint32_t original;
        // The "Ir" constraint allows the compiler to pass "expected" to the cmp instruction as an immediate
        // operand if it can. Otherwise, it falls back to loading it into a register first.
        asm volatile("1:     ldrex   %0, [%3]\n"
                     "       cmp     %0, %4\n"
                     "       bne     2f\n"
                     "       strex   %1, %5, [%3]\n"
                     "       cmp     %1, #0\n"
                     "       bne     1b\n"
                     "2:     ;"
                     : "=&r"(original), "=&r"(status), "+Qo"(object->_nonatomic)
                     : "r"(object), "Ir"(expected), "r"(desired)
                     : "cc");
        return original;
    }

    MINT_C_INLINE uint32_t mint_fetch_add_32_relaxed(mint_atomic32_t *object, int32_t operand)
    {
        uint32_t status;
        uint32_t original, desired;
        asm volatile("1:     ldrex   %0, [%4]\n"
                     "       mov     %3, %0\n"
                     "       add     %3, %5\n"
                     "       strex   %1, %3, [%4]\n"
                     "       cmp     %1, #0\n"
                     "       bne     1b"
                     : "=&r"(original), "=&r"(status), "+Qo"(object->_nonatomic), "=&r"(desired)
                     : "r"(object), "Ir"(operand)
                     : "cc");
        return original;
    }

    MINT_C_INLINE uint32_t mint_fetch_and_32_relaxed(mint_atomic32_t *object, uint32_t operand)
    {
        uint32_t status;
        uint32_t original, desired;
        asm volatile("1:     ldrex   %0, [%4]\n"
                     "       mov     %3, %0\n"
                     "       and     %3, %5\n"
                     "       strex   %1, %3, [%4]\n"
                     "       cmp     %1, #0\n"
                     "       bne     1b"
                     : "=&r"(original), "=&r"(status), "+Qo"(object->_nonatomic), "=&r"(desired)
                     : "r"(object), "Ir"(operand)
                     : "cc");
        return original;
    }

    MINT_C_INLINE uint32_t mint_fetch_or_32_relaxed(mint_atomic32_t *object, uint32_t operand)
    {
        uint32_t status;
        uint32_t original, desired;
        asm volatile("1:     ldrex   %0, [%4]\n"
                     "       mov     %3, %0\n"
                     "       orr     %3, %5\n"
                     "       strex   %1, %3, [%4]\n"
                     "       cmp     %1, #0\n"
                     "       bne     1b"
                     : "=&r"(original), "=&r"(status), "+Qo"(object->_nonatomic), "=&r"(desired)
                     : "r"(object), "Ir"(operand)
                     : "cc");
        return original;
    }
#endif


//----------------------------------------------
//  64-bit atomic operations
//----------------------------------------------
// Defined in mintomic_gcc.c
uint64_t mint_load_64_relaxed(mint_atomic64_t *object);
void mint_store_64_relaxed(mint_atomic64_t *object, uint64_t desired);
uint64_t mint_compare_exchange_strong_64_relaxed(mint_atomic64_t *object, uint64_t expected, uint64_t desired);
uint64_t mint_fetch_add_64_relaxed(mint_atomic64_t *object, int64_t operand);
uint64_t mint_fetch_and_64_relaxed(mint_atomic64_t *object, uint64_t operand);
uint64_t mint_fetch_or_64_relaxed(mint_atomic64_t *object, uint64_t operand);


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTOMIC_PRIVATE_MINTOMIC_GCC_ARM_H__
