#ifndef __MINTOMIC_PRIVATE_MINTOMIC_GCC_X86_64_H__
#define __MINTOMIC_PRIVATE_MINTOMIC_GCC_X86_64_H__

#ifdef __cplusplus
extern "C" {
#endif


//-------------------------------------
//  Atomic types
//-------------------------------------
#if MINT_HAS_C11_MEMORY_MODEL
    typedef struct { uint32_t _nonatomic; } __attribute__((aligned(4))) mint_atomic32_t;
    typedef struct { uint64_t _nonatomic; } __attribute__((aligned(8))) mint_atomic64_t;
    typedef struct { void *_nonatomic; } __attribute__((aligned(MINT_PTR_SIZE))) mint_atomicPtr_t;
#else
    // Without a C/C++11 memory model, we need to declare shared values volatile to
    // prevent out-of-thin-air stores
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
#define mint_thread_fence_acquire() asm volatile("" ::: "memory")
#define mint_thread_fence_release() asm volatile("" ::: "memory")
#if MINT_CPU_X64
#define mint_thread_fence_seq_cst() asm volatile("lock; orl $0, (%%rsp)" ::: "memory")
#else
#define mint_thread_fence_seq_cst() asm volatile("lock; orl $0, (%%esp)" ::: "memory")
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

MINT_C_INLINE uint32_t mint_compare_exchange_strong_32_relaxed(mint_atomic32_t *object, uint32_t expected, uint32_t desired)
{
    // CMPXCHG is written cmpxchgl because GCC (and Clang) uses AT&T assembler syntax.
    // Also due to AT&T syntax, the operands are swapped: %1 is the destination.
    // (This is the opposite of how Intel syntax lists the operands, where the destination comes first.)
    // "=a"(original) means the asm block outputs EAX to original, because CMPXCHG puts the old value in EAX.
    // "+m"(object->_nonatomic) is the memory address that is read/written. This becomes %1.
    // "q"(desired) puts desired into any of EBX, ECX or EDX before the block. This becomes %2.
    // "0"(expected) puts expected in the same register as "=a"(original), which is EAX, before the block.
    // Not putting "memory" in the clobber list because the operation is relaxed. It's OK for the compiler
    // to reorder this atomic followed by a load, for example. If the programmer wants to enforce ordering,
    // they will use an explicit fence.
    // http://www.ibiblio.org/gferg/ldp/GCC-Inline-Assembly-HOWTO.html
    // http://gcc.gnu.org/onlinedocs/gcc/Simple-Constraints.html#Simple-Constraints
    // http://gcc.gnu.org/onlinedocs/gcc/Modifiers.html#Modifiers
    // http://gcc.gnu.org/onlinedocs/gcc/Machine-Constraints.html#Machine-Constraints
    // http://gcc.gnu.org/onlinedocs/gcc/Extended-Asm.html
    // http://download.intel.com/products/processor/manual/325383.pdf
    uint32_t original;
    asm volatile("lock; cmpxchgl %2, %1"
                 : "=a"(original), "+m"(object->_nonatomic)
                 : "q"(desired), "0"(expected));
    return original;
}

MINT_C_INLINE uint32_t mint_fetch_add_32_relaxed(mint_atomic32_t *object, int32_t operand)
{
    // "=r"(original) chooses any general register, makes that %0, and outputs this register to original after the block.
    // "+m"(object->_nonatomic) is the memory address that is read/written. This becomes %1.
    // "0"(operand) puts operand into the same register as %0 before the block.
    // volatile is required. Otherwise, if the return value (original) is unused, the asm block
    // may be deleted. ("+m" is apparently not enough hint to the compiler that the asm
    // block has side effects on memory.)
    uint32_t original;
    asm volatile("lock; xaddl %0, %1"
                 : "=r"(original), "+m"(object->_nonatomic)
                 : "0"(operand));
    return original;
}

MINT_C_INLINE uint32_t mint_fetch_and_32_relaxed(mint_atomic32_t *object, uint32_t operand)
{
    // The & in "=&a"(original) makes eax an earlyclobber operand.
    // If we don't specify &, the compiler may assign eax to input operand %3 as well.
    uint32_t original;
    register uint32_t temp;
    asm volatile("1:     movl    %1, %0\n"
                 "       movl    %0, %2\n"
                 "       andl    %3, %2\n"
                 "       lock; cmpxchgl %2, %1\n"
                 "       jne     1b"
                 : "=&a"(original), "+m"(object->_nonatomic), "=&r"(temp)
                 : "r"(operand));
    return original;
}

MINT_C_INLINE uint32_t mint_fetch_or_32_relaxed(mint_atomic32_t *object, uint32_t operand)
{
    uint32_t original;
    register uint32_t temp;
    asm volatile("1:     movl    %1, %0\n"
                 "       movl    %0, %2\n"
                 "       orl     %3, %2\n"
                 "       lock; cmpxchgl %2, %1\n"
                 "       jne     1b"
                 : "=&a"(original), "+m"(object->_nonatomic), "=&r"(temp)
                 : "r"(operand));
    return original;
}


#if MINT_CPU_X64
    //------------------------------------------------------------------------
    //  64-bit atomic operations on 64-bit processor (x64)
    //------------------------------------------------------------------------

    MINT_C_INLINE uint64_t mint_load_64_relaxed(mint_atomic64_t *object)
    {
        // On x64, aligned 64-bit loads are already atomic.
        return object->_nonatomic;
    }

    MINT_C_INLINE void mint_store_64_relaxed(mint_atomic64_t *object, uint64_t desired)
    {
        // On x64, aligned 64-bit stores are already atomic.
        object->_nonatomic = desired;
    }

    MINT_C_INLINE uint64_t mint_compare_exchange_strong_64_relaxed(mint_atomic64_t *object, uint64_t expected, uint64_t desired)
    {
        // On x64, we can work with 64-bit values directly.
        // It's basically the same as the 32-bit versions except for the q suffix on opcodes.
        uint64_t original;
        asm volatile("lock; cmpxchgq %2, %1"
                     : "=a"(original), "+m"(object->_nonatomic)
                     : "q"(desired), "0"(expected));
        return original;
    }

    MINT_C_INLINE uint64_t mint_fetch_add_64_relaxed(mint_atomic64_t *object, int64_t operand)
    {
        uint64_t original;
        asm volatile("lock; xaddq %0, %1"
                     : "=r"(original), "+m"(object->_nonatomic)
                     : "0"(operand));
        return original;
    }

    MINT_C_INLINE uint64_t mint_fetch_and_64_relaxed(mint_atomic64_t *object, uint64_t operand)
    {
        uint64_t original;
        register uint64_t temp;
        asm volatile("1:     movq    %1, %0\n"
                     "       movq    %0, %2\n"
                     "       andq    %3, %2\n"
                     "       lock; cmpxchgq %2, %1\n"
                     "       jne     1b"
                     : "=&a"(original), "+m"(object->_nonatomic), "=&r"(temp)
                     : "r"(operand));
        return original;
    }

    MINT_C_INLINE uint64_t mint_fetch_or_64_relaxed(mint_atomic64_t *object, uint64_t operand)
    {
        uint64_t original;
        register uint64_t temp;
        asm volatile("1:     movq    %1, %0\n"
                     "       movq    %0, %2\n"
                     "       orq     %3, %2\n"
                     "       lock; cmpxchgq %2, %1\n"
                     "       jne     1b"
                     : "=&a"(original), "+m"(object->_nonatomic), "=&r"(temp)
                     : "r"(operand));
        return original;
    }

#elif MINT_CPU_X86
    //------------------------------------------------------------------------
    //  64-bit atomic operations on 32-bit processor (x86)
    //------------------------------------------------------------------------

    MINT_C_INLINE uint64_t mint_load_64_relaxed(mint_atomic64_t *object)
    {
        // On 32-bit x86, the most compatible way to get an atomic 64-bit load is with cmpxchg8b.
        // Essentially, we perform mint_compare_exchange_strong_64_relaxed(object, _dummyValue, _dummyValue).
        // "=&A"(original) outputs EAX:EDX to original after the block, while telling the compiler that
        // these registers are clobbered before %1 is used, so don't use EAX or EDX for %1.
        // "m"(object->_nonatomic) loads object's address into a register, which becomes %1, before the block.
        // No other registers are modified.
        uint64_t original;
        asm volatile("movl %%ebx, %%eax\n"
                     "movl %%ecx, %%edx\n"
                     "lock; cmpxchg8b %1"
                     : "=&A"(original)
                     : "m"(object->_nonatomic));
        return original;
    }

    MINT_C_INLINE void mint_store_64_relaxed(mint_atomic64_t *object, uint64_t desired)
    {
        // On 32-bit x86, the most compatible way to get an atomic 64-bit store is with cmpxchg8b.
        // Essentially, we perform mint_compare_exchange_strong_64_relaxed(object, object->_nonatomic, desired)
        // in a loop until it returns the original value.
        // According to the Linux kernel (atomic64_cx8_32.S), we don't need the "lock;" prefix
        // on cmpxchg8b since aligned 64-bit writes are already atomic on 586 and newer.
        // "=m"(object->_nonatomic) loads object's address into a register, which becomes %0, before the block,
        // and tells the compiler the variable at address will be modified by the block.
        // "b" and "c" move desired to ECX:EBX before the block.
        // "A"(expected) loads the original value of object->_nonatomic into EAX:EDX before the block.
        uint64_t expected = object->_nonatomic;
        asm volatile("1:    cmpxchg8b %0\n"
                     "      jne 1b"
                     : "=m"(object->_nonatomic)
                     : "b"((uint32_t) desired), "c"((uint32_t) (desired >> 32)), "A"(expected));
    }

    MINT_C_INLINE uint64_t mint_compare_exchange_strong_64_relaxed(mint_atomic64_t *object, uint64_t expected, uint64_t desired)
    {
        // cmpxchg8b is the only way to do 64-bit RMW operations on 32-bit x86.
        // "=A"(original) outputs EAX:EDX to original after the block.
        // "+m"(object->_nonatomic) is the memory address that is read/written. This becomes %1.
        // "b" and "c" move desired to ECX:EBX before the block.
        // "0"(expected) puts expected in the same registers as "=a"(original), which are EAX:EDX, before the block.
        uint64_t original;
        asm volatile("lock; cmpxchg8b %1"
                     : "=A"(original), "+m"(object->_nonatomic)
                     : "b"((uint32_t) desired), "c"((uint32_t) (desired >> 32)), "0"(expected));
        return original;
    }

    MINT_C_INLINE uint64_t mint_fetch_add_64_relaxed(mint_atomic64_t *object, int64_t operand)
    {
        // This implementation generates an unnecessary cmp instruction after the cmpxchg8b.
        // Could be optimized further using inline assembly.
        for (;;)
        {
            uint64_t original = object->_nonatomic;
            if (mint_compare_exchange_strong_64_relaxed(object, original, original + operand) == original)
                return original;
        }
    }

    MINT_C_INLINE uint64_t mint_fetch_and_64_relaxed(mint_atomic64_t *object, uint64_t operand)
    {
        // This implementation generates an unnecessary cmp instruction after the cmpxchg8b.
        // Could be optimized further using inline assembly.
        for (;;)
        {
            uint64_t original = object->_nonatomic;
            if (mint_compare_exchange_strong_64_relaxed(object, original, original & operand) == original)
                return original;
        }
    }

    MINT_C_INLINE uint64_t mint_fetch_or_64_relaxed(mint_atomic64_t *object, uint64_t operand)
    {
        // This implementation generates an unnecessary cmp instruction after the cmpxchg8b.
        // Could be optimized further using inline assembly.
        for (;;)
        {
            uint64_t original = object->_nonatomic;
            if (mint_compare_exchange_strong_64_relaxed(object, original, original | operand) == original)
                return original;
        }
    }

#else
    #error Unrecognized target CPU!
#endif


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTOMIC_PRIVATE_MINTOMIC_GCC_X86_64_H__

