#ifndef __MINTOMIC_PRIVATE_MINTOMIC_MSVC_H__
#define __MINTOMIC_PRIVATE_MINTOMIC_MSVC_H__

#ifdef __cplusplus
extern "C" {
#endif

#if MINT_TARGET_XBOX_360    // Xbox 360
void _ReadWriteBarrier();
#pragma intrinsic(_ReadWriteBarrier)
#endif


//-------------------------------------
//  Atomic types
//-------------------------------------
// In MSVC, correct alignment of each type is already ensured.
// MSVC doesn't seem subject to out-of-thin-air stores like GCC, so volatile is omitted.
// (MS volatile implies acquire & release semantics, which may be expensive on Xbox 360.)
typedef struct { uint32_t _nonatomic; } mint_atomic32_t;
typedef struct { uint64_t _nonatomic; } mint_atomic64_t;
typedef struct { void *_nonatomic; } mint_atomicPtr_t;


//-------------------------------------
//  Fences
//-------------------------------------
#define mint_signal_fence_consume() (0)
#define mint_signal_fence_acquire() _ReadWriteBarrier()
#define mint_signal_fence_release() _ReadWriteBarrier()
#define mint_signal_fence_seq_cst() _ReadWriteBarrier()

#if MINT_TARGET_XBOX_360    // Xbox 360
    // According to http://msdn.microsoft.com/en-us/library/windows/desktop/ee418650.aspx,
    // __lwsync() also acts a compiler barrier, unlike MemoryBarrier() on X360.
    // Hoping __sync() acts as a compiler barrier too.
    #define mint_thread_fence_consume() (0)
    #define mint_thread_fence_acquire() __lwsync()
    #define mint_thread_fence_release() __lwsync()
    #define mint_thread_fence_seq_cst() __sync()
#else                       // Windows
    #define mint_thread_fence_consume() (0)
    #define mint_thread_fence_acquire() _ReadWriteBarrier()
    #define mint_thread_fence_release() _ReadWriteBarrier()
    #define mint_thread_fence_seq_cst() MemoryBarrier()
#endif


//----------------------------------------------
//  32-bit atomic operations
//----------------------------------------------
MINT_C_INLINE uint32_t mint_load_32_relaxed(mint_atomic32_t *object)
{
    return object->_nonatomic;
}

MINT_C_INLINE void mint_store_32_relaxed(mint_atomic32_t *object, uint32_t value)
{
    object->_nonatomic = value;
}

MINT_C_INLINE uint32_t mint_compare_exchange_strong_32_relaxed(mint_atomic32_t *object, uint32_t expected, uint32_t desired)
{
    return _InterlockedCompareExchange((long *) object, desired, expected);
}

MINT_C_INLINE uint32_t mint_fetch_add_32_relaxed(mint_atomic32_t *object, int32_t operand)
{
    return _InterlockedExchangeAdd((long *) object, operand);
}

MINT_C_INLINE uint32_t mint_fetch_and_32_relaxed(mint_atomic32_t *object, uint32_t operand)
{
    return _InterlockedAnd((long *) object, operand);
}

MINT_C_INLINE uint32_t mint_fetch_or_32_relaxed(mint_atomic32_t *object, uint32_t operand)
{
    return _InterlockedOr((long *) object, operand);
}


//----------------------------------------------
//  64-bit atomic operations
//----------------------------------------------
MINT_C_INLINE uint64_t mint_load_64_relaxed(mint_atomic64_t *object)
{
#if MINT_CPU_X64 || MINT_TARGET_XBOX_360
    return object->_nonatomic;
#else
    // On 32-bit x86, the most compatible way to get an atomic 64-bit load is with cmpxchg8b.
    // This essentially performs mint_compare_exchange_strong_64_relaxed(object, _dummyValue, _dummyValue).
    uint64_t result;
    __asm
    {
        mov esi, object;
        mov ebx, eax;
        mov ecx, edx;
        lock cmpxchg8b [esi];
        mov dword ptr result, eax;
        mov dword ptr result[4], edx;
    }
    return result;
#endif
}

MINT_C_INLINE void mint_store_64_relaxed(mint_atomic64_t *object, uint64_t value)
{
#if MINT_CPU_X64 || MINT_TARGET_XBOX_360
    object->_nonatomic = value;
#else
    // On 32-bit x86, the most compatible way to get an atomic 64-bit store is with cmpxchg8b.
    // Essentially, we perform mint_compare_exchange_strong_64_relaxed(object, object->_nonatomic, desired)
    // in a loop until it returns the original value.
    // According to the Linux kernel (atomic64_cx8_32.S), we don't need the "lock;" prefix
    // on cmpxchg8b since aligned 64-bit writes are already atomic on 586 and newer.
    __asm
    {
        mov esi, object;
        mov ebx, dword ptr value;
        mov ecx, dword ptr value[4];
    retry:
        cmpxchg8b [esi];
        jne retry;
    }
#endif
}

MINT_C_INLINE uint64_t mint_compare_exchange_strong_64_relaxed(mint_atomic64_t *object, uint64_t expected, uint64_t desired)
{
    return _InterlockedCompareExchange64((LONGLONG *) object, desired, expected);
}

MINT_C_INLINE uint64_t mint_fetch_add_64_relaxed(mint_atomic64_t *object, int64_t operand)
{
#if MINT_CPU_X64 || MINT_TARGET_XBOX_360
    return _InterlockedExchangeAdd64((LONGLONG *) object, operand);
#else
    // It would be cool to check the zero flag, which is set by lock cmpxchg8b, to know whether the CAS succeeded,
    // but that would require an __asm block, which forces us to move the result to a stack variable.
    // Let's just re-compare the result with the original instead.
    uint64_t expected = object->_nonatomic;
    for (;;)
    {
        uint64_t original = _InterlockedCompareExchange64((LONGLONG *) object, expected + operand, expected);
        if (original == expected)
            return original;
        expected = original;
    }
#endif
}

MINT_C_INLINE uint64_t mint_fetch_and_64_relaxed(mint_atomic64_t *object, uint64_t operand)
{
#if MINT_CPU_X64 || MINT_TARGET_XBOX_360
    return _InterlockedAnd64((LONGLONG *) object, operand);
#else
    uint64_t expected = object->_nonatomic;
    for (;;)
    {
        uint64_t original = _InterlockedCompareExchange64((LONGLONG *) object, expected & operand, expected);
        if (original == expected)
            return original;
        expected = original;
    }
#endif
}

MINT_C_INLINE uint64_t mint_fetch_or_64_relaxed(mint_atomic64_t *object, uint64_t operand)
{
#if MINT_CPU_X64 || MINT_TARGET_XBOX_360
    return _InterlockedOr64((LONGLONG *) object, operand);
#else
    uint64_t expected = object->_nonatomic;
    for (;;)
    {
        uint64_t original = _InterlockedCompareExchange64((LONGLONG *) object, expected | operand, expected);
        if (original == expected)
            return original;
        expected = original;
    }
#endif
}


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTOMIC_PRIVATE_MINTOMIC_MSVC_H__
