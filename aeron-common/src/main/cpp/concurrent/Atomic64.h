#ifndef INCLUDED_ATOMIC64_
#define INCLUDED_ATOMIC64_

#ifndef __LP64__
#error This code is 64 specific
#endif

#include <cstdint>

/**
 * Set of Operations to support Atomic operations in C++ that are
 * consistent with the same semantics in the JVM.
 */

/**
 * A compiler directive not reorder instructions.
 */
inline void threadFence()
{
    __asm__ __volatile__("" ::: "memory");
}

/**
 * Fence operation that uses locked addl as mfence is sometimes expensive
 */
inline void fence()
{
    __asm__ volatile ("lock; addl $0,0(%%rsp)" : : : "cc", "memory");
}

inline void acquire()
{
    volatile std::int64_t* dummy;
    __asm__ volatile ("movq 0(%%rsp), %0" : "=r" (dummy) : : "memory");
}

inline void release()
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
    // Avoid hitting the same cache-line from different threads.
  volatile std::int64_t dummy = 0;
#pragma GCC diagnostic pop

}

/**
 * A more jitter friendly alternate to thread:yield in spin waits.
 */
inline void cpu_pause()
{
    asm volatile("pause\n": : :"memory");
}

/**
 * Returns a 32 bit integer without locking.
 */
inline std::int32_t getInt32(volatile std::int32_t* source)
{
    return *reinterpret_cast<volatile std::int32_t *>(source);
}

/**
 * Returns a 32 bit integer with ordered semantics.
 */
inline std::int32_t getInt32Ordered(volatile std::int32_t* source)
{
    int32_t sequence = *reinterpret_cast<volatile std::int32_t *>(source);
    threadFence();
    return sequence;
}

/**
 * Returns a 32 bit integer with volatile semantics.
 * On x64 MOV is a SC Atomic a operation.
 */
inline std::int32_t getInt32Volatile(volatile std::int32_t* source)
{
    int32_t sequence = *reinterpret_cast<volatile std::int32_t *>(source);
    threadFence();
    return sequence;
}

/**
 * Put a 32 bit integer
 */
 inline void putInt32(volatile std::int32_t* source, std::int32_t value)
 {
     *reinterpret_cast<volatile std::int32_t *>(source) = value;
 }

/**
 * Put a 32 bit integer
 */
inline void putInt32Ordered(volatile std::int32_t* source, std::int32_t value)
{
    threadFence();
    *reinterpret_cast<volatile std::int32_t *>(source) = value;
}

/**
 * Put a 32 bit with atomic semantics.
 **/
inline void putInt32Atomic(volatile std::int32_t*  address, std::int32_t value)
{
    __asm__ __volatile__ (  "xchgl (%2), %0"
                          : "=r" (value)
                          : "0" (value), "r" (address)
                          : "memory");
}

/**
 * Returns a 64 bit integer.
 */
inline std::int64_t getInt64(volatile std::int32_t* source)
{
    return *reinterpret_cast<volatile std::int64_t *>(source);
}

/**
 * Returns a 64 bit integer with ordered semantics.
 */
inline std::int64_t getInt64Ordered(volatile std::int32_t* source)
{
    int64_t sequence = *reinterpret_cast<volatile std::int64_t *>(source);
    threadFence();
    return sequence;
}

/**
 * Returns a 64 bit integer with volatile semantics.
 * On x64 MOV is a SC Atomic a operation.
 */
inline std::int64_t getInt64Volatile(volatile std::int32_t* source)
{
    int64_t sequence = *reinterpret_cast<volatile std::int64_t *>(source);
    threadFence();
    return sequence;
}

/**
 * Put a 64 bit int without ordered semantics.
 */
inline void  putInt64(volatile std::int32_t*  address, std::int64_t value)
{
    *reinterpret_cast<volatile std::int64_t *>(address) = value;
}
/**
 * Put a 64 bit with ordered semantics.
 */
inline void  putInt64Ordered(volatile std::int32_t*  address, std::int64_t value)
{
    threadFence();
    *reinterpret_cast<volatile std::int64_t *>(address) = value;
}


/**
 * Put a 64 bit with atomic semantics.
 **/
inline void putInt64Atomic(volatile std::int32_t*  address, std::int64_t value)
{
    __asm__ __volatile__ (  "xchgq (%2), %0"
                            : "=r" (value)
                            : "0" (value), "r" (address)
                            : "memory");
}

inline std::int64_t getAndAddInt64(volatile std::int32_t* address, std::int64_t value)
{
    std::int64_t original;
    asm volatile("lock; xaddq %0, %1"
                 : "=r"(original), "+m"(*address)
                 : "0"(value));
    return original;
}

inline std::int32_t getAndAddInt32(volatile std::int32_t* address, std::int32_t value)
{
    std::int32_t original;
    asm volatile("lock; xaddl %0, %1"
                 : "=r"(original), "+m"(*address)
                 : "0"(value));
    return original;
}

inline std::int32_t addInt32Ordered(volatile std::int32_t* address , int32_t value)
{
    // "=r"(original) chooses any general register, makes that %0, and outputs this register to original after the block.
    // "+m"(object->_nonatomic) is the memory address that is read/written. This becomes %1.
    // "0"(operand) puts operand into the same register as %0 before the block.
    // volatile is required. Otherwise, if the return value (original) is unused, the asm block
    // may be deleted. ("+m" is apparently not enough hint to the compiler that the asm
    // block has side effects on memory.)
    std::int32_t original;
    asm volatile("lock; xaddl %0, %1"
                 : "=r"(original), "+m"(*address)
                 : "0"(value));
    return original;
}

inline std::int64_t addInt64Ordered(volatile std::int32_t* address , int64_t value)
{
    // "=r"(original) chooses any general register, makes that %0, and outputs this register to original after the block.
    // "+m"(object->_nonatomic) is the memory address that is read/written. This becomes %1.
    // "0"(operand) puts operand into the same register as %0 before the block.
    // volatile is required. Otherwise, if the return value (original) is unused, the asm block
    // may be deleted. ("+m" is apparently not enough hint to the compiler that the asm
    // block has side effects on memory.)
    std::int64_t original;
    asm volatile("lock; xaddq %0, %1"
                 : "=r"(original), "+m"(*address)
                 : "0"(value));
    return original;
}


inline std::int32_t cmpxchg(volatile std::int32_t* destination,  std::int32_t expected, std::int32_t desired)
{
    std::int32_t original;
    asm volatile("lock; cmpxchgl %2, %1"
                 : "=a"(original), "+m"(*destination)
                 : "q"(desired), "0"(expected));
    return original;
}

inline std::int64_t cmpxchg(volatile std::int32_t* destination,  std::int64_t expected, std::int64_t desired)
{
    std::int64_t original;
    asm volatile("lock; cmpxchgq %2, %1"
                 : "=a"(original), "+m"(*destination)
                 : "q"(desired), "0"(expected));
    return original;
}

//-------------------------------------
//  Alignment
//-------------------------------------
// Note: May not work on local variables.
// http://gcc.gnu.org/bugzilla/show_bug.cgi?id=24691
#define MINT_DECL_ALIGNED(declaration, amt) declaration __attribute__((aligned(amt)))


#endif
