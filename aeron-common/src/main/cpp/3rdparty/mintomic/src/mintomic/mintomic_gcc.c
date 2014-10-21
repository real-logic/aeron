#include <mintomic/core.h>

#if MINT_COMPILER_GCC && MINT_CPU_ARM && (MINT_CPU_ARM_VERSION == 6) && MINT_CPU_ARM_THUMB  // ARMv6 Thumb mode

//----------------------------------------------------------------------------
// When compiling for ARMv6 in Thumb mode, the mcr, ldrex and strex instructions
// are not available. To use them, we need to switch temporarily to ARM (non-Thumb) mode
// via standalone functions. The nice thing about writing standalone code blocks
// this way is that can we generate non-Thumb assembly code, regardless of
// the compiler configuration, and it will be callable from any type of code (ARM or Thumb).
//----------------------------------------------------------------------------

__asm__(
"   .align 2\n"
"   .globl _mint_thread_fence_acquire\n"
"   .globl _mint_thread_fence_release\n"
"   .globl _mint_thread_fence_seq_cst\n"
"_mint_thread_fence_acquire:\n"
"_mint_thread_fence_release:\n"
"_mint_thread_fence_seq_cst:\n"
// Do we really need to set the source register to 0?
"   mov r0, #0\n"
"   mcr p15, 0, r0, c7, c10, 5\n"
"   bx lr\n"
);

__asm__(
"   .align  2\n"
"   .globl  _mint_compare_exchange_strong_32_relaxed\n"
"_mint_compare_exchange_strong_32_relaxed:\n"
"   push    {r4}\n"
"   mov     r4, r0\n"
"1:\n"
"   ldrex   r0, [r4]\n"
"   cmp     r0, r1\n"
"   bne     2f\n"
"   strex   r3, r2, [r4]\n"
"   cmp     r3, #0\n"
"   bne     1b\n"
"2:\n"
"   pop     {r4}\n"
"   bx      lr\n"
);

__asm__(
"   .align  2\n"
"   .globl  _mint_fetch_add_32_relaxed\n"
"_mint_fetch_add_32_relaxed:\n"
"   push    {r4}\n"
"   mov     r4, r0\n"
"1:\n"
"   ldrex   r0, [r4]\n"
"   adds    r2, r0, r1\n"
"   strex   r3, r2, [r4]\n"
"   cmp     r3, #0\n"
"   bne     1b\n"
"   pop     {r4}\n"
"   bx      lr\n"
);

__asm__(
"   .align  2\n"
"   .globl  _mint_fetch_and_32_relaxed\n"
"_mint_fetch_and_32_relaxed:\n"
"   push    {r4}\n"
"   mov     r4, r0\n"
"1:\n"
"   ldrex   r0, [r4]\n"
"   and     r2, r0, r1\n"
"   strex   r3, r2, [r4]\n"
"   cmp     r3, #0\n"
"   bne     1b\n"
"   pop     {r4}\n"
"   bx      lr\n"
);

__asm__(
"   .align  2\n"
"   .globl  _mint_fetch_or_32_relaxed\n"
"_mint_fetch_or_32_relaxed:\n"
"   push    {r4}\n"
"   mov     r4, r0\n"
"1:\n"
"   ldrex   r0, [r4]\n"
"   orr     r2, r0, r1\n"
"   strex   r3, r2, [r4]\n"
"   cmp     r3, #0\n"
"   bne     1b\n"
"   pop     {r4}\n"
"   bx      lr\n"
);

#endif  // ARMv6 Thumb mode

#if MINT_COMPILER_GCC && MINT_CPU_ARM  // 64-bit atomics

//----------------------------------------------------------------------------
// Why not implement 64-bit atomics as inline assembly, like the 32-bit atomics?
// Mainly because in current versions of LLVM, the compiler does not accept the %H0 operand
// in inline assembly. For example, using Xcode 4.5 using iOS SDK 6.0,
// both compilers were tried in the project settings: Apple LLVM compiler 4.1, and LLVM GCC 4.2.
// Apparently, others have had this problem with LLVM as well. There is no way to tell LLVM that 64-bit
// integers passed to an asm block belong in an even/odd number pair, as ldrexd/strexd instructions require.
// http://infocenter.arm.com/help/index.jsp?topic=/com.arm.doc.ddi0360e/CHDCHAJF.html
// http://lists.cs.uiuc.edu/pipermail/llvm-commits/Week-of-Mon-20110523/121381.html
// http://markmail.org/thread/tvhxyvhrfuhonued
//
// Why not implement 64-bit atomics as inline assembly, but only for GCC?
// That doesn't work when compiling for the "old" ABI.
// For example, Xcode 3.2.5 using GCC 4.2 and iPhoneOS SDK 4.2.
// There's no way to make GCC allocate registers for an asm block in an even/odd number pair.
// (Actually, Debug builds appear to allocate registers that way by default, but not Release.)
//
// Why not implement 64-bit atomics as inline C functions using LLVM's ldrexd/strexd intrinsics,
// which were added around May 2011, (see above link), if those are available? (eg. in Xcode 4.5.)
// That works, but the resulting code seems quite large. Not sure it performs better than the
// implementations below, or if it's worth having two implementations. Worth benchmarking.
//
// For iOS devices, why not use Apple's native atomics?
// http://developer.apple.com/library/ios/#documentation/System/Conceptual/ManPages_iPhoneOS/man3/atomic.3.html
// Also viable, but would need to investigate the code generation.
// Not sure the extra implementation is worth it.
//
// Instead, the following standalone functions ought to work on every GCC-like compiler, including LLVM,
// on a wide range of ARM devices. The tradeoff is that the functions are not inlined.
//
// If these functions are to be optimized further using one of the previously mentioned
// approaches and inlining, it would be good to have benchmark data to support the optimization,
// such as by comparing the minimum timings from a large set of Mintomic test suite runs.
//----------------------------------------------------------------------------

__asm__(
"   .align  2\n"
"   .globl  _mint_load_64_relaxed\n"
#if MINT_CPU_ARM_THUMB && (MINT_CPU_ARM_VERSION != 6)
"   .thumb\n"
"   .thumb_func _mint_load_64_relaxed\n"
#endif
"_mint_load_64_relaxed:\n"
"   mov     r2, r0\n"
"1:\n"
"   ldrexd  r0, r1, [r2]\n"
"   strexd  r3, r0, r1, [r2]\n"
"   cmp     r3, #0\n"
"   bne     1b\n"
"   bx      lr\n"
);

__asm__(
"   .align  2\n"
"   .globl  _mint_store_64_relaxed\n"
#if MINT_CPU_ARM_THUMB && (MINT_CPU_ARM_VERSION != 6)
"   .thumb\n"
"   .thumb_func _mint_store_64_relaxed\n"
#endif
"_mint_store_64_relaxed:\n"
"   push    {r4-r5}\n"
"   mov     r5, r0\n"
"   mov     r3, r2\n"
"   mov     r2, r1\n"
"1:\n"
"   ldrexd  r0, r1, [r5]\n"
"   strexd  r4, r2, r3, [r5]\n"
"   cmp     r4, #0\n"
"   bne     1b\n"
"   pop     {r4-r5}\n"
"   bx      lr\n"
);

__asm__(
"   .align  2\n"
"   .globl  _mint_compare_exchange_strong_64_relaxed\n"
#if MINT_CPU_ARM_THUMB && (MINT_CPU_ARM_VERSION != 6)
"   .thumb\n"
"   .thumb_func _mint_compare_exchange_strong_64_relaxed\n"
#endif
"_mint_compare_exchange_strong_64_relaxed:\n"
"   push    {r4-r7}\n"
"   mov     r6, r3\n"
"   ldr     r7, [sp, #16]\n"
"   mov     r5, r0\n"
"   mov     r4, r1\n"
"1:\n"
"   ldrexd  r0, r1, [r5]\n"
"   cmp     r0, r4\n"
#if MINT_CPU_ARM_THUMB && (MINT_CPU_ARM_VERSION != 6)
"   it      eq\n"
#endif
"   cmpeq   r1, r2\n"
"   bne     2f\n"
"   strexd  r3, r6, r7, [r5]\n"
"   cmp     r3, #0\n"
"   bne     1b\n"
"2:\n"
"   pop     {r4-r7}\n"
"   bx      lr\n"
);

__asm__(
"   .align  2\n"
"   .globl  _mint_fetch_add_64_relaxed\n"
#if MINT_CPU_ARM_THUMB && (MINT_CPU_ARM_VERSION != 6)
"   .thumb\n"
"   .thumb_func _mint_fetch_add_64_relaxed\n"
#endif
"_mint_fetch_add_64_relaxed:\n"
"   push    {r4-r7}\n"
"   mov     r5, r0\n"
"   mov     r4, r1\n"
"1:\n"
"   ldrexd  r0, r1, [r5]\n"
"   adds    r6, r0, r4\n"
"   adc     r7, r1, r2\n"
"   strexd  r3, r6, r7, [r5]\n"
"   cmp     r3, #0\n"
"   bne     1b\n"
"   pop     {r4-r7}\n"
"   bx      lr\n"
);

__asm__(
"   .align  2\n"
"   .globl  _mint_fetch_and_64_relaxed\n"
#if MINT_CPU_ARM_THUMB && (MINT_CPU_ARM_VERSION != 6)
"   .thumb\n"
"   .thumb_func _mint_fetch_and_64_relaxed\n"
#endif
"_mint_fetch_and_64_relaxed:\n"
"   push    {r4-r7}\n"
"   mov     r5, r0\n"
"   mov     r4, r1\n"
"1:\n"
"   ldrexd  r0, r1, [r5]\n"
"   and     r6, r0, r4\n"
"   and     r7, r1, r2\n"
"   strexd  r3, r6, r7, [r5]\n"
"   cmp     r3, #0\n"
"   bne     1b\n"
"   pop     {r4-r7}\n"
"   bx      lr\n"
);

__asm__(
"   .align  2\n"
"   .globl  _mint_fetch_or_64_relaxed\n"
#if MINT_CPU_ARM_THUMB && (MINT_CPU_ARM_VERSION != 6)
"   .thumb\n"
"   .thumb_func _mint_fetch_or_64_relaxed\n"
#endif
"_mint_fetch_or_64_relaxed:\n"
"   push    {r4-r7}\n"
"   mov     r5, r0\n"
"   mov     r4, r1\n"
"1:\n"
"   ldrexd  r0, r1, [r5]\n"
"   orr     r6, r0, r4\n"
"   orr     r7, r1, r2\n"
"   strexd  r3, r6, r7, [r5]\n"
"   cmp     r3, #0\n"
"   bne     1b\n"
"   pop     {r4-r7}\n"
"   bx      lr\n"
);

#endif  // 64-bit atomics
