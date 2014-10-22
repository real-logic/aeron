#ifndef __MINTSYSTEM_PRIVATE_TID_MSVC_H__
#define __MINTSYSTEM_PRIVATE_TID_MSVC_H__

#ifdef __cplusplus
extern "C" {
#endif


//-------------------------------------
//  Thread IDs
//-------------------------------------
typedef uint32_t mint_tid_t;

MINT_C_INLINE mint_tid_t mint_get_current_thread_id()
{
#if MINT_TARGET_XBOX_360  // Xbox 360
    return GetCurrentThreadId();
#elif MINT_CPU_X64        // Windows x64
    return ((uint32_t*) __readgsqword(48))[18]; // Read directly from the TIB
#elif MINT_CPU_X86        // Windows x86
    return ((uint32_t*) __readfsdword(24))[9];  // Read directly from the TIB
#endif
}


//-------------------------------------
//  Process IDs
//-------------------------------------
typedef uint32_t mint_pid_t;

MINT_C_INLINE mint_pid_t mint_get_current_process_id()
{
#if MINT_TARGET_XBOX_360  // Xbox 360
    return 0;
#elif MINT_CPU_X64        // Windows x64
    return ((uint32_t*) __readgsqword(48))[16]; // Read directly from the TIB
#elif MINT_CPU_X86        // Windows x86
    return ((uint32_t*) __readfsdword(24))[8];  // Read directly from the TIB
#endif
}


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_TID_MSVC_H__
