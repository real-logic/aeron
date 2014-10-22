#ifndef __MINTSYSTEM_PRIVATE_THREAD_MSVC_H__
#define __MINTSYSTEM_PRIVATE_THREAD_MSVC_H__

#ifdef __cplusplus
extern "C" {
#endif


struct mint_thread_msvc_t;
typedef struct mint_thread_msvc_t *mint_thread_t;

int mint_thread_create(mint_thread_t *thread, void *(*start_routine) (void *), void *arg);
int mint_thread_join(mint_thread_t thread, void **retval);


#ifdef __cplusplus
} // extern "C"
#endif

#endif // __MINTSYSTEM_PRIVATE_THREAD_MSVC_H__
