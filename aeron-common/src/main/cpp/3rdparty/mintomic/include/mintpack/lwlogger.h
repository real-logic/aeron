#ifndef __MINTPACK_LWLOGGER_H__
#define __MINTPACK_LWLOGGER_H__

#include <mintomic/mintomic.h>
#include <mintsystem/tid.h>


//-------------------------------------
//  LWLogger
//-------------------------------------
#ifndef LWLOGGER_ENABLED
#define LWLOGGER_ENABLED 1
#endif

namespace LWLogger
{
    struct Event
    {
        mint_tid_t tid;     // Thread ID
        const char* msg;  // Message string
        uint32_t param;   // A parameter which can mean anything you want
    };

    static const int BUFFER_SIZE = 65536;   // Must be a power of 2
    extern Event g_events[BUFFER_SIZE];
    extern mint_atomic32_t g_pos;

    inline void log(const char* msg, uint32_t param)
    {
        // Get next event index
        uint32_t index = mint_fetch_add_32_relaxed(&g_pos, 1);
        // Write an event at this index
        Event* e = g_events + (index & (BUFFER_SIZE - 1));  // Wrap to buffer size
        e->tid = mint_get_current_thread_id();
        e->msg = msg;
        e->param = param;
    }
}

#if LWLOGGER_ENABLED
#define LWLOG(m, p) LWLogger::log(m, p)
#else
#define LWLOG(m, p) {}
#endif


#endif // __MINTPACK_LWLOGGER_H__
