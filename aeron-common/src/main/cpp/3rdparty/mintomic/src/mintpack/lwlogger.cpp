#include <mintpack/lwlogger.h>

namespace LWLogger
{
    Event g_events[BUFFER_SIZE];
    mint_atomic32_t g_pos = { 0 };
}
