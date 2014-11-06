#ifndef INCLUDED_AERON_UTIL_INDEX_FILE__
#define INCLUDED_AERON_UTIL_INDEX_FILE__

#include <cstdint>

namespace aeron { namespace common { namespace util {

// a 32bit signed int that is to be used for sizes and offsets to be compatible with
// java's signed 32 bit int.
typedef std::int32_t index_t;

}}}

#endif
