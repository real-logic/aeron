#include <mintsystem/datetime.h>


uint64_t mint_get_current_utc_time()
{
    FILETIME fileTime;
    ULARGE_INTEGER largeInteger;

    GetSystemTimeAsFileTime(&fileTime);
    largeInteger.LowPart = fileTime.dwLowDateTime;
    largeInteger.HighPart = fileTime.dwHighDateTime;
    return largeInteger.QuadPart / 10;
}
