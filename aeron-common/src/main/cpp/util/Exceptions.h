#ifndef INCLUDED_AERON_UTIL_EXCEPTIONS_FILE__
#define INCLUDED_AERON_UTIL_EXCEPTIONS_FILE__

#include <stdexcept>
#include "MacroUtil.h"

// ==========================================================================================================================
// macro to create a const char* with the details of current source line in the format:
//    [Class::Method : d:\projects\file.cpp : 127]
// ==========================================================================================================================

namespace aeron { namespace common { namespace util {


#ifdef _WIN32
	#define SOURCEINFO __FUNCTION__,  " : "  __FILE__  " : " TOSTRING(__LINE__)
	#define AERON_NOEXCEPT
#else 
	#define SOURCEINFO  __PRETTY_FUNCTION__,  " : "  __FILE__  " : " TOSTRING(__LINE__)
	#define AERON_NOEXCEPT noexcept 
#endif

class SourcedException : public std::exception
{
private:
    std::string m_where;
    std::string m_what;

public:
    SourcedException(const std::string &what, const std::string& function, const std::string& where)
            : m_where(function + where), m_what(what)
    {
    }

	virtual const char *what() const AERON_NOEXCEPT
    {
        return m_what.c_str();
    }

		const char *where() const AERON_NOEXCEPT
    {
        return m_where.c_str();
    }
};

#define DECLARE_SOURCED_EXCEPTION(exceptionName)                                            \
            class exceptionName : public SourcedException                                   \
            {                                                                               \
                public:                                                                     \
                    exceptionName (const std::string &what, const std::string& function, const std::string& where)   \
                            : SourcedException (what, function, where)                      \
                        {}                                                                  \
            }                                                                               \

DECLARE_SOURCED_EXCEPTION (IOException);
DECLARE_SOURCED_EXCEPTION (FormatException);
DECLARE_SOURCED_EXCEPTION (OutOfBoundsException);
DECLARE_SOURCED_EXCEPTION (ParseException);
DECLARE_SOURCED_EXCEPTION (ElementNotFound);
DECLARE_SOURCED_EXCEPTION (IllegalArgumentException);

}}}
#endif