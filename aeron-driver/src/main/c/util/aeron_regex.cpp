#include <regex>
#include <thread>
#include <time.h>

extern "C"
{
    #include "util/aeron_regex.h"

    int aeron_regcomp(aeron_regex_t* output, const char* pattern, char* error, size_t max_error_size)
    {
        try
        {
            *output = new std::regex{ pattern };
            return 0;
        }
        catch (const std::exception& e)
        {
            *output = nullptr;
            strncpy(error, e.what(), max_error_size);
            return -1;
        }
    }

    int aeron_regexec(aeron_regex_t regex, const char* text, aeron_regex_matches_t* matches, size_t max_matches_count, char* error, size_t max_error_size)
    {
        try 
        {
            const auto& base_regex = *reinterpret_cast<std::regex*>(regex);
            auto str = std::string(text);

            std::smatch base_match;


            if (std::regex_match(str, base_match, base_regex))
            {
                for (size_t i = 1; i < base_match.size() && i <= max_matches_count; i++)
                {
                    matches[i - 1].offset = base_match.position(i);
                    matches[i - 1].length = base_match.length(i);
                }

                return base_match.size();
            }
        }
        catch (const std::exception& e)
        {
            strncpy(error, e.what(), max_error_size);
            return -1;
        }

        return 0;
    }
}
