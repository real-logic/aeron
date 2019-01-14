#include "util/aeron_parser.h"
#include <string.h>

// ([^:/]+)(:([0-9]+))?(/([0-9]+))?
int aeron_parse_ipv4(const char* text, aeron_regex_matches_t* matches, size_t max_matches_count)
{
    // Expect Host / Port / Prefix
    if (max_matches_count < 3)
        return -1;
   
    memset(matches, 0, sizeof(aeron_regex_matches_t) * max_matches_count);

    size_t m = 0;
    matches[0].offset = 0;

    for(size_t i = 0; ; i++)
    {
        // Parsing host
        if (m == 0)
        {
            if (text[i] == ':' || text[i] == '/' || text[i] == 0)
            {
                matches[0].length = i;
                
                if (text[i] == ':')
                {
                    m = 1;
                }
                else if (text[i] == '/')
                {
                    m = 2;
                }

                matches[m].offset = i + 1;
                matches[0].offset = 0;

                if (matches[0].length == 0)
                {
                    return -1;
                }
            }
        }
        // Parsing port
        else if(m == 1)
        {
            if (text[i] == '/' || text[i] == 0)
            {                    
                matches[1].length = i - matches[1].offset;
                matches[2].offset = i + 1;
                m++;

                if (matches[1].length == 0)
                {
                    return -1;
                }
            }
            else if (text[i] < '0' || text[i] > '9')
            {
                return -1;
            }
        }
        // Parsing prefix length
        else if(m == 2)
        {
            if (text[i] == 0)
            {
                matches[2].length = i - matches[2].offset;
                m = 999;
            }
            else if (text[i] < '0' || text[i] > '9')
            {
                return -1;
            }
        }
        else
        {
            return -1;
        }

        if (text[i] == 0)
        {
            break;
        }
    }

    return 0;
}

// original: \\[([0-9A-Fa-f:]+)(%([a-zA-Z0-9_.~-]+))?\\](:([0-9]+))?(/([0-9]+))?
// simplified: 
int aeron_parse_ipv6(const char* text, aeron_regex_matches_t* matches, size_t max_matches_count)
{
    // Expect Host / Port / Prefix
    if (max_matches_count < 3)
    {
        return -1;
    }

    if (text[0] != '[')
    {
        return -1;
    }

    memset(matches, 0, sizeof(aeron_regex_matches_t) * max_matches_count);

    size_t m = 0;
    matches[0].offset = 1;

    for(size_t i = 1; ; i++)
    {
        // Parsing host
        if (m == 0)
        {
            if (text[i] == ']' || text[i] == 0)
            {
                matches[0].length = i - matches[0].offset;
                if (matches[0].length == 0)
                {
                    return -1;
                }
                m++;
            }
            else if (text[i] == '%')
            {
                matches[0].length = i - matches[0].offset;
                if (matches[0].length == 0)
                {
                    return -1;
                }
                m = 4;
            }
            else if (text[i] >= '0' && text[i] <= '9') {}
            else if (text[i] >= 'a' && text[i] <= 'f') {}
            else if (text[i] >= 'A' && text[i] <= 'F') {}
            else if (text[i] == ':') {}
            else 
            {
                return -1;
            }
        }
        // Parsing interface (%eth0)
        else if(m == 4)
        {
            if (text[i] == ']')
            {
                m = 1;
            }
            if (text[i] == 0)
            {
                return -1;
            }
        }
        // Right after ] of the address part
        else if (m == 1)
        {
            if (text[i] == ':')
            {
                m = 2;
                matches[1].offset = i + 1;
            }
            else if (text[i] == '/') 
            {
                m = 3;
                matches[2].offset = i + 1;
            }
        }
        // Parsing port
        else if (m == 2)
        {
            if(text[i] == '/' || text[i] == 0)
            {
                matches[1].length = i - matches[1].offset;
                matches[2].offset = i + 1;
                m++;

                if (matches[1].length == 0)
                {
                    return -1;
                }
            }
            else if(text[i] < '0' || text[i] > '9')
            {                    
                return -1;
            }
        }
        // Parsing prefix length
        else if(m == 3)
        {
            if(text[i] == 0)
            {
                matches[2].length = i - matches[2].offset;
                m = 999;

                if (matches[2].length == 0)
                {
                    return -1;
                }
            }
            else if(text[i] < '0' || text[i] > '9')
            {                    
                return -1;
            }
        }
        else
        {
            return -1;
        }

        if (text[i] == 0)
        {
            break;
        }
    }

    return 0;
}

