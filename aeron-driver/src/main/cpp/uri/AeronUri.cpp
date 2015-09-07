//
// Created by Michael Barker on 26/08/15.
//

#include <algorithm>
#include <iostream>
#include <sstream>

#include "AeronUri.h"
#include "util/Exceptions.h"

using namespace aeron::driver::uri;

enum State
{
    MEDIA, PARAMS_KEY, PARAMS_VALUE
};

AeronUri* AeronUri::parse(std::string &uriString)
{
    const std::string prefix = "aeron:";
    std::stringstream media;
    std::stringstream key;
    std::stringstream value;
    std::unordered_map<std::string, std::string> params;

    auto result = std::mismatch(prefix.begin(), prefix.end(), uriString.begin());

    if (*result.first != '\0')
    {
        throw aeron::util::ParseException("Aeron URI does not start with 'aeron:'", SOURCEINFO);
    }

    auto iter = result.second;
    State state = MEDIA;

    for (; iter != uriString.end(); ++iter)
    {
        char c = *iter;

        switch (state)
        {
            case MEDIA:
                switch (c)
                {
                    case '?':
                        state = PARAMS_KEY;
                        break;

                    case ':':
                        // TODO: throw exception
                        break;

                    default:
                        media << c;
                }
                break;

            case PARAMS_KEY:
                switch (c)
                {
                    case '=':
                        state = PARAMS_VALUE;
                        break;

                    default:
                        key << c;
                        break;
                }
                break;

            case PARAMS_VALUE:
                switch (c)
                {
                    case '|':
                        params[key.str()] = value.str();
                        key.str(""); value.str("");
                        state = PARAMS_KEY;
                        break;

                    default:
                        value << c;
                        break;
                }
                break;
        }
    }

    if (state == PARAMS_VALUE)
    {
        params[key.str()] = value.str();
    }

    return new AeronUri{media.str(), params};
}
