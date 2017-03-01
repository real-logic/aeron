/*
 * Copyright 2014-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
