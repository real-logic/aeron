/*
 * Copyright 2015 - 2016 Real Logic Ltd.
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

#ifndef INCLUDE_AERON_DRIVER_URI_AERON_URI_
#define INCLUDE_AERON_DRIVER_URI_AERON_URI_

#include <string>
#include <unordered_map>
#include <iostream>

namespace aeron { namespace driver { namespace uri {

class AeronUri
{
public:
    AeronUri(
        const std::string& media,
        const std::unordered_map<std::string, std::string>& params) :
        m_media(std::move(media)), m_params(std::move(params))
    {
    }

    const std::string& scheme() const
    {
        return m_scheme;
    }

    const std::string& media() const
    {
        return m_media;
    }

    const std::string& param(const char* key) const
    {
        std::string s{key};
        return m_params.at(s);
    }

    const std::string& param(const char* key, std::string& defaultVal) const
    {
        if (!hasParam(key))
        {
            return defaultVal;
        }

        std::string s{key};
        return m_params.at(s);
    }

    const std::string& param(std::string const & key) const
    {
        return m_params.at(key);
    }

    bool hasParam(const char* key) const
    {
        std::string s{key};
        return hasParam(s);
    }

    bool hasParam(std::string& key) const
    {
        return m_params.find(key) != m_params.end();
    }

    static AeronUri* parse(std::string& uriString);
    friend std::ostream& operator<<(std::ostream& os, const AeronUri& dt);

private:
    const std::string m_scheme = "aeron";
    const std::string m_media;
    const std::unordered_map<std::string, std::string> m_params;
};

inline std::ostream& operator<<(std::ostream& os, const AeronUri& dt)
{
    os << dt.m_scheme << ':' << dt.m_media << '?';
    for (const auto & pair : dt.m_params)
    {
        os << '|' << pair.first << '=' << pair.second;
    }
    return os;
}

}}}

#endif //AERON_AERONURI_H
