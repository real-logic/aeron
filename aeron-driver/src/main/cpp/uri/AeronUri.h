//
// Created by Michael Barker on 26/08/15.
//

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
