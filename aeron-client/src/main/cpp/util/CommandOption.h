/*
 * Copyright 2014-2020 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef AERON_UTIL_COMMAND_OPTION_H
#define AERON_UTIL_COMMAND_OPTION_H

#include <iostream>
#include <exception>
#include <string>
#include <vector>
#include <map>

#include "Exceptions.h"
#include "util/Export.h"

namespace aeron { namespace util
{

AERON_DECLARE_SOURCED_EXCEPTION (CommandOptionException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);

class CLIENT_EXPORT CommandOption
{

private:
    char m_optionChar;
    size_t m_minParams;
    size_t m_maxParams;
    std::string m_helpText;

    bool m_isPresent;

    std::vector<std::string> m_params;

    void checkIndex(size_t index) const;

public:
    static const char UNNAMED = -1;

    CommandOption();

    CommandOption(char optionChar, size_t minParams, size_t maxParams, std::string helpText);

    char getOptionChar() const
    {
        return m_optionChar;
    }

    std::string getHelpText() const
    {
        return m_helpText;
    }

    void addParam(std::string p)
    {
        m_params.push_back(std::move(p));
    }

    void validate() const;

    bool isPresent() const
    {
        return m_isPresent;
    }

    void setPresent()
    {
        m_isPresent = true;
    }

    size_t getNumParams() const
    {
        return m_params.size();
    }

    std::string getParam(size_t index) const;

    std::string getParam(size_t index, std::string defaultValue) const;

    int getParamAsInt(size_t index) const;

    long long getParamAsLong(size_t index) const;

    int getParamAsInt(size_t index, int minValue, int maxValue, int defaultValue) const;

    long long getParamAsLong(size_t index, long long minValue, long long maxValue, long long defaultValue) const;
};

}}

#endif