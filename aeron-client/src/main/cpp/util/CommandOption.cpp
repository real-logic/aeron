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

#include "CommandOptionParser.h"
#include "StringUtil.h"

namespace aeron { namespace util {

CommandOption::CommandOption()
        : m_optionChar('-'),
          m_minParams(0),
          m_maxParams(0),
          m_helpText(""),
          m_isPresent(false)
{
}


CommandOption::CommandOption(char optionChar, size_t minParams, size_t maxParams, std::string helpText)
        : m_optionChar(optionChar),
          m_minParams(minParams),
          m_maxParams(maxParams),
          m_helpText(helpText),
          m_isPresent(false)
{
}


void CommandOption::checkIndex(size_t index) const
{
    if (index > m_params.size())
        throw CommandOptionException(std::string("Internal Error: index out of range for option: ") + m_optionChar, SOURCEINFO);
}

std::string CommandOption::getParam(size_t index) const
{
    checkIndex(index);
    return m_params[index];
}

std::string CommandOption::getParam(size_t index, std::string defaultValue) const
{
    // if this option was not present on the command line then return the default value.
    if (!isPresent())
        return defaultValue;

    return getParam(index);
}

int CommandOption::getParamAsInt(size_t index) const
{
    checkIndex(index);

    std::string param = m_params[index];

    try {
        return parse<int>(param);
    }
    catch (const ParseException &) {
        throw CommandOptionException(std::string("Invalid numeric value: \"") + param + "\" on option -" + m_optionChar, SOURCEINFO);
    }
}

long CommandOption::getParamAsLong(size_t index) const
{
    checkIndex(index);

    std::string param = m_params[index];

    try {
        return parse<long>(param);
    }
    catch (const ParseException &) {
        throw CommandOptionException(std::string("Invalid numeric value: \"") + param + "\" on option -" + m_optionChar, SOURCEINFO);
    }
}

int CommandOption::getParamAsInt(size_t index, int minValue, int maxValue, int defaultValue) const
{
    // if this option was not present on the command line then return the default value.
    if (!isPresent())
        return defaultValue;

    int value = getParamAsInt(index);
    if ((value < minValue) || (value > maxValue))
        throw CommandOptionException(std::string("Value \"") + toString(value) + "\" out of range: [" +
                toString(minValue) + ".." + toString(maxValue) + "] on option -" + m_optionChar, SOURCEINFO);


    return value;
}

long CommandOption::getParamAsLong(size_t index, long minValue, long maxValue, long defaultValue) const
{
    // if this option was not present on the command line then return the default value.
    if (!isPresent())
        return defaultValue;

    long value = getParamAsLong(index);
    if ((value < minValue) || (value > maxValue))
        throw CommandOptionException(std::string("Value \"") + toString(value) + "\" out of range: [" +
            toString(minValue) + ".." + toString(maxValue) + "] on option -" + m_optionChar, SOURCEINFO);


    return value;
}

void CommandOption::validate() const
{
    if (!isPresent())
        return;

    if (m_params.size() > m_maxParams)
        throw CommandOptionException(std::string("option -") + m_optionChar + " has too many parameters specified.", SOURCEINFO);

    if (m_params.size() < m_minParams)
        throw CommandOptionException(std::string("option -") + m_optionChar + " has too few parameters specified.", SOURCEINFO);
}

}}
