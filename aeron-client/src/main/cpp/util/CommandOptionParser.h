/*
 * Copyright 2014-2025 Real Logic Limited.
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
#ifndef AERON_UTIL_COMMAND_OPTION_PARSER_H
#define AERON_UTIL_COMMAND_OPTION_PARSER_H

#include <string>
#include <vector>
#include <map>
#include <iostream>

#include "util/CommandOption.h"
#include "util/Export.h"

namespace aeron { namespace util
{

class CLIENT_EXPORT CommandOptionParser
{
private:
    std::map<char, CommandOption> m_options;

public:
    CommandOptionParser();

    void parse(int argc, char **argv);

    void addOption(const CommandOption &option);

    CommandOption &getOption(char optionChar);

    void displayOptionsHelp(std::ostream &out) const;
};

}
}

#endif