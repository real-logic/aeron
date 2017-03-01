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

namespace aeron { namespace util {

CommandOptionParser::CommandOptionParser ()
{
    addOption(CommandOption(CommandOption::UNNAMED, 0, 0, "Unnamed Options"));
}

void CommandOptionParser::parse (int argc, char** argv)
{
    char currentOption = CommandOption::UNNAMED;
    getOption(currentOption).setPresent();

    // start at 1 as this is the program name.
    for (int n = 1; n < argc; n++)
    {
        std::string argStr (argv[n]);
        // is this an option?
        if ((argStr.size() >= 2) && (argStr[0] == '-'))
        {
            for (size_t argNum = 1; argNum < argStr.size(); argNum++)
            {
                currentOption = argStr[argNum];

                // is this a valid option ?
                auto opt = m_options.find(currentOption);
                if (m_options.end() == opt)
                    throw CommandOptionException(std::string ("-") + currentOption + " is not a valid command option.", SOURCEINFO);
                else
                    opt->second.setPresent();
            }
        }
        else
        {
            // add param to current option
            CommandOption& opt = getOption(currentOption);
            opt.addParam(argStr);
        }
    }

    // validate the options. the validate method on each option will throw if the options are not valid.
    for (auto opt = m_options.begin(); opt != m_options.end(); ++opt)
    {
        opt->second.validate();
    }
}

void CommandOptionParser::addOption (const CommandOption& option)
{
    m_options[option.getOptionChar()] = option;
}

CommandOption& CommandOptionParser::getOption (char optionChar)
{
    auto opt = m_options.find(optionChar);

    if (m_options.end() == opt)
    {
        throw CommandOptionException(std::string ("CommandOptionParser::getOption invalid option lookup: ") + optionChar, SOURCEINFO);
    }

    return opt->second;
}

void CommandOptionParser::displayOptionsHelp (std::ostream& out) const
{
    for (auto i = m_options.begin(); i != m_options.end(); ++i)
    {
        if (i->first != CommandOption::UNNAMED)
            out << "    -" << i->first << " " << i->second.getHelpText() << std::endl;
    }
}

}}
