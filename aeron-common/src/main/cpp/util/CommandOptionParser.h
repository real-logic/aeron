#pragma once

#include <string>
#include <vector>
#include <map>
#include <iostream>

#include "CommandOption.h"

namespace aeron { namespace common { namespace util {

class CommandOptionParser
{
    private:
        std::map<char, CommandOption> m_options;

    public:
        CommandOptionParser ();

        void parse (int argc, char** argv);
        void addOption (const CommandOption& option);
        CommandOption& getOption (char optionChar);

        void displayOptionsHelp (std::ostream& out);
};

}}}
