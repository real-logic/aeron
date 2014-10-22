#include "CommandOptionParser.h"

namespace aeron { namespace common { namespace util {

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
                std::map<char, CommandOption>::iterator opt = m_options.find(currentOption);
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
    std::map<char, CommandOption>::const_iterator opt;
    for (opt = m_options.begin(); opt != m_options.end(); ++opt)
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
    std::map<char, CommandOption>::iterator opt = m_options.find(optionChar);

    if (m_options.end() == opt)
    {
        throw CommandOptionException(std::string ("CommandOptionParser::getOption invalid option lookup: ") + optionChar, SOURCEINFO);
    }

    return opt->second;
}

void CommandOptionParser::displayOptionsHelp (std::ostream& out)
{
    for (std::map<char, CommandOption>::iterator i = m_options.begin(); i != m_options.end(); ++i)
    {
        if (i->first != CommandOption::UNNAMED)
            out << "    -" << i->first << " " << i->second.getHelpText() << std::endl;
    }
}

}}}