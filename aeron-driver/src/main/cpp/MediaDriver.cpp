//
// Created by Michael Barker on 18/08/15.
//

#include "MediaDriver.h"

aeron::driver::MediaDriver::MediaDriver(std::map<std::string, std::string>& properties) :
    m_properties(std::move(properties))
{
}

aeron::driver::MediaDriver::MediaDriver(std::string &propertiesFile)
{

}

aeron::driver::MediaDriver::~MediaDriver()
{

}
