/*
 * Copyright 2016 Real Logic Ltd.
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

#ifndef INCLUDED_AERON_DRIVER_MEDIADRIVER_H_
#define INCLUDED_AERON_DRIVER_MEDIADRIVER_H_

#include <map>
#include <string>

namespace aeron { namespace driver {


class MediaDriver
{
public:
    class Context
    {
    };

    MediaDriver(std::map<std::string, std::string>& properties);
    MediaDriver(std::string& propertiesFile);

    ~MediaDriver();

private:
    std::map<std::string, std::string> m_properties;
};


}};



#endif //AERON_MEDIADRIVER_H
