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
#ifndef AERON_UTIL_SCOPE_FILE_H
#define AERON_UTIL_SCOPE_FILE_H

#include <memory>

namespace aeron { namespace util
{

class OnScopeExit
{
public:
    template<typename func_t>
    inline explicit OnScopeExit(const func_t &func) : m_holder(new FuncHolder<func_t>(func))
    {
    }

private:
    struct FuncHolderBase
    {
        virtual ~FuncHolderBase() = default;
    };

    template<typename func_t>
    struct FuncHolder : public FuncHolderBase
    {
        func_t f;

        inline explicit FuncHolder(const func_t &func) : f(func)
        {
        }

        virtual ~FuncHolder()
        {
            f();
        }
    };

    std::unique_ptr<FuncHolderBase> m_holder;
};

class CallbackGuard
{
public:
    explicit CallbackGuard(bool &isInCallback) : m_isInCallback(isInCallback)
    {
        m_isInCallback = true;
    }

    ~CallbackGuard()
    {
        m_isInCallback = false;
    }

    CallbackGuard(const CallbackGuard &) = delete;

    CallbackGuard &operator=(const CallbackGuard &) = delete;

private:
    bool &m_isInCallback;
};

}}


#endif
