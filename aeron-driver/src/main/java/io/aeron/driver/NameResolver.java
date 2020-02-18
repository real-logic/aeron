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
package io.aeron.driver;

import java.net.InetAddress;

/**
 * Interface to allow resolving a name into an {@link InetAddress}.
 */
public interface NameResolver
{
    /**
     * Resolve a name and return the most up to date {@link InetAddress} that represents the name.
     *
     * @param name to resolve.
     * @return address for the name that most recently represents the name or null if not resolvable currently.
     */
    InetAddress resolve(String name);
}
