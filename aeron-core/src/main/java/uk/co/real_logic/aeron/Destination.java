/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.aeron;

/**
 * Interface for Aeron session address.
 *
 * Destinations follow a URI-type scheme
 *
 * {@code
 * UDP (unicast and multicast) - https://www.iana.org/assignments/uri-schemes/prov/udp
 *  udp://<server>[:<port>]/
 *  udp://[<localaddress>@]<destination|multicastgroup>[:<port>]
 *
 * InfiniBand
 *  TBD (16-bit LID?)
 *
 * Shared Memory (memory mapped files)
 *  shm:///path/name
 * }
 */
public class Destination
{
    private final String destination;

    public Destination(final String destination)
    {
        this.destination = destination;
    }

    public String destination()
    {
        return destination;
    }
}
