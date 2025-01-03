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
package io.aeron;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

class AeronContextTest
{
    @NullAndEmptySource
    @ParameterizedTest
    void shouldUseEmptyStringIfNameIsEmpty(final String name)
    {
        final Aeron.Context context = new Aeron.Context();
        assertEquals("", context.clientName());

        context.clientName(name);
        assertEquals("", context.clientName());
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "gdajsdgajsd", "7326482374hdfy7dsyf8dyf9sd.-)"})
    void shouldAssignClientName(final String clientName)
    {
        final Aeron.Context context = new Aeron.Context();

        context.clientName(clientName);
        assertSame(clientName, context.clientName());
    }
}
