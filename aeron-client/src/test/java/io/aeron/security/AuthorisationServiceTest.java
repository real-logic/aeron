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
package io.aeron.security;

import org.agrona.ErrorHandler;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.security.AuthorisationService.ALLOW_ALL;
import static io.aeron.security.AuthorisationService.DENY_ALL;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

class AuthorisationServiceTest
{
    @Test
    void shouldAllowAnyCommandIfAllowAllIsUsed()
    {
        final byte[] encodedCredentials = { 0x1, 0x2, 0x3 };
        final ErrorHandler errorHandler = mock(ErrorHandler.class);
        final int protocolId = 77;
        final int actionId = ThreadLocalRandom.current().nextInt();

        assertTrue(ALLOW_ALL.isAuthorised(protocolId, actionId, null, encodedCredentials));
        verifyNoInteractions(errorHandler);
    }

    @Test
    void shouldForbidAllCommandsIfDenyAllIsUsed()
    {
        final byte[] encodedCredentials = { 0x4, 0x5, 0x6 };
        final ErrorHandler errorHandler = mock(ErrorHandler.class);
        final int protocolId = 77;
        final int actionId = ThreadLocalRandom.current().nextInt();

        assertFalse(DENY_ALL.isAuthorised(protocolId, actionId, null, encodedCredentials));
        verifyNoInteractions(errorHandler);
    }
}
