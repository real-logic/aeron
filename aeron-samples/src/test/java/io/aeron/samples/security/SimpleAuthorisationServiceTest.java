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
package io.aeron.samples.security;

import io.aeron.archive.codecs.MessageHeaderDecoder;
import io.aeron.archive.codecs.ReplicateRequest2Decoder;
import io.aeron.archive.codecs.StartRecordingRequestDecoder;
import io.aeron.archive.codecs.TruncateRecordingRequestDecoder;
import io.aeron.cluster.codecs.BackupQueryDecoder;
import io.aeron.security.AuthorisationService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.security.AuthorisationService.ALLOW_ALL;
import static io.aeron.security.AuthorisationService.DENY_ALL;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("checkstyle:Indentation")
class SimpleAuthorisationServiceTest
{
    private static final int ARCHIVE_PROTOCOL_ID = MessageHeaderDecoder.SCHEMA_ID;
    private static final int CLUSTER_PROTOCOL_ID = io.aeron.cluster.codecs.MessageHeaderDecoder.SCHEMA_ID;
    private static final int OTHER_PROTOCOL_ID = 873648576;

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldEnableGeneralAuthorisationForProtocol(final boolean shouldAcceptByDefault)
    {
        final byte[] encodedPrincipal = "user".getBytes(US_ASCII);
        final AuthorisationService defaultAuthorisation = shouldAcceptByDefault ? ALLOW_ALL : DENY_ALL;

        final SimpleAuthorisationService simpleAuthorisationService = new SimpleAuthorisationService.Builder()
            .defaultAuthorisation(defaultAuthorisation)
            .addGeneralRule(ARCHIVE_PROTOCOL_ID, true)
            .addGeneralRule(CLUSTER_PROTOCOL_ID, false)
            .newInstance();

        assertTrue(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, null, encodedPrincipal));

        assertFalse(simpleAuthorisationService.isAuthorised(
            CLUSTER_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, null, encodedPrincipal));

        assertEquals(shouldAcceptByDefault, simpleAuthorisationService.isAuthorised(
            OTHER_PROTOCOL_ID, ReplicateRequest2Decoder.TEMPLATE_ID, null, encodedPrincipal));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldEnableGeneralAuthorisationForProtocolAndMessage(final boolean shouldAcceptByDefault)
    {
        final byte[] encodedPrincipal = "user".getBytes(US_ASCII);
        final AuthorisationService defaultAuthorisation = shouldAcceptByDefault ? ALLOW_ALL : DENY_ALL;

        final SimpleAuthorisationService simpleAuthorisationService = new SimpleAuthorisationService.Builder()
            .defaultAuthorisation(defaultAuthorisation)
            .addGeneralRule(ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, true)
            .addGeneralRule(ARCHIVE_PROTOCOL_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, false)
            .newInstance();

        assertTrue(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, null, encodedPrincipal));

        assertFalse(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, null, encodedPrincipal));

        assertEquals(shouldAcceptByDefault, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, ReplicateRequest2Decoder.TEMPLATE_ID, null, encodedPrincipal));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldEnableGeneralAuthorisationForProtocolAndMessageAndType(final boolean shouldAcceptByDefault)
    {
        final byte[] encodedPrincipal = "user".getBytes(US_ASCII);
        final AuthorisationService defaultAuthorisation = shouldAcceptByDefault ? ALLOW_ALL : DENY_ALL;
        final String typeAllowed = "allowed";
        final String typeDenied = "denied";
        final String typeUnspecified = "unspecified";

        final SimpleAuthorisationService simpleAuthorisationService = new SimpleAuthorisationService.Builder()
            .defaultAuthorisation(defaultAuthorisation)
            .addGeneralRule(ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeAllowed, true)
            .addGeneralRule(ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeDenied, false)
            .newInstance();

        assertTrue(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeAllowed, encodedPrincipal));

        assertFalse(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeDenied, encodedPrincipal));

        assertEquals(shouldAcceptByDefault, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeUnspecified, encodedPrincipal));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldEnableUserSpecificAuthorisationForProtocol(final boolean shouldAcceptByDefault)
    {
        final byte[] encodedPrincipal = "user".getBytes(US_ASCII);
        final AuthorisationService defaultAuthorisation = shouldAcceptByDefault ? ALLOW_ALL : DENY_ALL;

        final SimpleAuthorisationService simpleAuthorisationService = new SimpleAuthorisationService.Builder()
            .defaultAuthorisation(defaultAuthorisation)
            .addPrincipalRule(ARCHIVE_PROTOCOL_ID, encodedPrincipal, true)
            .addPrincipalRule(CLUSTER_PROTOCOL_ID, encodedPrincipal, false)
            .newInstance();

        assertTrue(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, null, encodedPrincipal));

        assertTrue(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, "some resource", encodedPrincipal));

        assertFalse(simpleAuthorisationService.isAuthorised(
            CLUSTER_PROTOCOL_ID, BackupQueryDecoder.TEMPLATE_ID, null, encodedPrincipal));

        assertFalse(simpleAuthorisationService.isAuthorised(
            CLUSTER_PROTOCOL_ID, BackupQueryDecoder.TEMPLATE_ID, "some resource", encodedPrincipal));

        assertEquals(shouldAcceptByDefault, simpleAuthorisationService.isAuthorised(
            OTHER_PROTOCOL_ID, ReplicateRequest2Decoder.TEMPLATE_ID, null, encodedPrincipal));

        assertEquals(shouldAcceptByDefault, simpleAuthorisationService.isAuthorised(
            OTHER_PROTOCOL_ID, ReplicateRequest2Decoder.TEMPLATE_ID, "some resource", encodedPrincipal));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldEnableUserSpecificAuthorisationForProtocolAndMessage(final boolean shouldAcceptByDefault)
    {
        final byte[] encodedPrincipal = "user".getBytes(US_ASCII);
        final AuthorisationService defaultAuthorisation = shouldAcceptByDefault ? ALLOW_ALL : DENY_ALL;

        final SimpleAuthorisationService simpleAuthorisationService = new SimpleAuthorisationService.Builder()
            .defaultAuthorisation(defaultAuthorisation)
            .addPrincipalRule(ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, encodedPrincipal, true)
            .addPrincipalRule(ARCHIVE_PROTOCOL_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, encodedPrincipal, false)
            .newInstance();

        assertTrue(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, null, encodedPrincipal));

        assertTrue(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, "some resource", encodedPrincipal));

        assertFalse(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, null, encodedPrincipal));

        assertFalse(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, "some resource", encodedPrincipal));

        assertEquals(shouldAcceptByDefault, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, ReplicateRequest2Decoder.TEMPLATE_ID, null, encodedPrincipal));

        assertEquals(shouldAcceptByDefault, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, ReplicateRequest2Decoder.TEMPLATE_ID, "some resource", encodedPrincipal));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldEnableUserSpecificAuthorisationForProtocolAndMessageAndType(final boolean shouldAcceptByDefault)
    {
        final byte[] encodedPrincipal = "user".getBytes(US_ASCII);
        final AuthorisationService defaultAuthorisation = shouldAcceptByDefault ? ALLOW_ALL : DENY_ALL;

        final String typeAllowed = "allowed";
        final String typeDenied = "denied";
        final String typeUnspecified = "unspecified";

        final SimpleAuthorisationService simpleAuthorisationService = new SimpleAuthorisationService.Builder()
            .defaultAuthorisation(defaultAuthorisation)
            .addPrincipalRule(
                ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeAllowed, encodedPrincipal, true)
            .addPrincipalRule(
                ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeDenied, encodedPrincipal, false)
            .newInstance();

        assertTrue(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeAllowed, encodedPrincipal));

        assertFalse(simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeDenied, encodedPrincipal));

        assertEquals(shouldAcceptByDefault, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeUnspecified, encodedPrincipal));

        assertEquals(shouldAcceptByDefault, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, null, encodedPrincipal));
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldPrioritisePrincipalOverGeneralRules(final boolean accept)
    {
        final byte[] encodedPrincipal = "user".getBytes(US_ASCII);
        final byte[] unknownPrincipal = "unknownUser".getBytes(US_ASCII);
        final String typeAllowed = "allowed";

        final SimpleAuthorisationService simpleAuthorisationService = new SimpleAuthorisationService.Builder()
            .defaultAuthorisation(!accept ? ALLOW_ALL : DENY_ALL)
            .addPrincipalRule(
                ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeAllowed, encodedPrincipal, accept)
            .addGeneralRule(
                ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeAllowed, !accept)
            .addPrincipalRule(
                ARCHIVE_PROTOCOL_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, encodedPrincipal, accept)
            .addGeneralRule(
                ARCHIVE_PROTOCOL_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, !accept)
            .addPrincipalRule(
                CLUSTER_PROTOCOL_ID, encodedPrincipal, accept)
            .addGeneralRule(
                CLUSTER_PROTOCOL_ID, !accept)
            .newInstance();

        assertEquals(accept, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeAllowed, encodedPrincipal));
        assertEquals(accept, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, null, encodedPrincipal));
        assertEquals(accept, simpleAuthorisationService.isAuthorised(
            CLUSTER_PROTOCOL_ID, BackupQueryDecoder.TEMPLATE_ID, null, encodedPrincipal));

        assertEquals(!accept, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, StartRecordingRequestDecoder.TEMPLATE_ID, typeAllowed, unknownPrincipal));
        assertEquals(!accept, simpleAuthorisationService.isAuthorised(
            ARCHIVE_PROTOCOL_ID, TruncateRecordingRequestDecoder.TEMPLATE_ID, null, unknownPrincipal));
        assertEquals(!accept, simpleAuthorisationService.isAuthorised(
            CLUSTER_PROTOCOL_ID, BackupQueryDecoder.TEMPLATE_ID, null, unknownPrincipal));
    }
}
