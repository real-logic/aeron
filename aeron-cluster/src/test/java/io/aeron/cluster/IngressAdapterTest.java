package io.aeron.cluster;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.function.Consumer;
import java.util.stream.Stream;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.AdminRequestDecoder;
import io.aeron.cluster.codecs.AdminRequestEncoder;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.ChallengeResponseDecoder;
import io.aeron.cluster.codecs.ChallengeResponseEncoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionCloseRequestDecoder;
import io.aeron.cluster.codecs.SessionCloseRequestEncoder;
import io.aeron.cluster.codecs.SessionConnectRequestDecoder;
import io.aeron.cluster.codecs.SessionConnectRequestEncoder;
import io.aeron.cluster.codecs.SessionKeepAliveDecoder;
import io.aeron.cluster.codecs.SessionKeepAliveEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;

class IngressAdapterTest
{
    private IngressAdapter adapter;
    private ConsensusModuleAgent consensusModuleAgent;
    private ExpandableArrayBuffer buffer;

    @BeforeEach
    public void setup()
    {
        buffer = new ExpandableArrayBuffer();
        consensusModuleAgent = mock(ConsensusModuleAgent.class);
        adapter = new IngressAdapter(0, consensusModuleAgent);
    }

    @Test
    public void shouldBlowUpOnUnexpectedSchema()
    {
        new MessageHeaderEncoder()
            .wrap(buffer, 0)
            .blockLength(0)
            .schemaId(MessageHeaderDecoder.SCHEMA_ID + 1)
            .templateId(0)
            .version(0);

        assertThrows(ClusterException.class,
            () -> adapter.onFragment(buffer, 0, MessageHeaderDecoder.ENCODED_LENGTH, null));
    }


    @SuppressWarnings("unused") // name used for test display name
    @ParameterizedTest(name = "{index} {0}")
    @MethodSource
    public void shouldDelegateToConsensusModuleAgentByTemplateId(
        final String name,
        final Consumer<MutableDirectBuffer> encoder,
        final int blockLength,
        final ConsensusModuleAgentExpectation expectation)
    {
        encoder.accept(buffer);

        final int length = MessageHeaderDecoder.ENCODED_LENGTH + blockLength;

        adapter.onFragment(buffer, 0, length, null);

        expectation.expect(verify(consensusModuleAgent), buffer);
    }

    public static Stream<Arguments> shouldDelegateToConsensusModuleAgentByTemplateId()
    {
        return Stream.of(
            Arguments.of(
                "SessionMessageHeaderDecoder",
                (Consumer<MutableDirectBuffer>) buffer ->
                {
                    new SessionMessageHeaderEncoder()
                        .wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                        .leadershipTermId(17)
                        .clusterSessionId(19)
                        .timestamp(23);
                },
                SessionMessageHeaderDecoder.BLOCK_LENGTH,
                (ConsensusModuleAgentExpectation) (a, buffer) ->
                {
                    a.onIngressMessage(17, 19, buffer,
                        MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH, 0);
                }),
            Arguments.of(
                "SessionConnectRequestDecoder",
                (Consumer<MutableDirectBuffer>) buffer ->
                {
                    new SessionConnectRequestEncoder()
                        .wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                        .correlationId(17)
                        .responseStreamId(19)
                        .version(23)
                        .responseChannel("x");
                },
                SessionConnectRequestDecoder.BLOCK_LENGTH,
                (ConsensusModuleAgentExpectation) (a, buffer) ->
                {
                    a.onSessionConnect(17, 19, 23, "x", new byte[0]);
                }),
            Arguments.of(
                "SessionCloseRequestDecoder",
                (Consumer<MutableDirectBuffer>) buffer ->
                {
                    new SessionCloseRequestEncoder()
                        .wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                        .leadershipTermId(17)
                        .clusterSessionId(19);
                },
                SessionCloseRequestDecoder.BLOCK_LENGTH,
                (ConsensusModuleAgentExpectation) (a, buffer) ->
                {
                    a.onSessionClose(17, 19);
                }),
            Arguments.of(
                "SessionKeepAliveDecoder",
                (Consumer<MutableDirectBuffer>) buffer ->
                {
                    new SessionKeepAliveEncoder()
                        .wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                        .leadershipTermId(17)
                        .clusterSessionId(19);
                },
                SessionKeepAliveDecoder.BLOCK_LENGTH,
                (ConsensusModuleAgentExpectation) (a, buffer) ->
                {
                    a.onSessionKeepAlive(17, 19);
                }),
            Arguments.of(
                "ChallengeResponseDecoder",
                (Consumer<MutableDirectBuffer>) buffer ->
                {
                    final byte[] challenge = "foo".getBytes();

                    new ChallengeResponseEncoder()
                        .wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                        .correlationId(17)
                        .clusterSessionId(19)
                        .putEncodedCredentials(challenge, 0, challenge.length);
                },
                ChallengeResponseDecoder.BLOCK_LENGTH,
                (ConsensusModuleAgentExpectation) (a, buffer) ->
                {
                    a.onIngressChallengeResponse(17, 19, "foo".getBytes());
                }),
            Arguments.of(
                "AdminRequestDecoder",
                (Consumer<MutableDirectBuffer>) buffer ->
                {
                    final byte[] payload = "foo".getBytes();
                    new AdminRequestEncoder()
                        .wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                        .leadershipTermId(17)
                        .clusterSessionId(19)
                        .correlationId(23)
                        .putPayload(payload, 0, payload.length);
                },
                AdminRequestDecoder.BLOCK_LENGTH,
                (ConsensusModuleAgentExpectation) (a, buffer) ->
                {
                    a.onAdminRequest(17, 19, 23,
                        AdminRequestType.SNAPSHOT, buffer,
                        AdminRequestDecoder.BLOCK_LENGTH +
                                        MessageHeaderDecoder.ENCODED_LENGTH +
                                        AdminRequestDecoder.payloadHeaderLength(),
                        "foo".getBytes().length);
                })
        );
    }

    private interface ConsensusModuleAgentExpectation
    {
        void expect(ConsensusModuleAgent agent, DirectBuffer buffer);
    }
}