package org.limitless.seqeron.proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.ProxyProtocol;
import org.limitless.seqeron.proxy.AttachRules.Decision;
import org.limitless.seqeron.sbe.hosted.Kind;

class AttachRulesTest {
    private static final String AERON = "1.53.2";

    private final AttachRules rules = AttachRules.of(AERON, true, false);

    private Decision decide(final boolean live, final long clientId, final long requestId, final Kind kind) {
        return rules.decide(live, 7, 70, ProxyProtocol.DEFAULT_HOST_NAME, clientId, requestId, kind,
                            ProxyProtocol.PROTOCOL_VERSION, AERON);
    }

    @Test
    void aFreeProxyAcceptsAnApplication() {
        assertEquals(Decision.ACCEPT, decide(false, 1, 1, Kind.APPLICATION));
    }

    @Test
    void aResendOfTheLiveRequestIsAnsweredAgain() {
        assertEquals(Decision.RESEND, decide(true, 7, 70, Kind.APPLICATION));
    }

    @Test
    void anotherClientOrAnotherRequestOfTheLiveClientIsBusy() {
        assertEquals(Decision.BUSY, decide(true, 8, 70, Kind.APPLICATION));
        assertEquals(Decision.BUSY, decide(true, 7, 71, Kind.APPLICATION));
    }

    @Test
    void anAttachNamedForAnotherHostIsNotAnswered() {
        assertEquals(Decision.IGNORE, rules.decide(false, 0, 0, "other", 1, 1, Kind.APPLICATION,
                                                   ProxyProtocol.PROTOCOL_VERSION, AERON));
        assertEquals(Decision.IGNORE, rules.decide(true, 7, 70, "other", 7, 70, Kind.APPLICATION,
                                                   ProxyProtocol.PROTOCOL_VERSION, AERON));
    }

    @Test
    void aDifferentProtocolOrAeronVersionIsRefusedAsVersion() {
        assertEquals(Decision.VERSION, rules.decide(false, 0, 0, ProxyProtocol.DEFAULT_HOST_NAME, 1, 1, Kind.APPLICATION,
                                                    ProxyProtocol.PROTOCOL_VERSION + 1, AERON));
        assertEquals(Decision.VERSION, rules.decide(false, 0, 0, ProxyProtocol.DEFAULT_HOST_NAME, 1, 1, Kind.APPLICATION,
                                                    ProxyProtocol.PROTOCOL_VERSION, "1.53.1"));
    }

    @Test
    void aKindThisProxyIsNotConfiguredForIsRefusedAsKind() {
        assertEquals(Decision.ACCEPT, decide(false, 1, 1, Kind.GATEWAY));
        assertEquals(Decision.KIND, decide(false, 1, 1, Kind.CONSUMER));
        assertEquals(Decision.KIND, AttachRules.of(AERON, false, true).decide(false, 0, 0, ProxyProtocol.DEFAULT_HOST_NAME, 1, 1,
                                                                             Kind.GATEWAY, ProxyProtocol.PROTOCOL_VERSION, AERON));
    }

    @Test
    void aBusyProxyAnswersBusyBeforeItChecksTheVersion() {
        assertEquals(Decision.BUSY, rules.decide(true, 7, 70, ProxyProtocol.DEFAULT_HOST_NAME, 8, 1, Kind.CONSUMER, 99, "x"));
    }
}
