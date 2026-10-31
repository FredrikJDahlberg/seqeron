package org.limitless.seqeron.proxy;

import org.limitless.seqeron.protocol.ProxyProtocol;
import org.limitless.seqeron.sbe.hosted.Kind;

/**
 * Who a ClientProxy answers, and how (doc/hosted-facade-draft.md H-8): a resend of the live request is answered
 * again, any other client is refused as busy, and a client of another protocol, Aeron version or kind is refused.
 * Holds no Aeron object, and no state of the live attachment: the proxy passes it in.
 */
public final class AttachRules {
    /** What the proxy does with an {@code Attach}. */
    public enum Decision {
        /** Not addressed to this proxy: no answer. */
        IGNORE,
        /** The live request again: answer {@code Attached} again. */
        RESEND,
        /** Another client holds the attachment. */
        BUSY,
        /** The protocol or the Aeron version differs. */
        VERSION,
        /** The kind is one this proxy does not run. */
        KIND,
        /** Start the façade. */
        ACCEPT
    }

    private final String hostName;
    private final String aeronVersion;
    private final boolean servesGateway;
    private final boolean servesConsumer;

    /**
     * @param hostName       the name an {@code Attach} must carry
     * @param aeronVersion   the version of Aeron this proxy runs; a client must run the same
     * @param servesGateway  whether this proxy is configured with a gateway row
     * @param servesConsumer whether this proxy is configured with a queue group to consume
     */
    public AttachRules(final String hostName, final String aeronVersion, final boolean servesGateway,
                       final boolean servesConsumer) {
        this.hostName = hostName;
        this.aeronVersion = aeronVersion;
        this.servesGateway = servesGateway;
        this.servesConsumer = servesConsumer;
    }

    /** The rules of a proxy that serves the default host name. */
    public static AttachRules of(final String aeronVersion, final boolean servesGateway, final boolean servesConsumer) {
        return new AttachRules(ProxyProtocol.HOST_NAME, aeronVersion, servesGateway, servesConsumer);
    }

    /**
     * @param live            whether an attachment is live
     * @param liveClientId    its client, if live
     * @param liveRequestId   its request, if live
     */
    public Decision decide(final boolean live, final long liveClientId, final long liveRequestId,
                           final String requestedHostName, final long clientId, final long requestId, final Kind kind,
                           final int protocolVersion, final String clientAeronVersion) {
        if (!hostName.equals(requestedHostName)) {
            return Decision.IGNORE;
        }
        if (live) {
            return clientId == liveClientId && requestId == liveRequestId ? Decision.RESEND : Decision.BUSY;
        }
        if (protocolVersion != ProxyProtocol.PROTOCOL_VERSION || !aeronVersion.equals(clientAeronVersion)) {
            return Decision.VERSION;
        }
        final boolean served = kind == Kind.APPLICATION || kind == Kind.GATEWAY && servesGateway ||
                               kind == Kind.CONSUMER && servesConsumer;
        return served ? Decision.ACCEPT : Decision.KIND;
    }
}
