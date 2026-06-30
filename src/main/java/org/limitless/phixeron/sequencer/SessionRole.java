package org.limitless.phixeron.sequencer;

public enum SessionRole {
    ACCEPTOR,    // buy-side: waits for client Logon
    INITIATOR;   // sell-side: gateway sends Logon after connect

    public byte encode() {
        return (byte) ordinal();
    }

    public static SessionRole decode(final byte value) {
        return values()[value & 0xFF];
    }
}