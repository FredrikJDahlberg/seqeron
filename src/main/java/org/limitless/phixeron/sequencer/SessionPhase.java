package org.limitless.phixeron.sequencer;

public enum SessionPhase
{
    DISCONNECTED,
    LOGON_PENDING,
    ACTIVE,
    LOGOUT_PENDING;

    public byte encode()
    {
        return (byte) ordinal();
    }

    public static SessionPhase decode(final byte b)
    {
        return values()[b & 0xFF];
    }
}