package org.limitless.seqeron.proxy;

import java.util.Properties;

/**
 * One host's {@code proxy.*} properties. In a process that runs several ({@code proxy.hosts}), {@code
 * proxy.<name>.<key>} comes before {@code proxy.<key>}, which the hosts share; a lone host has no name.
 *
 * @param name   the host's name in {@code proxy.hosts}, or null for a lone host
 * @param source where the properties are read
 */
record HostProperties(String name, Properties source) {
    String get(final String key) {
        final String own = name == null ? null : source.getProperty("proxy." + name + "." + key);
        return own != null ? own : source.getProperty("proxy." + key);
    }

    String get(final String key, final String fallback) {
        final String value = get(key);
        return value != null ? value : fallback;
    }

    long number(final String key, final long fallback) {
        final String value = get(key);
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value.trim());
        } catch (final NumberFormatException ex) {
            throw new IllegalArgumentException(qualified(key) + " is not a number: " + value);
        }
    }

    boolean flag(final String key) {
        return Boolean.parseBoolean(get(key));
    }

    /** The property's name as an error names it. */
    String qualified(final String key) {
        return name == null ? "proxy." + key : "proxy." + name + "." + key;
    }
}
