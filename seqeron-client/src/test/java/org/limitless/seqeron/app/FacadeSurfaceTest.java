package org.limitless.seqeron.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.aeron.Aeron;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.agrona.DirectBuffer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.limitless.seqeron.protocol.Publish;

/**
 * The façades' surface is closed: a consumer that takes one names {@code app}, {@code protocol.Publish},
 * {@code Aeron} and {@code DirectBuffer}, and nothing else. {@code package-info.java} and
 * {@code doc/client-api.md} both say so in prose; this is what makes it fail the build. A public signature
 * that reaches into {@code sequencer.client}, {@code replayer.client}, the generated codecs or one of this
 * package's own package-private blocks is the leak it catches.
 */
class FacadeSurfaceTest {
    /** The front door: these types and their public nested types are what a consumer sees. */
    private static final Class<?>[] FACADES = {
        Gateway.class, Application.class, Payload.class, ClusterError.class
    };

    @Test
    @DisplayName("no façade signature names a type a consumer cannot, or should not, reach")
    void facadesNameNothingElse() {
        final List<String> leaks = new ArrayList<>();
        for (final Class<?> facade : FACADES) {
            check(facade, leaks);
        }
        assertEquals(List.of(), leaks, "the façade surface must stay app + Publish + Aeron + DirectBuffer");
    }

    /** Walks one type's own visible surface, then the public types nested in it. */
    private static void check(final Class<?> owner, final List<String> leaks) {
        inspect(owner, owner.getGenericSuperclass(), "extends", leaks);
        for (final Type iface : owner.getGenericInterfaces()) {
            inspect(owner, iface, "implements", leaks);
        }
        for (final Constructor<?> ctor : owner.getDeclaredConstructors()) {
            if (isVisible(ctor.getModifiers()) && !ctor.isSynthetic()) {
                inspectAll(owner, ctor.getGenericParameterTypes(), "constructor parameter", leaks);
                inspectAll(owner, ctor.getGenericExceptionTypes(), "constructor throws", leaks);
            }
        }
        for (final Method method : owner.getDeclaredMethods()) {
            if (isVisible(method.getModifiers()) && !method.isSynthetic()) {
                inspect(owner, method.getGenericReturnType(), method.getName() + "() returns", leaks);
                inspectAll(owner, method.getGenericParameterTypes(), method.getName() + "() parameter", leaks);
                inspectAll(owner, method.getGenericExceptionTypes(), method.getName() + "() throws", leaks);
            }
        }
        for (final Field field : owner.getDeclaredFields()) {
            if (isVisible(field.getModifiers()) && !field.isSynthetic()) {
                inspect(owner, field.getGenericType(), "field " + field.getName(), leaks);
            }
        }
        for (final Class<?> nested : owner.getDeclaredClasses()) {
            if (Modifier.isPublic(nested.getModifiers())) {
                check(nested, leaks);
            }
        }
    }

    private static boolean isVisible(final int modifiers) {
        return Modifier.isPublic(modifiers) || Modifier.isProtected(modifiers);
    }

    private static void inspectAll(final Class<?> owner, final Type[] types, final String where,
                                   final List<String> leaks) {
        for (final Type type : types) {
            inspect(owner, type, where, leaks);
        }
    }

    private static void inspect(final Class<?> owner, final Type type, final String where,
                                final List<String> leaks) {
        for (final Class<?> named : rawTypes(type)) {
            if (!isAllowed(named)) {
                leaks.add(owner.getName() + ": " + where + " " + named.getName());
            }
        }
    }

    /** Every class a generic type names — the raw type, its arguments, and the bounds of either. */
    private static Set<Class<?>> rawTypes(final Type type) {
        final Set<Class<?>> raw = new HashSet<>();
        walk(type, raw, new HashSet<>());
        return raw;
    }

    private static void walk(final Type type, final Set<Class<?>> raw, final Set<Type> seen) {
        if (type == null || !seen.add(type)) {
            return;
        }
        if (type instanceof Class<?> cls) {
            raw.add(cls);
        } else if (type instanceof ParameterizedType parameterized) {
            walk(parameterized.getRawType(), raw, seen);
            for (final Type argument : parameterized.getActualTypeArguments()) {
                walk(argument, raw, seen);
            }
        } else if (type instanceof GenericArrayType array) {
            walk(array.getGenericComponentType(), raw, seen);
        } else if (type instanceof WildcardType wildcard) {
            for (final Type bound : wildcard.getUpperBounds()) {
                walk(bound, raw, seen);
            }
            for (final Type bound : wildcard.getLowerBounds()) {
                walk(bound, raw, seen);
            }
        } else if (type instanceof TypeVariable<?> variable) {
            for (final Type bound : variable.getBounds()) {
                walk(bound, raw, seen);
            }
        } else {
            throw new IllegalStateException("unhandled type: " + type.getTypeName());
        }
    }

    private static boolean isAllowed(final Class<?> named) {
        Class<?> type = named;
        while (type.isArray()) {
            type = type.getComponentType();
        }
        if (type.isPrimitive() || type.getName().startsWith("java.")) {
            return true;
        }
        if (type == DirectBuffer.class || type == Aeron.class || type == Publish.class) {
            return true;
        }
        // An app type counts only if a consumer can see it: a package-private block named by a public
        // signature is exactly the leak this test exists for.
        return type.getName().startsWith("org.limitless.seqeron.app.") && isPubliclyVisible(type);
    }

    /** Public, and nested in nothing that is not: {@code GatewayLifecycle.State} is a public enum
     *  inside a package-private class, so a consumer cannot name it. */
    private static boolean isPubliclyVisible(final Class<?> type) {
        for (Class<?> cls = type; cls != null; cls = cls.getEnclosingClass()) {
            if (!Modifier.isPublic(cls.getModifiers())) {
                return false;
            }
        }
        return true;
    }
}
