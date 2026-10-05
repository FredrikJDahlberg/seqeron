using System;
using System.Collections.Generic;
using System.Reflection;
using Adaptive.Aeron;
using Adaptive.Agrona;
using Org.Limitless.Seqeron.Protocol;
using Xunit;

namespace Org.Limitless.Seqeron.App;

/// <summary>
/// The façades' surface is closed: a consumer that takes one names <c>App</c>, <c>Protocol.Publish</c>,
/// <c>Aeron</c>, <c>IDirectBuffer</c> and <c>IMutableDirectBuffer</c>, and nothing else beyond <c>System</c>. A
/// public signature that reaches into <c>Sequencer.Client</c>, <c>Replayer.Client</c>, the generated codecs or one of
/// this namespace's own internal blocks is the leak it catches. <c>FacadeSurfaceTest.java</c> is its twin; Java's
/// nested builders and listeners are the options classes and listener interfaces here.
/// </summary>
public class FacadeSurfaceTest
{
    // The front door: these types and their public nested types are what a consumer sees.
    private static readonly Type[] Facades = {
        typeof(Gateway),     typeof(GatewayOptions),     typeof(IGatewayListener),
        typeof(Application), typeof(ApplicationOptions), typeof(IApplicationListener),
        typeof(Payload),     typeof(ClusterError),       typeof(ISnapshotListener)
    };

    private const BindingFlags Declared = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                                          BindingFlags.Static | BindingFlags.DeclaredOnly;

    [Fact(DisplayName = "no façade signature names a type a consumer cannot, or should not, reach")]
    public void FacadesNameNothingElse()
    {
        var leaks = new List<string>();
        foreach (Type facade in Facades)
        {
            Check(facade, leaks);
        }
        Assert.Empty(leaks);
    }

    // Walks one type's own visible surface, then the public types nested in it.
    private static void Check(Type owner, List<string> leaks)
    {
        Inspect(owner, owner.BaseType, "extends", leaks);
        foreach (Type iface in owner.GetInterfaces())
        {
            Inspect(owner, iface, "implements", leaks);
        }
        foreach (ConstructorInfo ctor in owner.GetConstructors(Declared))
        {
            if (IsVisible(ctor))
            {
                InspectParameters(owner, ctor.GetParameters(), "constructor parameter", leaks);
            }
        }
        foreach (MethodInfo method in owner.GetMethods(Declared))
        {
            if (IsVisible(method))
            {
                Inspect(owner, method.ReturnType, method.Name + "() returns", leaks);
                InspectParameters(owner, method.GetParameters(), method.Name + "() parameter", leaks);
            }
        }
        foreach (FieldInfo field in owner.GetFields(Declared))
        {
            if (field.IsPublic || field.IsFamily || field.IsFamilyOrAssembly)
            {
                Inspect(owner, field.FieldType, "field " + field.Name, leaks);
            }
        }
        foreach (EventInfo evt in owner.GetEvents(Declared))
        {
            Inspect(owner, evt.EventHandlerType, "event " + evt.Name, leaks);
        }
        foreach (Type nested in owner.GetNestedTypes(BindingFlags.Public))
        {
            Check(nested, leaks);
        }
    }

    // Property and event accessors are methods, so this covers them too.
    private static bool IsVisible(MethodBase method)
    {
        return method.IsPublic || method.IsFamily || method.IsFamilyOrAssembly;
    }

    private static void InspectParameters(Type owner, ParameterInfo[] parameters, string where, List<string> leaks)
    {
        foreach (ParameterInfo parameter in parameters)
        {
            Inspect(owner, parameter.ParameterType, where, leaks);
        }
    }

    private static void Inspect(Type owner, Type type, string where, List<string> leaks)
    {
        foreach (Type named in RawTypes(type))
        {
            if (!IsAllowed(named))
            {
                leaks.Add(owner.FullName + ": " + where + " " + named.FullName);
            }
        }
    }

    // Every type a signature names — the type, its element, its generic arguments, and their constraints.
    private static HashSet<Type> RawTypes(Type type)
    {
        var raw = new HashSet<Type>();
        Walk(type, raw, new HashSet<Type>());
        return raw;
    }

    private static void Walk(Type type, HashSet<Type> raw, HashSet<Type> seen)
    {
        if (type == null || !seen.Add(type))
        {
            return;
        }
        if (type.HasElementType)
        {
            Walk(type.GetElementType(), raw, seen);
        }
        else if (type.IsGenericParameter)
        {
            foreach (Type constraint in type.GetGenericParameterConstraints())
            {
                Walk(constraint, raw, seen);
            }
        }
        else if (type.IsGenericType)
        {
            raw.Add(type.GetGenericTypeDefinition());
            foreach (Type argument in type.GetGenericArguments())
            {
                Walk(argument, raw, seen);
            }
        }
        else
        {
            raw.Add(type);
        }
    }

    private static bool IsAllowed(Type type)
    {
        if (type.IsPrimitive || type.Namespace == "System" || type.Namespace.StartsWith("System."))
        {
            return true;
        }
        if (type == typeof(IDirectBuffer) || type == typeof(IMutableDirectBuffer) || type == typeof(Aeron) ||
            type == typeof(Publish))
        {
            return true;
        }
        // An App type counts only if a consumer can see it: an internal block named by a public signature is exactly
        // the leak this test exists for. IsVisible is false for a public type nested in an internal one too.
        return type.Namespace == typeof(Gateway).Namespace && type.IsVisible;
    }
}
