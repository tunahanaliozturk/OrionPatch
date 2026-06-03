namespace Moongazing.OrionPatch.Tests.Configuration;

using Microsoft.Extensions.DependencyInjection;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.DependencyInjection;
using Moongazing.OrionPatch.Internal;
using Moongazing.OrionPatch.Models;
using Xunit;

public sealed class MessageTypeRegistryTests
{
    public sealed class OrderShipped { }
    public sealed class OrderShippedV2 { }
    public sealed class CustomerRegistered { }

    [Fact]
    public void Empty_registry_has_zero_mappings_and_default_fallback_enabled()
    {
        var reg = MessageTypeRegistry.Empty;

        Assert.Null(reg.ResolveLogicalName(typeof(OrderShipped)));
        Assert.Null(reg.ResolveClrType("anything"));
        Assert.True(reg.AllowAssemblyQualifiedNameFallback);
    }

    [Fact]
    public void Empty_registry_options_are_immutable_after_build()
    {
        // Guards against accidental mutation of the shared Empty singleton's behaviour.
        // The registry snapshots the bool at construction, so external mutation of any
        // options instance can never flip the global Empty's fallback flag.
        var reg = MessageTypeRegistry.Empty;
        Assert.True(reg.AllowAssemblyQualifiedNameFallback);

        // Build a new registry with fallback disabled and verify Empty is unaffected.
        _ = new MessageTypeRegistryBuilder()
            .Configure(o => o.AllowAssemblyQualifiedNameFallback = false)
            .Build();
        Assert.True(MessageTypeRegistry.Empty.AllowAssemblyQualifiedNameFallback);
    }

    [Fact]
    public void Builder_round_trips_mapping_in_both_directions()
    {
        var reg = new MessageTypeRegistryBuilder()
            .Map<OrderShipped>("OrderShipped")
            .Map<OrderShippedV2>("OrderShipped.V2")
            .Build();

        Assert.Equal("OrderShipped", reg.ResolveLogicalName(typeof(OrderShipped)));
        Assert.Equal("OrderShipped.V2", reg.ResolveLogicalName(typeof(OrderShippedV2)));
        Assert.Equal(typeof(OrderShipped), reg.ResolveClrType("OrderShipped"));
        Assert.Equal(typeof(OrderShippedV2), reg.ResolveClrType("OrderShipped.V2"));
    }

    [Fact]
    public void Builder_throws_on_duplicate_type()
    {
        var builder = new MessageTypeRegistryBuilder()
            .Map<OrderShipped>("OrderShipped");

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.Map<OrderShipped>("DifferentName"));
        Assert.Contains("OrderShipped", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Builder_throws_on_duplicate_logical_name()
    {
        var builder = new MessageTypeRegistryBuilder()
            .Map<OrderShipped>("OrderShipped");

        var ex = Assert.Throws<InvalidOperationException>(
            () => builder.Map<CustomerRegistered>("OrderShipped"));
        Assert.Contains("OrderShipped", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Builder_throws_on_whitespace_logical_name()
    {
        var builder = new MessageTypeRegistryBuilder();
        Assert.Throws<ArgumentException>(() => builder.Map<OrderShipped>("   "));
    }

    [Fact]
    public void Resolver_prefers_per_enqueue_override_over_registry()
    {
        var reg = new MessageTypeRegistryBuilder()
            .Map<OrderShipped>("OrderShipped")
            .Build();
        var resolver = new MessageTypeNameResolver(reg);

        var name = resolver.Resolve(typeof(OrderShipped), new OutboxEnqueueOptions { MessageType = "override" });

        Assert.Equal("override", name);
    }

    [Fact]
    public void Resolver_prefers_registry_over_fallback_when_no_override()
    {
        var reg = new MessageTypeRegistryBuilder()
            .Map<OrderShipped>("OrderShipped")
            .Build();
        var resolver = new MessageTypeNameResolver(reg);

        Assert.Equal("OrderShipped", resolver.Resolve(typeof(OrderShipped), options: null));
    }

    [Fact]
    public void Resolver_falls_back_to_FullName_when_unmapped_and_fallback_enabled()
    {
        var reg = MessageTypeRegistry.Empty;
        var resolver = new MessageTypeNameResolver(reg);

        Assert.Equal(typeof(OrderShipped).FullName, resolver.Resolve(typeof(OrderShipped), options: null));
    }

    [Fact]
    public void Resolver_throws_when_unmapped_and_fallback_disabled()
    {
        var reg = new MessageTypeRegistryBuilder()
            .Configure(o => o.AllowAssemblyQualifiedNameFallback = false)
            .Build();
        var resolver = new MessageTypeNameResolver(reg);

        var ex = Assert.Throws<InvalidOperationException>(
            () => resolver.Resolve(typeof(OrderShipped), options: null));
        Assert.Contains("AllowAssemblyQualifiedNameFallback", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void DI_extension_replaces_empty_default_with_user_registry()
    {
        var services = new ServiceCollection();
        services.AddOrionPatch()
            .UseMessageTypeRegistry(r => r
                .Map<OrderShipped>("OrderShipped"));

        using var provider = services.BuildServiceProvider();
        var reg = provider.GetRequiredService<MessageTypeRegistry>();

        Assert.Equal("OrderShipped", reg.ResolveLogicalName(typeof(OrderShipped)));
        Assert.Equal(typeof(OrderShipped), reg.ResolveClrType("OrderShipped"));
    }

    [Fact]
    public void DI_extension_last_call_wins()
    {
        var services = new ServiceCollection();
        services.AddOrionPatch()
            .UseMessageTypeRegistry(r => r.Map<OrderShipped>("first"))
            .UseMessageTypeRegistry(r => r.Map<OrderShipped>("second"));

        using var provider = services.BuildServiceProvider();
        var reg = provider.GetRequiredService<MessageTypeRegistry>();

        Assert.Equal("second", reg.ResolveLogicalName(typeof(OrderShipped)));
    }
}
