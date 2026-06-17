using Moongazing.OrionPatch.Configuration;

namespace Moongazing.OrionPatch.Demo;

/// <summary>
/// The message-type registry decouples the wire name stored on an outbox row from the CLR type,
/// so you can rename or refactor message classes without breaking in-flight rows. It resolves both
/// directions: CLR type -> logical name (at enqueue) and logical name -> CLR type (at dispatch).
/// </summary>
public static class MessageTypeRegistryDemo
{
    public static void Run()
    {
        Console.WriteLine("\n== Message type registry (logical name <-> CLR type) ==");

        var registry = new MessageTypeRegistryBuilder()
            .Map<OrderConfirmed>("order.confirmed")
            // Document a rename: OrderShipped (v1) was replaced by OrderShippedV2 at a versioned name.
            .MapVersion<OrderShipped, OrderShippedV2>("order.shipped.v2")
            .Build();

        Console.WriteLine($"  CLR -> logical: OrderConfirmed   => {registry.ResolveLogicalName(typeof(OrderConfirmed))}");
        Console.WriteLine($"  CLR -> logical: OrderShippedV2   => {registry.ResolveLogicalName(typeof(OrderShippedV2))}");

        Console.WriteLine($"  logical -> CLR: 'order.confirmed'  => {registry.ResolveClrType("order.confirmed")?.Name}");
        Console.WriteLine($"  logical -> CLR: 'order.shipped.v2' => {registry.ResolveClrType("order.shipped.v2")?.Name}");

        // Unmapped lookups return null, signalling the caller to fall back to the CLR-derived name.
        var unmappedName = registry.ResolveLogicalName(typeof(PaymentCaptured));
        var unknownType = registry.ResolveClrType("does.not.exist");
        Console.WriteLine($"  unmapped CLR type PaymentCaptured -> logical: {unmappedName ?? "(null, falls back to FullName)"}");
        Console.WriteLine($"  unknown logical name -> CLR: {(unknownType is null ? "(null)" : unknownType.Name)}");

        Console.WriteLine($"  AllowAssemblyQualifiedNameFallback default: {registry.AllowAssemblyQualifiedNameFallback}");
        Console.WriteLine($"  empty registry resolves nothing: {MessageTypeRegistry.Empty.ResolveLogicalName(typeof(OrderConfirmed)) ?? "(null)"}");
    }
}
