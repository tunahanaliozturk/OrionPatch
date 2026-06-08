namespace Moongazing.OrionPatch.EntityFrameworkCore.Tests.Abstractions;

using System.Text.Json;
using Microsoft.EntityFrameworkCore;
using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Configuration;
using Moongazing.OrionPatch.EntityFrameworkCore;
using Moongazing.OrionPatch.Internal;
using Moongazing.OrionPatch.Models;
using Xunit;

public sealed class TenantResolverIntegrationTests
{
    private sealed class TestDbContext(DbContextOptions<TestDbContext> options) : DbContext(options) { }

    public sealed class OrderShipped { public int Id { get; init; } }

    private static (EfCoreOutbox outbox, MessageSerializer serializer) NewOutbox(
        IOutboxTenantResolver? resolver = null)
    {
        var options = new DbContextOptionsBuilder<TestDbContext>()
            .UseSqlite("DataSource=:memory:")
            .Options;
        var db = new TestDbContext(options);
        var serializer = new MessageSerializer(new JsonSerializerOptions(JsonSerializerDefaults.Web));
        var typeResolver = new MessageTypeNameResolver(MessageTypeRegistry.Empty);
        return (new EfCoreOutbox(db, typeResolver, serializer, resolver), serializer);
    }

    private static Dictionary<string, string>? Headers(EfCoreOutbox outbox)
    {
        // The buffer holds the row before SaveChanges flushes; HeadersJson is the persisted shape.
        var row = outbox.Buffer.Single();
        if (string.IsNullOrEmpty(row.HeadersJson))
        {
            return null;
        }
        return JsonSerializer.Deserialize<Dictionary<string, string>>(row.HeadersJson);
    }

    [Fact]
    public void Default_null_resolver_does_not_stamp_tenant_header()
    {
        var (outbox, _) = NewOutbox();
        outbox.Enqueue(new OrderShipped { Id = 1 });

        Assert.Null(Headers(outbox));
    }

    [Fact]
    public void Active_resolver_stamps_tenant_header_when_no_caller_override()
    {
        var (outbox, _) = NewOutbox(new DelegateOutboxTenantResolver(() => "tenant-A"));
        outbox.Enqueue(new OrderShipped { Id = 1 });

        var headers = Headers(outbox);
        Assert.NotNull(headers);
        Assert.Equal("tenant-A", headers![IOutboxTenantResolver.TenantHeaderName]);
    }

    [Fact]
    public void Resolver_does_not_overwrite_caller_supplied_tenant_header()
    {
        var (outbox, _) = NewOutbox(new DelegateOutboxTenantResolver(() => "ambient-tenant"));
        outbox.Enqueue(
            new OrderShipped { Id = 1 },
            new OutboxEnqueueOptions
            {
                Headers = new Dictionary<string, string>
                {
                    [IOutboxTenantResolver.TenantHeaderName] = "explicit-tenant",
                },
            });

        var headers = Headers(outbox);
        Assert.NotNull(headers);
        Assert.Equal("explicit-tenant", headers![IOutboxTenantResolver.TenantHeaderName]);
    }

    [Fact]
    public void Resolver_returning_null_does_not_stamp_header()
    {
        var (outbox, _) = NewOutbox(new DelegateOutboxTenantResolver(() => null));
        outbox.Enqueue(new OrderShipped { Id = 1 });

        Assert.Null(Headers(outbox));
    }

    [Fact]
    public void Resolver_returning_empty_string_does_not_stamp_header()
    {
        var (outbox, _) = NewOutbox(new DelegateOutboxTenantResolver(() => string.Empty));
        outbox.Enqueue(new OrderShipped { Id = 1 });

        Assert.Null(Headers(outbox));
    }

    [Fact]
    public void Resolver_merges_with_other_caller_headers_when_tenant_key_absent()
    {
        var (outbox, _) = NewOutbox(new DelegateOutboxTenantResolver(() => "tenant-B"));
        outbox.Enqueue(
            new OrderShipped { Id = 1 },
            new OutboxEnqueueOptions
            {
                Headers = new Dictionary<string, string>
                {
                    ["x-source"] = "import-job",
                },
            });

        var headers = Headers(outbox);
        Assert.NotNull(headers);
        Assert.Equal("tenant-B", headers![IOutboxTenantResolver.TenantHeaderName]);
        Assert.Equal("import-job", headers["x-source"]);
    }

    [Fact]
    public void Resolver_is_called_per_enqueue_so_ambient_changes_take_effect()
    {
        var current = "tenant-X";
        var (outbox, _) = NewOutbox(new DelegateOutboxTenantResolver(() => current));

        outbox.Enqueue(new OrderShipped { Id = 1 });
        current = "tenant-Y";
        outbox.Enqueue(new OrderShipped { Id = 2 });

        var rows = outbox.Buffer.ToList();
        Assert.Equal(2, rows.Count);
        var headersFirst = JsonSerializer.Deserialize<Dictionary<string, string>>(rows[0].HeadersJson!);
        var headersSecond = JsonSerializer.Deserialize<Dictionary<string, string>>(rows[1].HeadersJson!);
        Assert.Equal("tenant-X", headersFirst![IOutboxTenantResolver.TenantHeaderName]);
        Assert.Equal("tenant-Y", headersSecond![IOutboxTenantResolver.TenantHeaderName]);
    }
}
