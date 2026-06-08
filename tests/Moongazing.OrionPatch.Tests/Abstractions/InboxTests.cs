namespace Moongazing.OrionPatch.Tests.Abstractions;

using Moongazing.OrionPatch.Abstractions;
using Moongazing.OrionPatch.Channels;
using Moongazing.OrionPatch.Models;
using Xunit;

public sealed class InboxTests
{
    private static OutboxEnvelope MakeEnvelope(Guid id) => new(
        Id: id,
        MessageType: "test.message",
        Payload: "{}",
        Headers: null,
        CorrelationId: null,
        OccurredAtUtc: DateTime.UtcNow,
        AttemptNumber: 1);

    [Fact]
    public async Task InMemoryInbox_first_TryAccept_returns_true()
    {
        var inbox = new InMemoryInbox();

        var accepted = await inbox.TryAcceptAsync(Guid.NewGuid(), CancellationToken.None);

        Assert.True(accepted);
        Assert.Equal(1, inbox.Count);
    }

    [Fact]
    public async Task InMemoryInbox_duplicate_TryAccept_returns_false()
    {
        var inbox = new InMemoryInbox();
        var id = Guid.NewGuid();

        var first = await inbox.TryAcceptAsync(id, CancellationToken.None);
        var second = await inbox.TryAcceptAsync(id, CancellationToken.None);

        Assert.True(first);
        Assert.False(second);
        Assert.Equal(1, inbox.Count);
    }

    [Fact]
    public async Task InMemoryInbox_different_ids_each_first_delivery()
    {
        var inbox = new InMemoryInbox();

        var a = await inbox.TryAcceptAsync(Guid.NewGuid(), CancellationToken.None);
        var b = await inbox.TryAcceptAsync(Guid.NewGuid(), CancellationToken.None);

        Assert.True(a);
        Assert.True(b);
        Assert.Equal(2, inbox.Count);
    }

    [Fact]
    public async Task InMemoryInbox_concurrent_accepts_for_same_id_exactly_one_wins()
    {
        var inbox = new InMemoryInbox();
        var id = Guid.NewGuid();

        var tasks = Enumerable.Range(0, 64).Select(_ =>
            inbox.TryAcceptAsync(id, CancellationToken.None).AsTask()).ToArray();

        await Task.WhenAll(tasks);

        Assert.Equal(1, tasks.Count(t => t.Result));
        Assert.Equal(1, inbox.Count);
    }

    [Fact]
    public async Task InMemoryInbox_Reset_clears_state()
    {
        var inbox = new InMemoryInbox();
        var id = Guid.NewGuid();

        await inbox.TryAcceptAsync(id, CancellationToken.None);
        inbox.Reset();
        var redelivered = await inbox.TryAcceptAsync(id, CancellationToken.None);

        Assert.True(redelivered);
    }

    [Fact]
    public async Task InboxFilter_runs_handler_on_first_delivery()
    {
        var inbox = new InMemoryInbox();
        var filter = new InboxFilter(inbox);
        var handlerInvocations = 0;

        var ran = await filter.InvokeIfFirstAsync(
            MakeEnvelope(Guid.NewGuid()),
            (_, _) => { handlerInvocations++; return ValueTask.CompletedTask; },
            CancellationToken.None);

        Assert.True(ran);
        Assert.Equal(1, handlerInvocations);
    }

    [Fact]
    public async Task InboxFilter_skips_handler_on_duplicate()
    {
        var inbox = new InMemoryInbox();
        var filter = new InboxFilter(inbox);
        var handlerInvocations = 0;
        var envelope = MakeEnvelope(Guid.NewGuid());

        await filter.InvokeIfFirstAsync(envelope,
            (_, _) => { handlerInvocations++; return ValueTask.CompletedTask; },
            CancellationToken.None);
        var ranSecond = await filter.InvokeIfFirstAsync(envelope,
            (_, _) => { handlerInvocations++; return ValueTask.CompletedTask; },
            CancellationToken.None);

        Assert.False(ranSecond);
        Assert.Equal(1, handlerInvocations);
    }

    [Fact]
    public async Task InboxFilter_propagates_handler_exception_but_keeps_dedup_state()
    {
        var inbox = new InMemoryInbox();
        var filter = new InboxFilter(inbox);
        var envelope = MakeEnvelope(Guid.NewGuid());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            filter.InvokeIfFirstAsync(envelope,
                (_, _) => throw new InvalidOperationException("boom"),
                CancellationToken.None).AsTask());

        // The dedup row was claimed before the handler ran, so the retry sees a duplicate.
        // Consumers who want at-least-once behaviour wire the inbox to commit only after the
        // handler succeeds; v0.2.2 documents the at-most-once contract on the interface.
        Assert.Equal(1, inbox.Count);
    }
}
