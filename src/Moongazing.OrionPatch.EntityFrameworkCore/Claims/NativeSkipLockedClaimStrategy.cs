namespace Moongazing.OrionPatch.EntityFrameworkCore.Claims;

using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Moongazing.OrionPatch.Models;

/// <summary>
/// Provider-native batch claim that issues a real <c>FOR UPDATE SKIP LOCKED</c> (PostgreSQL,
/// MySQL) or <c>WITH (UPDLOCK, READPAST, ROWLOCK)</c> (SQL Server) statement so competing
/// dispatchers never claim the same row and race losers skip locked rows instead of consuming a
/// no-op compare-and-swap round-trip. Selected by <see cref="ProviderClaimStrategy.For"/> for the
/// three native-capable providers; SQLite and unrecognized providers keep the portable
/// <see cref="CompareAndSwapClaimStrategy"/>.
/// </summary>
/// <remarks>
/// <para>
/// <b>Competing-consumers safety.</b> The claim is a single atomic statement on PostgreSQL and SQL
/// Server (lock-skip-update-return in one round-trip), and a three-statement sequence inside one
/// explicit transaction on MySQL (the <c>SELECT ... FOR UPDATE SKIP LOCKED</c> holds the row locks
/// until the enclosing transaction commits, so the subsequent <c>UPDATE</c> and re-select operate on
/// rows no other dispatcher can have claimed). In every dialect a row is handed to exactly one of N
/// concurrent claimers.
/// </para>
/// <para>
/// The statement is executed directly against the bound connection via a <see cref="DbCommand"/> so
/// the <c>RETURNING</c> / <c>OUTPUT</c> result set can be projected straight into
/// <see cref="OutboxRow"/> without EF Core change tracking. When the caller already holds an ambient
/// <see cref="IDbContextTransaction"/> the command enlists in it; otherwise (MySQL only) a private
/// transaction is opened and committed around the three statements.
/// </para>
/// </remarks>
/// <param name="dialect">SQL dialect this strategy targets.</param>
internal sealed class NativeSkipLockedClaimStrategy(SqlDialect dialect) : IClaimStrategy
{
    /// <summary>Cached composite format for the MySQL re-select (CA1863).</summary>
    private static readonly CompositeFormat MySqlReselectFormat = CompositeFormat.Parse(NativeClaimSql.MySqlReselect);

    /// <summary>Dialect this strategy was constructed with.</summary>
    internal SqlDialect Dialect => dialect;

    /// <inheritdoc/>
    public async Task<IReadOnlyList<OutboxRow>> ClaimNextAsync(
        DbContext db,
        int batchSize,
        string dispatcherIdentity,
        TimeSpan leaseDuration,
        DateTime utcNow,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(db);
        ArgumentException.ThrowIfNullOrEmpty(dispatcherIdentity);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        var leaseExpiry = utcNow - leaseDuration;

        // EF Core opens/owns the connection; reuse it so the claim runs on the same connection (and,
        // when present, the same ambient transaction) as the rest of the unit of work.
        var connection = db.Database.GetDbConnection();
        var ambient = db.Database.CurrentTransaction;
        if (connection.State != ConnectionState.Open)
        {
            await db.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }

        return dialect switch
        {
            SqlDialect.PostgreSql => await ClaimSingleStatementAsync(
                connection, ambient, NativeClaimSql.PostgreSql, batchSize, dispatcherIdentity, utcNow, leaseExpiry, cancellationToken).ConfigureAwait(false),
            SqlDialect.SqlServer => await ClaimSingleStatementAsync(
                connection, ambient, NativeClaimSql.SqlServer, batchSize, dispatcherIdentity, utcNow, leaseExpiry, cancellationToken).ConfigureAwait(false),
            SqlDialect.MySql => await ClaimMySqlAsync(
                db, connection, ambient, batchSize, dispatcherIdentity, utcNow, leaseExpiry, cancellationToken).ConfigureAwait(false),
            _ => throw new InvalidOperationException($"Unsupported dialect {dialect} for native claim."),
        };
    }

    /// <summary>
    /// PostgreSQL / SQL Server path: one statement that locks, claims, and returns the rows.
    /// </summary>
    private static async Task<IReadOnlyList<OutboxRow>> ClaimSingleStatementAsync(
        DbConnection connection,
        IDbContextTransaction? ambient,
        string sql,
        int batchSize,
        string dispatcherIdentity,
        DateTime utcNow,
        DateTime leaseExpiry,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (ambient?.GetDbTransaction() is { } tx)
        {
            command.Transaction = tx;
        }

        AddClaimParameters(command, batchSize, dispatcherIdentity, utcNow, leaseExpiry);

        var rows = new List<OutboxRow>(batchSize);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            rows.Add(ReadRow(reader));
        }

        // RETURNING / OUTPUT do not guarantee row order, so re-impose FIFO by enqueue time to match
        // the portable strategy's observable ordering.
        rows.Sort(static (a, b) => a.EnqueuedAtUtc.CompareTo(b.EnqueuedAtUtc));
        return rows;
    }

    /// <summary>
    /// MySQL path: <c>SELECT ... FOR UPDATE SKIP LOCKED</c> to take the locks, <c>UPDATE</c> to claim
    /// the locked ids, then a re-select to read the claimed rows back. All three run inside one
    /// transaction (the caller's ambient one if present, otherwise a private one committed here) so
    /// the row locks the select takes survive through the update.
    /// </summary>
    private static async Task<IReadOnlyList<OutboxRow>> ClaimMySqlAsync(
        DbContext db,
        DbConnection connection,
        IDbContextTransaction? ambient,
        int batchSize,
        string dispatcherIdentity,
        DateTime utcNow,
        DateTime leaseExpiry,
        CancellationToken cancellationToken)
    {
        var ownsTransaction = ambient is null;
        IDbContextTransaction? owned = ownsTransaction
            ? await db.Database.BeginTransactionAsync(cancellationToken).ConfigureAwait(false)
            : null;
        var tx = (ambient ?? owned)!.GetDbTransaction();
        try
        {
            // Step 1: lock the eligible ids.
            var ids = new List<Guid>(batchSize);
            await using (var select = connection.CreateCommand())
            {
                select.CommandText = NativeClaimSql.MySqlSelectForUpdate;
                select.Transaction = tx;
                AddClaimParameters(select, batchSize, dispatcherIdentity, utcNow, leaseExpiry);
                await using var reader = await select.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    ids.Add(ReadGuid(reader, 0));
                }
            }

            if (ids.Count == 0)
            {
                if (ownsTransaction && owned is not null)
                {
                    await owned.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
                return Array.Empty<OutboxRow>();
            }

            // Step 2: claim exactly the locked ids.
            await using (var update = connection.CreateCommand())
            {
                var idParams = BuildIdParameterList(update, ids);
                update.CommandText =
                    $"UPDATE `OrionPatch_Outbox` SET `Status` = @claimed, `ClaimedAtUtc` = @now, `ClaimedBy` = @dispatcher WHERE `Id` IN ({idParams});";
                update.Transaction = tx;
                AddParameter(update, "@claimed", NativeClaimSql.ClaimedOrdinal);
                AddParameter(update, "@now", utcNow);
                AddParameter(update, "@dispatcher", dispatcherIdentity);
                await update.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            // Step 3: read the claimed rows back.
            var rows = new List<OutboxRow>(ids.Count);
            await using (var reselect = connection.CreateCommand())
            {
                var idParams = BuildIdParameterList(reselect, ids);
                reselect.CommandText = string.Format(CultureInfo.InvariantCulture, MySqlReselectFormat, idParams);
                reselect.Transaction = tx;
                await using var reader = await reselect.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    rows.Add(ReadRow(reader));
                }
            }

            if (ownsTransaction && owned is not null)
            {
                await owned.CommitAsync(cancellationToken).ConfigureAwait(false);
            }

            return rows;
        }
        finally
        {
            if (owned is not null)
            {
                await owned.DisposeAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Bind the shared claim parameters used by every dialect's statement.</summary>
    private static void AddClaimParameters(DbCommand command, int batchSize, string dispatcherIdentity, DateTime utcNow, DateTime leaseExpiry)
    {
        AddParameter(command, "@batchSize", batchSize);
        AddParameter(command, "@dispatcher", dispatcherIdentity);
        AddParameter(command, "@now", utcNow);
        AddParameter(command, "@leaseExpiry", leaseExpiry);
        AddParameter(command, "@pending", NativeClaimSql.PendingOrdinal);
        AddParameter(command, "@claimedState", NativeClaimSql.ClaimedOrdinal);
        AddParameter(command, "@claimed", NativeClaimSql.ClaimedOrdinal);
    }

    /// <summary>Append one bound parameter per id and return the comma-joined placeholder list.</summary>
    private static string BuildIdParameterList(DbCommand command, List<Guid> ids)
    {
        var names = new string[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            var name = "@id" + i.ToString(CultureInfo.InvariantCulture);
            names[i] = name;
            AddParameter(command, name, ids[i]);
        }

        return string.Join(", ", names);
    }

    private static void AddParameter(DbCommand command, string name, object value)
    {
        var p = command.CreateParameter();
        p.ParameterName = name;
        p.Value = value;
        command.Parameters.Add(p);
    }

    /// <summary>Project the current reader row (all 14 outbox columns, in declared order) into a row.</summary>
    private static OutboxRow ReadRow(DbDataReader reader)
    {
        return new OutboxRow
        {
            Id = ReadGuid(reader, 0),
            MessageType = reader.GetString(1),
            Payload = reader.GetString(2),
            HeadersJson = reader.IsDBNull(3) ? null : reader.GetString(3),
            CorrelationId = reader.IsDBNull(4) ? null : reader.GetString(4),
            OccurredAtUtc = ReadUtc(reader, 5),
            EnqueuedAtUtc = ReadUtc(reader, 6),
            Status = (OutboxStatus)Convert.ToByte(reader.GetValue(7), CultureInfo.InvariantCulture),
            AttemptCount = Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture),
            ClaimedAtUtc = reader.IsDBNull(9) ? null : ReadUtc(reader, 9),
            ClaimedBy = reader.IsDBNull(10) ? null : reader.GetString(10),
            LastError = reader.IsDBNull(11) ? null : reader.GetString(11),
            ProcessedAtUtc = reader.IsDBNull(12) ? null : ReadUtc(reader, 12),
            NextAttemptAtUtc = reader.IsDBNull(13) ? null : ReadUtc(reader, 13),
        };
    }

    /// <summary>
    /// Read a Guid column. PostgreSQL (<c>uuid</c>) and SQL Server (<c>uniqueidentifier</c>) return a
    /// native <see cref="Guid"/>; MySQL stores it as <c>char(36)</c> and may surface it as a string,
    /// so fall back to parsing.
    /// </summary>
    private static Guid ReadGuid(DbDataReader reader, int ordinal)
    {
        var value = reader.GetValue(ordinal);
        return value switch
        {
            Guid g => g,
            string s => Guid.Parse(s, CultureInfo.InvariantCulture),
            byte[] b => new Guid(b),
            _ => reader.GetGuid(ordinal),
        };
    }

    /// <summary>
    /// Read a UTC <see cref="DateTime"/>. Providers may hand back a <see cref="DateTime"/> with
    /// <see cref="DateTimeKind.Unspecified"/> (the column carries no zone); the storage convention is
    /// that these columns are UTC, so the kind is normalized back to <see cref="DateTimeKind.Utc"/>.
    /// </summary>
    private static DateTime ReadUtc(DbDataReader reader, int ordinal)
    {
        var value = reader.GetDateTime(ordinal);
        return value.Kind == DateTimeKind.Utc ? value : DateTime.SpecifyKind(value, DateTimeKind.Utc);
    }
}
