namespace Moongazing.OrionPatch.EntityFrameworkCore.Claims;

/// <summary>
/// Per-dialect SQL text for the native <c>SKIP LOCKED</c>-style batch claim. Each statement
/// atomically selects up to <c>@batchSize</c> eligible rows under a row lock that competing
/// dispatchers skip rather than block on, flips them to <see cref="Moongazing.OrionPatch.Models.OutboxStatus.Claimed"/>,
/// and returns every column of the claimed rows so the caller can rebuild
/// <see cref="Moongazing.OrionPatch.Models.OutboxRow"/> without a second round-trip.
/// </summary>
/// <remarks>
/// <para>
/// Eligibility mirrors the portable <see cref="CompareAndSwapClaimStrategy"/> exactly: a row is
/// claimable when it is <c>Pending</c> and due (<c>NextAttemptAtUtc</c> null or already past), or
/// when it is <c>Claimed</c> but its lease has expired (<c>ClaimedAtUtc &lt; @leaseExpiry</c>).
/// Ordering is FIFO by <c>EnqueuedAtUtc</c>. Status is stored as its underlying <see cref="byte"/>
/// ordinal (<c>Pending = 0</c>, <c>Claimed = 1</c>), so the SQL compares against the literal
/// ordinals to stay independent of any value converter.
/// </para>
/// <para>
/// Column and table identifiers are the EF Core defaults from
/// <see cref="Configuration.OutboxEntityConfiguration"/> (property name == column name, table
/// <c>OrionPatch_Outbox</c>). Quoting is dialect-correct: double quotes for PostgreSQL, backticks
/// for MySQL, square brackets for SQL Server.
/// </para>
/// <para>
/// All values are passed as parameters (never interpolated), so these statements are not an
/// injection surface even though they are raw SQL.
/// </para>
/// </remarks>
internal static class NativeClaimSql
{
    /// <summary>Ordinal of <c>OutboxStatus.Pending</c> as stored.</summary>
    internal const int PendingOrdinal = 0;

    /// <summary>Ordinal of <c>OutboxStatus.Claimed</c> as stored.</summary>
    internal const int ClaimedOrdinal = 1;

    /// <summary>
    /// PostgreSQL 9.5+. Single statement: a <c>FOR UPDATE SKIP LOCKED</c> sub-select picks the
    /// eligible ids under a row lock competing dispatchers skip, the outer <c>UPDATE</c> claims them,
    /// and <c>RETURNING</c> hands back the claimed rows. Atomic in one round-trip; no explicit
    /// transaction required because the sub-select's locks live for the statement.
    /// </summary>
    internal const string PostgreSql = """
        UPDATE "OrionPatch_Outbox" AS o
        SET "Status" = @claimed, "ClaimedAtUtc" = @now, "ClaimedBy" = @dispatcher
        WHERE o."Id" IN (
            SELECT c."Id" FROM "OrionPatch_Outbox" AS c
            WHERE (c."Status" = @pending AND (c."NextAttemptAtUtc" IS NULL OR c."NextAttemptAtUtc" <= @now))
               OR (c."Status" = @claimedState AND c."ClaimedAtUtc" IS NOT NULL AND c."ClaimedAtUtc" < @leaseExpiry)
            ORDER BY c."EnqueuedAtUtc"
            LIMIT @batchSize
            FOR UPDATE SKIP LOCKED
        )
        RETURNING o."Id", o."MessageType", o."Payload", o."HeadersJson", o."CorrelationId",
                  o."OccurredAtUtc", o."EnqueuedAtUtc", o."Status", o."AttemptCount",
                  o."ClaimedAtUtc", o."ClaimedBy", o."LastError", o."ProcessedAtUtc", o."NextAttemptAtUtc";
        """;

    /// <summary>
    /// SQL Server 2008+. Single statement: a CTE locks the top eligible rows with
    /// <c>UPDLOCK, READPAST, ROWLOCK</c> (acquire update locks, skip rows another dispatcher already
    /// locked) in FIFO order, the <c>UPDATE</c> claims the CTE, and <c>OUTPUT inserted.*</c> returns
    /// the claimed rows. Atomic in one round-trip.
    /// </summary>
    internal const string SqlServer = """
        WITH eligible AS (
            SELECT TOP (@batchSize) *
            FROM [OrionPatch_Outbox] WITH (UPDLOCK, READPAST, ROWLOCK)
            WHERE ([Status] = @pending AND ([NextAttemptAtUtc] IS NULL OR [NextAttemptAtUtc] <= @now))
               OR ([Status] = @claimedState AND [ClaimedAtUtc] IS NOT NULL AND [ClaimedAtUtc] < @leaseExpiry)
            ORDER BY [EnqueuedAtUtc]
        )
        UPDATE eligible
        SET [Status] = @claimed, [ClaimedAtUtc] = @now, [ClaimedBy] = @dispatcher
        OUTPUT inserted.[Id], inserted.[MessageType], inserted.[Payload], inserted.[HeadersJson],
               inserted.[CorrelationId], inserted.[OccurredAtUtc], inserted.[EnqueuedAtUtc],
               inserted.[Status], inserted.[AttemptCount], inserted.[ClaimedAtUtc], inserted.[ClaimedBy],
               inserted.[LastError], inserted.[ProcessedAtUtc], inserted.[NextAttemptAtUtc];
        """;

    /// <summary>
    /// MySQL 8.0+ / MariaDB 10.6+ candidate select. MySQL has no <c>RETURNING</c> on <c>UPDATE</c>,
    /// so the claim is a three-statement sequence under one explicit transaction: this
    /// <c>SELECT ... FOR UPDATE SKIP LOCKED</c> takes the row locks, an <c>UPDATE ... WHERE Id IN (...)</c>
    /// claims exactly the locked ids, then <see cref="MySqlReselect"/> reads the claimed rows back.
    /// The locks taken here are held until the transaction commits, so a competing dispatcher's
    /// concurrent select skips these rows.
    /// </summary>
    internal const string MySqlSelectForUpdate = """
        SELECT `Id` FROM `OrionPatch_Outbox`
        WHERE (`Status` = @pending AND (`NextAttemptAtUtc` IS NULL OR `NextAttemptAtUtc` <= @now))
           OR (`Status` = @claimedState AND `ClaimedAtUtc` IS NOT NULL AND `ClaimedAtUtc` < @leaseExpiry)
        ORDER BY `EnqueuedAtUtc`
        LIMIT @batchSize
        FOR UPDATE SKIP LOCKED;
        """;

    /// <summary>
    /// MySQL re-select reading every column of the just-claimed rows back in FIFO order. The
    /// id-list predicate is appended by the caller as an <c>IN (...)</c> of bound parameters.
    /// </summary>
    internal const string MySqlReselect = """
        SELECT `Id`, `MessageType`, `Payload`, `HeadersJson`, `CorrelationId`, `OccurredAtUtc`,
               `EnqueuedAtUtc`, `Status`, `AttemptCount`, `ClaimedAtUtc`, `ClaimedBy`, `LastError`,
               `ProcessedAtUtc`, `NextAttemptAtUtc`
        FROM `OrionPatch_Outbox`
        WHERE `Id` IN ({0})
        ORDER BY `EnqueuedAtUtc`;
        """;
}
