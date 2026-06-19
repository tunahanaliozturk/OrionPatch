using Moongazing.OrionPatch.Demo;

Console.WriteLine("OrionPatch - Transactional Outbox Demo");
Console.WriteLine(new string('=', 60));
Console.WriteLine("In-memory only. No RabbitMQ / Kafka / Azure Service Bus / SQL required.");

await EnvelopeDispatchDemo.RunAsync();
await RetryBackoffDemo.RunAsync();
await DeadLetterDemo.RunAsync();
await DeadLetterStoreAndArchivalDemo.RunAsync();
MessageTypeRegistryDemo.Run();
await RowLifecycleDemo.RunAsync();
await ChannelSinkDemo.RunAsync();

Console.WriteLine();
Console.WriteLine(new string('=', 60));
Console.WriteLine("Summary");
Console.WriteLine(new string('=', 60));
Console.WriteLine("Demos covered: envelope dispatch (enqueue -> claim -> send -> complete),");
Console.WriteLine("retry with exponential backoff on a flaky sink, dead-lettering after");
Console.WriteLine("MaxAttempts, the v0.3 dead-letter store and archival maintenance APIs,");
Console.WriteLine("the message-type registry (logical name <-> CLR type with a");
Console.WriteLine("versioned rename), the outbox row lifecycle driven against the storage SPI,");
Console.WriteLine("and the in-process ChannelOutboxSink fan-out.");
Console.WriteLine();
Console.WriteLine("Every scenario runs in-memory via Moongazing.OrionPatch.Testing");
Console.WriteLine("(InMemoryOutbox / InMemoryOutboxStorage / DeterministicDispatcher / TestClock),");
Console.WriteLine("so the at-least-once contract is observable without any broker or database.");
