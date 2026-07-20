# Stream connectors

A connector is a streaming input for a pipeline: a gRPC server stream or a
Kafka topic, opened once and then pushed to a listener until it ends, fails,
or is closed. The Kafka Connect runtime (`protomolt-connect`) and a pipeline
executor both build on this SPI.

## The SPI

`StreamSource` is the contract, typed by its plan:

```java
Handle handle = source.open(plan, listener);
```

- The plan is a plain record per source kind: `GrpcSourcePlan` (channel,
  server-streaming method, request) or `KafkaSourcePlan` (bootstrap servers,
  topic, consumer group, payload parser, raw consumer-property overrides).
- The listener receives `onMessage` calls in order from one source thread,
  then exactly one terminal signal: `onComplete` or `onError`.
- The handle is the only flow control: `pause()`, `resume()`, `close()`.
  Pause is not a wall; messages already in flight still arrive.

There is no cursor and no pull method. Offset and resume-token ownership stays
in the deployment layer (Kafka Connect owns offsets; the Connect source owns
CEL resume tokens).

## The pump

`SourcePump` bridges the push SPI to a synchronous consumer. The source fills
a bounded queue (default 64); the consumer pulls one message at a time with
`take(timeout)`:

- When the queue fills, the pump pauses the source, then blocks the producer
  thread until there is room.
- When the consumer drains the queue below half, the pump resumes the source.
- A failure surfaces after the messages buffered before it: `take` returns
  them first, then throws `SourceException`.
- `close()` stops the source and frees a blocked producer; queued messages
  remain drainable.

This is how a push source feeds a synchronous pipeline with bounded memory
and without dropping records.

## gRPC server streams

`GrpcStreamSource` opens any server-streaming method from its descriptor via
`protomolt-grpc-invoke` and pumps it in batches. Pausing stops pulling, and
the transport's flow control stops the server from sending; closing cancels
the call, which the server observes.

```java
SourcePump pump = new SourcePump();
StreamSource.Handle handle = new GrpcStreamSource().open(
        new GrpcSourcePlan(channel, method, request), pump);
pump.attach(handle);
Message first = pump.take(Duration.ofSeconds(5));
```

## Kafka topics

`KafkaSource` subscribes one poll thread and pushes each record through the
plan's `MessageParser` (`MessageParser.forType(descriptor)` or
`MessageParser.bytes()`). Pausing pauses the assigned partitions while polls
continue, so group membership survives a stall. Defaults are a live tap:
auto-commit on, newest offsets. `KafkaSourcePlan.overrides()` carries any raw
consumer property.

One caveat: a full pump blocks the poll thread, and a stall longer than
`max.poll.interval.ms` drops the consumer from its group; it rejoins and
resumes from the last committed offsets. For managed offset ownership and
rebalance-safe delivery, use the Connect source instead.

## Testing

Unit tests run without infrastructure. `KafkaSourceLiveIntegrationTest` starts
its own Apache Kafka broker as a Testcontainer and skips when Docker is
unavailable.
