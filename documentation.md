# Usage
## Producing messages
```typescript
import KafkaTide from 'kafkatide';

const { sendSubject, event$, error$, disconnect } = new KafkaTide().produce('my-topic');

sendSubject.next({ /* Kafka message */ });

event$.subscribe(e => console.log(e)); // KafkaJS producer events
error$.subscribe(e => console.log(e)); // KafkaJS producer errors
disconnect(); // Disconnect the producer
```

## Consuming messages
```typescript
const { message$, event$ } = new KafkaTide().consume({ 
  topic: 'my-topic', 
  config: { groupId: 'my-group' } 
});

message$.subscribe(m => {
  // Use `m.workComplete` to signal that processing of the message is done.
  // This will commit the offset.
});

event$.subscribe(e => console.log(e)); // KafkaJS consumer events
```
# API
## `KafkaTide(config)`
Creates a new KafkaTide instance. Accepts an optional `kafkaConfig` for the underlying KafkaJS client.

## `.produce(topic, producerConfig?)`
Creates a producer for the given topic. Returns an object with:

* `sendSubject`: RxJS Subject to send messages
* `event$`: Observable of KafkaJS producer events
* `error$`: Observable of KafkaJS producer errors
* `disconnect`: RxJS Subject to disconnect the producer

Accepts an optional `producerConfig` for the KafkaJS producer.

## `.consume({ config, topic, partition, offset })`
Creates a consumer for the given `topic` and `config`. Optionally seeks to partition and offset. Returns an object with:

* `message$`: Observable of consumed Kafka messages
* `event$`: Observable of KafkaJS consumer events

## Types and Interfaces

### ProducerConfig

Optionally supply a KafkaJS `ProducerConfig`. See the [KafkaJS documentation](https://kafka.js.org/docs/producing) for more information.

- `createPartitioner` (optional): Custom partitioner.
- `retry` (optional): Retry options.
- `metadataMaxAge` (optional): Maximum age of metadata.
- `allowAutoTopicCreation` (optional): Flag to allow auto topic creation.
- `idempotent` (optional): Flag for idempotent operation.
- `transactionalId` (optional): Transactional ID.
- `transactionTimeout` (optional): Transaction timeout.
- `maxInFlightRequests` (optional): Maximum in-flight requests.

### ConsumeParams

`ConsumeParams` is a type that represents the parameters for the consume method of KafkaTide.

- `config`: KafkaJS consumer configuration object.
- `topic`: Kafka topic to consume from.
- `partition` (optional): Partition to consume from.
- `offset` (optional): Offset to start consuming from.
- `recoverOffsets` (optional): Flag to recover offsets.
- `runConfig` (optional): Configuration to pass to consumer.run.

### ConsumerConfig

`ConsumerConfig` is an interface that represents the configuration for a KafkaJS consumer. See the [KafkaJS Documentation](https://kafka.js.org/docs/consuming#a-name-options-a-options) for more information.

- `groupId`: The group ID of the consumer.
- `partitionAssigners` (optional): Array of partition assigners.
- `metadataMaxAge` (optional): Maximum age of metadata.
- `sessionTimeout` (optional): Session timeout.
- `rebalanceTimeout` (optional): Rebalance timeout.
- `heartbeatInterval` (optional): Heartbeat interval.
- `maxBytesPerPartition` (optional): Maximum bytes per partition.
- `minBytes` (optional): Minimum bytes per batch.
- `maxBytes` (optional): Maximum bytes per batch. KafkaTide Default: `2048` (2KB)
- `maxWaitTimeInMs` (optional): Maximum wait time in milliseconds.
- `retry` (optional): Retry options and a function to determine whether to restart on failure.
- `allowAutoTopicCreation` (optional): Flag to allow auto topic creation. KafkaTide Default: `false`
- `maxInFlightRequests` (optional): Maximum in-flight requests. This appears to be unused by the KafkaJS consumer.
- `readUncommitted` (optional): Flag to read uncommitted.
- `rackId` (optional): Rack ID.

### RunConfig

`RunConfig` is a type that represents a subset of `ConsumerRunConfig`. See the [KafkaJS Documentation](https://kafka.js.org/docs/consuming#a-name-auto-commit-a-autocommit) for more information.

- `autoCommit`
- `autoCommitInterval`
- `autoCommitThreshold`
- `partitionsConsumedConcurrently`
