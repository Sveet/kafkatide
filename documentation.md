# Usage
## Producing messages
```typescript
import KafkaTide from 'kafkatide';

const { sendSubject, event$, error$, disconnectSubject } = new KafkaTide().produce('my-topic');

sendSubject.next({ /* Kafka message */ });

event$.subscribe(e => console.log(e)); // KafkaJS producer events
error$.subscribe(e => console.log(e)); // KafkaJS producer errors
disconnectSubject.next(); // Disconnect the producer
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
* `disconnectSubject`: RxJS Subject to disconnect the producer

Accepts an optional `producerConfig` for the KafkaJS producer.

## `.consume({ config, topic, partition, offset })`
Creates a consumer for the given `topic` and `config`. Optionally seeks to partition and offset. Returns an object with:

* `message$`: Observable of consumed Kafka messages
* `event$`: Observable of KafkaJS consumer events