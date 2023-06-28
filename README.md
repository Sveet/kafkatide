[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md)
[![Version](https://img.shields.io/npm/v/kafkatide.svg)](https://npmjs.org/package/kafkatide)
[![Downloads/week](https://img.shields.io/npm/dw/kafkatide.svg)](https://npmjs.org/package/kafkatide)

# KafkaTide

KafkaTide is a lightweight wrapper around the KafkaJS library which provides a RxJS interface for producing and consuming Kafka messages.

The goal of this project is to give the user full control of the asynchronous behavior of their code.

The underlying KafkaJS configs are exposed for maximum control, while smart defaults are chosen to accommodate average use cases.

## Installation

```bash
npm install kafkatide
```
    
## Documentation
[Full API Documenation for KafkaTide](documentation.md)


## Usage/Examples

### Initialize Kafka Connection
The KafkaTide constructor is identical to KafkaJS constructor. [KafkaJS Documentation](https://kafka.js.org/docs/configuration)
```typescript
import KafkaTide from 'kafkatide';
const { consume, produce } = new KafkaTide({
  brokers: ['broker-1'],
  clientId: 'kafkatide-example',
})
```

### Produce Messages

Produce messages by supplying the topic. Optionally supply a KafkaJS ProducerConfig as a second parameter. See the [KafkaJS documentation](https://kafka.js.org/docs/producing) for more information.

```typescript
const { sendSubject, disconnect } = kafkaTide.produce('my-topic');

// Send a message
sendSubject.next({
  value: 'Hello, world!',
});

// Disconnect when done
disconnect();
```

### Consume Messages

Consume messages by supplying KafkaJS consumer config and the topic. The consumer config minimally needs a groupId. See the [KafkaJS Documentation](https://kafka.js.org/docs/consuming#a-name-options-a-options) for more information.

See the [KafkaTide API docs](documentation.md#consumeparams) for all consume options.

```typescript
const topic = 'com.kafkatide.example'
const config = {
  groupId: 'kafkatide'
}
const { message$ } = kafkaTide.consume({ config, topic });

// Handle incoming messages
message$.subscribe(({value, workComplete}) => {
  console.log(value);
  workComplete();
});
```

### Advanced Usage

In this example, we will consume messages from one Kafka topic, modify the messages, and then produce the modified messages to another Kafka topic.

```typescript
import KafkaTide from 'kafkatide';

const { consume, produce } = new KafkaTide({
  brokers: ['broker-1'],
  clientId: 'kafkatide-example',
})

const config = {
  groupId: 'kafkatide'
}

// Consume messages from 'input-topic'
const { message$ } = consume({ config, topic: 'input-topic' });

// Produce messages to 'output-topic'
const { sendSubject, disconnect } = produce('output-topic');

// Handle incoming messages
message$
  .pipe(
    // consume messages until the value is 'stop'
    takeWhile(m => m.value != 'stop')
  )
  .subscribe({
    next: (message) => {
      console.log(`Received message: ${message.value}`);

      // Modify the message
      const modifiedMessage = modifyMessage(message.value);

      // Send the modified message to 'output-topic'
      sendSubject.next({
        headers: message.headers,
        value: modifiedMessage,
      });

      // Mark the work as complete
      message.workComplete();
    },
    complete: () => {
      // disconnect the producer after consuming is complete
      disconnect();
    }
  });
```

### Disconnecting the Consumer
The consumer is automatically stopped and disconnected when the Observable's subscription has been ended. Each of the following examples results in a disconnected consumer.

Unsubscribing from the subscription
```typescript
const subscription = message$.subscribe(m => console.log(m.value))

// unsubscribe after 10 seconds
setTimeout(() => subscription.unsubscribe(), 10000)
```
The subscription is completed
```typescript
message$
  .pipe(
    // take messages as long as the value is greater than 0
    takeWhile(m => m.value > 0)
  )
  .subscribe({
    next: m => console.log(m.value),
    complete: () => console.log('complete')
  })
```
The subscription encounters an error
```typescript
message$
  .pipe(
    throwError(new Error('Something went wrong!'))
  ).subscribe({
    next: m => console.log(m.value),
    // Handle errors
    error: err => console.error('Error occurred:', err)
  })
```

### Committing Offsets
Auto Commit is enabled by default. This will automatically commit the offset when processing is completed. See the [KafkaJS Docs](https://kafka.js.org/docs/consuming#a-name-auto-commit-a-autocommit) for more information.

Alternatively, KafkaTide implements an offset management strategy that is safe for concurrent processing. To use this, set autoCommit to false. Manual offset committing is not currently exposed by KafkaTide.

Regardless of commit strategy, `workComplete()` must be called to trigger offsets to be committed, and allow new messages to be consumed.

```typescript
const { message$ } = consume({ topic, config })

message$.subscribe(async ({value, workComplete}) => {
  await saveValue(value)
  workComplete()
}))
```

## Running Tests
This repo adheres to a code coverage threshold of 90% (lines).

To run tests, run the following command.

```bash
  npm run test
```

## Contributing

Contributions are always welcome!

See [contributing.md](contributing.md) for ways to get started.

Please adhere to this project's [code of conduct](code_of_conduct.md).

## Roadmap

* [ ] Transactions support
* [ ] Exactly Once support