[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE) [![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md) 

# KafkaTide

KafkaTide is a lightweight wrapper around the KafkaJS library which provides a RxJS interface for producing and consuming Kafka messages.

The goal of this project is to give the user full control of the asynchronous behavior of their code.

The underlying KafkaJS configs are exposed for maximum control, while smart defaults are chosen to accommodate average use cases.

## Installation

```bash
npm install kafkatide
```
    
## Documentation

[Documentation](documentation.md)


## Usage/Examples

### Initialize Kafka Connection
The KafkaTide constructor is identical to KafkaJS constructor.
```typescript
const { consume, produce } = new KafkaTide({
  brokers: ['broker-1'],
  clientId: 'kafkatide-example',
})
```

### Produce Messages

```typescript
const { sendSubject, disconnectSubject } = produce(topic)

sendSubject.next('Kafka has never been easier')

disconnectSubject.next()
```

### Consume Messages

```typescript
const topic = 'com.kafkatide.example'
const config = {
  groupId: 'kafkatide'
}

const { message$ } = consume({ topic, config })

message$.subscribe((m) => console.log(`received: ${m.value}`))
```

### Committing Offsets
Auto Commit is enabled by default. This will automatically commit the offset when the message has been read. See the [KafkaJS Docs](https://kafka.js.org/docs/consuming#a-name-auto-commit-a-autocommit) for more information.

You may also handle commit offsets manually by setting `autoCommit: false` in the runConfig.

Alternatively, messages expose a workComplete subject. Call `workComplete.next()` to trigger offsets to be committed.

```typescript
const { message$ } = consume({ topic, config })

message$.subscribe(async ({value, workComplete}) => {
  await saveValue(value)
  workComplete.next()
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

* [ ] Investigate better support for KafkaJS autoCommit functionality
  * [ ] Potentially remove workComplete handler
* [ ] Expose consumer.commitOffsets for manual offset handling
* [ ] Use eachBatch instead of eachMessage
* [ ] Allow consuming to be throttled via RxJS